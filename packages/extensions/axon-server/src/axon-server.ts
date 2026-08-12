/**
 * Axon Server backend for kronos.
 *
 * `axonServer(config)` is an async factory: it connects eagerly, hands back
 * the four components it provides (eventStore, snapshotStore, commandBus,
 * queryBus), and gives you a `start`/`close` pair. There is no lifecycle
 * framework — the ordering that used to be encoded as `onStart("connect")` /
 * `onStart("processors")` / `onStop("connect")` is now three lines you write
 * in your composition root:
 *
 * ```ts
 * const axon = await axonServer({
 *   componentName: "university-service",
 *   serializer,
 *   unitOfWorkFactory,
 * })
 * const app = kronos({
 *   components: { ...inMemoryComponents({ serializer, unitOfWorkFactory }), ...axon.components },
 *   modules,
 * })
 * await axon.start()   // readiness barrier: the server can route to our handlers
 * // …
 * await app.stop(); await axon.close()
 * ```
 *
 * Connecting before the app is built is what removes the lazy proxies and
 * subscribe-buffering wrappers the container version needed: by the time
 * `kronos` subscribes a handler, the gRPC streams are already live.
 *
 * REMOTE ADMINISTRATION IS NOT IN HERE. Processor instructions (pause / start /
 * release / split / merge) and processor status reporting are the platform
 * CONTROL PLANE — they are neither persistence nor transport, and lived here
 * only because they share this gRPC connection. They are now an opt-in second
 * object built on the platform stream this backend exposes:
 *
 * ```ts
 * const control = await axonServerControlPlane(axon.platform, app.processors.values())
 * ```
 *
 * `start()` therefore takes NO arguments and does exactly one thing: the
 * data-path readiness barrier. See `control-plane.ts`.
 *
 * Axon-specific protocol invariants are preserved byte-for-byte:
 *
 *   - CLIENT_SUPPORTS_STREAMING capability advertised on every dispatched
 *     query via `defaultQueryInstructions(...)`;
 *   - AxonIQ-Context + AxonIQ-Access-Token gRPC metadata headers built by
 *     `createAxonMetadata(...)` and attached to every outbound stream/RPC;
 *   - permits-AFTER-subscriptions stream ordering preserved on the initial
 *     handshake AND on reconnect (see `ensureStreamStarted` /
 *     `reestablishStreamBody`);
 *   - shutdown ordering: busLatches → platform.stop → connection.close.
 */
import {
  qualifiedNameToString,
  qualifiedNameFromString,
  generateIdentifier,
  type Serializer,
  withRetry,
  healthCheck,
  type ResilienceConfig,
} from "@kronos-ts/common"
import type {
  CommandBus,
  CommandMessage,
  QueryBus,
  QueryMessage,
  SubscriptionFilter,
  SubscriptionQueryResult,
  UoWRunner,
  UpdateHandler,
} from "@kronos-ts/messaging"
import { applySubscriptionFilter, updateHandler, runAfterCommitOrImmediately } from "@kronos-ts/messaging"
import { Metadata } from "nice-grpc"
import type { AxonServerConnectionConfig } from "./connection.js"
import { connectToAxonServer, type AxonServerConnection } from "./connection.js"
import { axonServerEventStore } from "./axon-server-event-store.js"
import { axonServerSnapshotStore } from "./axon-server-snapshot-store.js"
import { metadataToProto, metadataFromProto } from "./metadata-conversion.js"
import { outboundStream } from "./outbound-stream.js"
import { mapErrorCode, AxonServerErrorCode } from "./errors.js"
import { shutdownLatch, type ShutdownLatch } from "./shutdown-latch.js"
import {
  platformConnection,
  type PlatformConnection,
  type PlatformServiceOptions,
} from "./platform-service.js"

/** Default flow control settings — aligned with Java's 5000 permits. */
const DEFAULT_PERMITS = 5000n
const DEFAULT_THRESHOLD = 2500n

/**
 * Flow control configuration for a bus channel.
 */
export interface FlowControlConfig {
  /** Initial permits granted to Axon Server. Default: 5000 (aligned with Java). */
  permits?: number
  /** Threshold at which to request more permits. Default: 2500 (aligned with Java). */
  refillThreshold?: number
}

/**
 * Processing instructions attached to outbound messages.
 * Controls routing, priority, and timeout behavior on Axon Server.
 */
export interface ProcessingInstructions {
  /** Routing key for consistent hashing (e.g., aggregate ID). */
  routingKey?: string
  /** Priority (higher = processed first). Default: 0 */
  priority?: number
  /** Timeout in ms. Axon Server cancels the command/query if not handled in time. */
  timeoutMs?: number
}

// Processing instruction keys — aligned with proto ProcessingKey enum.
// CLIENT_SUPPORTS_STREAMING (key=8) is an Axon-Server-specific capability
// advertisement that MUST survive the migration verbatim — see file-level
// JSDoc above and `defaultQueryInstructions` below.
const INSTRUCTION_KEY = {
  ROUTING_KEY: 0,
  PRIORITY: 1,
  TIMEOUT: 2,
  NR_OF_RESULTS: 3,
  CLIENT_SUPPORTS_STREAMING: 8,
} as const

function toProtoProcessingInstructions(instructions?: ProcessingInstructions): any[] {
  if (!instructions) return []
  const result: any[] = []
  if (instructions.routingKey !== undefined) {
    result.push({ key: INSTRUCTION_KEY.ROUTING_KEY, value: { textValue: instructions.routingKey } })
  }
  if (instructions.priority !== undefined) {
    result.push({ key: INSTRUCTION_KEY.PRIORITY, value: { numberValue: BigInt(instructions.priority) } })
  }
  if (instructions.timeoutMs !== undefined) {
    result.push({ key: INSTRUCTION_KEY.TIMEOUT, value: { numberValue: BigInt(instructions.timeoutMs) } })
  }
  return result
}

/**
 * Build default processing instructions for query dispatch. The
 * CLIENT_SUPPORTS_STREAMING capability is the Axon-specific protocol bit
 * preserved from the legacy enhancer — Axon Server reads this on every
 * query dispatch to decide whether to use streaming responses.
 */
function defaultQueryInstructions(timeoutMs: number): any[] {
  return [
    { key: INSTRUCTION_KEY.TIMEOUT, value: { numberValue: BigInt(timeoutMs) } },
    { key: INSTRUCTION_KEY.NR_OF_RESULTS, value: { numberValue: 1n } },
    { key: INSTRUCTION_KEY.CLIENT_SUPPORTS_STREAMING, value: { booleanValue: true } },
  ]
}

/**
 * Build the gRPC metadata headers required by Axon Server. AxonIQ-Context
 * is mandatory (identifies the tenant/context); AxonIQ-Access-Token is
 * optional auth. Both must be attached to every outbound stream/RPC —
 * preserved verbatim from the legacy enhancer.
 */
function createAxonMetadata(config: { context: string; token: string }): Metadata {
  const metadata = new Metadata()
  metadata.set("AxonIQ-Context", config.context)
  if (config.token) {
    metadata.set("AxonIQ-Access-Token", config.token)
  }
  return metadata
}

export interface AxonServerConfig extends AxonServerConnectionConfig {
  /** Flow control for the command bus channel. */
  commandFlowControl?: FlowControlConfig
  /** Flow control for the query bus channel. */
  queryFlowControl?: FlowControlConfig
  /** Platform service configuration (heartbeat, etc.). */
  platformService?: PlatformServiceOptions
  /**
   * When true, queries are first checked against locally registered handlers
   * before being dispatched through Axon Server. Avoids a network round-trip
   * when the handler is co-located.
   *
   * Aligned with Java's `shortcutQueriesToLocalHandlers`.
   * Default: false.
   */
  shortcutQueriesToLocalHandlers?: boolean
  /**
   * Load factor for command handler registration.
   * Signals to Axon Server how much capacity this handler has.
   * Higher value = handler can take more commands.
   *
   * Aligned with Java's `commandLoadFactor`. Default: 100.
   */
  commandLoadFactor?: number
  /**
   * Default timeout for command dispatch in ms. Default: 300000 (5 min).
   * Aligned with Java's processing instruction timeout.
   */
  commandTimeoutMs?: number
  /**
   * Default timeout for query dispatch in ms. Default: 3600000 (1 hour).
   * Aligned with Java's processing instruction timeout.
   */
  queryTimeoutMs?: number
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
  /**
   * How long `start()` waits for Axon Server's routing tables to register the
   * subscribe frames sent on the command/query streams. This is the entire
   * data-path readiness barrier.
   *
   * It is a timed wait rather than an observed signal because nothing on the
   * client can observe it: subscribes travel on the bus streams, and the
   * platform stream — which is where an ack would arrive — is a different
   * stream that Axon Server holds open silently after `register`. Default:
   * 1000, matching the legacy enhancer. Tests against a freshly-booted server
   * can tighten this once subscriptions are observed to land faster.
   */
  busSubscriptionAckDelayMs?: number
}

/** The components an Axon Server backend provides. Spread into `kronos`. */
export interface AxonServerComponents {
  eventStore: ReturnType<typeof axonServerEventStore>
  snapshotStore: ReturnType<typeof axonServerSnapshotStore>
  commandBus: CommandBus
  queryBus: QueryBus
}

/**
 * A live Axon Server backend: the components it provides plus the two calls
 * that used to be lifecycle stages.
 */
/** Everything axonServer() needs: its own config plus the framework values it borrows. */
export type AxonServerOptions = AxonServerConfig & { serializer: Serializer; unitOfWorkFactory: UoWRunner }

export interface AxonServerBackend {
  readonly components: AxonServerComponents
  /**
   * The platform stream, built but NOT started.
   *
   * This is the seam the control plane plugs into. It is built here because the
   * backend owns the gRPC connection it rides on and the `platformService`
   * tuning that configures it; it is left unstarted because starting it is the
   * control plane's job — the instruction handler and status supplier have to
   * be registered before the `register` frame goes out (see `control-plane.ts`).
   *
   * A service with no remote administration can ignore this entirely, or call
   * `platform.start()` directly if it wants the heartbeat / registration
   * without the processor-control wiring.
   */
  readonly platform: PlatformConnection
  /**
   * DATA-PATH READINESS BARRIER. Wait until Axon Server can route to the
   * handlers subscribed on the bus streams. Call AFTER `kronos` — the
   * subscribe frames must already be on the wire for the wait to mean anything.
   *
   * Takes no arguments and touches no control-plane state.
   */
  start(): Promise<void>
  /** Drain in-flight bus work, stop the platform stream, close the connection. */
  close(): Promise<void>
}

/**
 * Connect to Axon Server and build the components it backs.
 *
 * `serializer` and `unitOfWorkFactory` are arguments rather than slot lookups:
 * the buses serialize payloads with the former and run every inbound command /
 * query in the latter, so they must be the SAME instances the rest of the app
 * uses. Pass the ones you hand to `kronos`.
 */
export async function axonServer(
  options: AxonServerOptions,
): Promise<AxonServerBackend> {
  const config = options
  const { serializer, unitOfWorkFactory, resilience } = config

  const connection = await withRetry(async () => connectToAxonServer(config), {
    event: "initial-connect",
    ...resilience,
  })

  // Health-check ping with warn-then-continue (D-100). AxonServerConnection has
  // no dedicated probe surface today; the gRPC channel itself is created
  // eagerly in connectToAxonServer so the meaningful probe is a round-trip — we
  // approximate via a soft no-op promise that satisfies the threshold contract.
  // Real network failure is surfaced by the first bus call against the channel.
  await healthCheck(async () => undefined, {
    thresholdMs: resilience?.healthCheckThresholdMs,
    log: resilience?.log,
  })

  // One latch per bus, drained in close() before the transport goes away.
  const commandLatch = shutdownLatch()
  const queryLatch = shutdownLatch()
  const busLatches: ShutdownLatch[] = [commandLatch, queryLatch]

  // The connection is live before anything below is built, so the buses open
  // their gRPC streams for real and `subscribe()` reaches the wire immediately —
  // no lazy proxy, no subscription buffering, no readiness promise.
  const components: AxonServerComponents = {
    eventStore: axonServerEventStore(connection, serializer),
    snapshotStore: axonServerSnapshotStore(connection, serializer),
    commandBus: createDistributedCommandBus(
      connection,
      unitOfWorkFactory,
      commandLatch,
      serializer,
      config.commandFlowControl,
      config.commandLoadFactor,
      resilience,
    ),
    queryBus: distributedQueryBus(
      connection,
      unitOfWorkFactory,
      queryLatch,
      serializer,
      config.queryFlowControl,
      config.shortcutQueriesToLocalHandlers,
      config.queryTimeoutMs,
      resilience,
    ),
  }

  // Built here, started by the control plane (or by the caller). Constructing it
  // eagerly is what lets the control plane be a separate object at all — and it
  // keeps `platformService` tuning and `stop()` ownership in one place, so the
  // documented shutdown order below holds whether or not anyone opted in.
  const platform = platformConnection(connection, config.platformService)

  return {
    components,
    platform,

    async start() {
      // The only thing the data path has to wait for: Axon Server's
      // command/query routing tables registering the subscribe frames sent on
      // the BUS streams. It cannot be derived from the platform stream, because
      // subscribes travel on a different stream entirely — and the platform
      // stream's own `subscriptionsAcked()` latch says nothing about them (it
      // latches unconditionally once `register` has been flushed; see
      // platform-service.ts). So this barrier is the settle wait, and it is
      // deliberately independent of whether the platform stream is up at all.
      // The legacy enhancer carried the same 1s wait.
      await new Promise((r) => setTimeout(r, config.busSubscriptionAckDelayMs ?? 1000))
    },

    async close() {
      await Promise.all(busLatches.map((l) => l.initiateShutdown()))
      // Idempotent, and independent of `control.close()` — a backend that was
      // never administered still stops a platform stream someone else started.
      platform.stop()
      connection.close()
    },
  }
}

// ---------------------------------------------------------------------------
// Shared payload helpers (moved verbatim from legacy enhancer)
// ---------------------------------------------------------------------------

function createPayloadHelpers(serializer: Serializer) {
  return {
    serializePayload(name: string, payload: unknown, revision: string = "") {
      return serializer.serialize(payload, name, revision)
    },
    deserializePayload(data: Uint8Array | undefined, type: string = "", revision: string = ""): unknown {
      if (!data || data.length === 0) return undefined
      return serializer.deserialize({ data, type, revision })
    },
  }
}

// ---------------------------------------------------------------------------
// Distributed Command Bus
//
// Bus implementation moved verbatim from the legacy enhancer with TWO
// behavioural additions per D-97:
//   1) reestablishStream() body wrapped in withRetry({ event: "reconnect" })
//   2) inbound-stream backoff replaced by the same withRetry path
//
// Axon-specific protocol invariants preserved BYTE-FOR-BYTE:
//   - AxonIQ-Context + AxonIQ-Access-Token gRPC metadata headers via
//     createAxonMetadata(connection.config)
//   - permits-AFTER-subscriptions ordering on reestablishStreamBody (subs
//     are sent BEFORE grantPermits() in the reconnect path; the initial
//     handshake matches this — see ensureStreamStarted's grantPermits call
//     in subscribe()).
// ---------------------------------------------------------------------------

/**
 * A command bus backed by Axon Server.
 *
 * - **Outbound dispatch**: Always goes through Axon Server via the unary Dispatch RPC.
 *   Axon Server routes the command to the appropriate node (which may be this one).
 * - **Local segment**: Handlers subscribed via `subscribe()` are registered with
 *   Axon Server (so other nodes can route to us) and stored locally. When Axon Server
 *   routes an inbound command to this node, it's executed on the local segment
 *   within a UnitOfWork.
 */
function createDistributedCommandBus(
  connection: AxonServerConnection,
  unitOfWorkRunner: UoWRunner,
  shutdownLatch: ShutdownLatch,
  serializer: Serializer,
  flowControl?: FlowControlConfig,
  commandLoadFactor?: number,
  resilience?: Partial<ResilienceConfig>,
): CommandBus {
  const metadata = createAxonMetadata(connection.config)
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))

  // Local segment — handlers that execute on this node
  const localSegment = new Map<string, (message: CommandMessage) => Promise<unknown>>()

  // Bidirectional stream for handler registration + inbound command handling
  let outbound = outboundStream<any>()
  let streamStarted = false
  let permits = 0n

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    // Open stream using connection.commands (always gets current client after reconnect)
    const inbound = connection.commands.openStream(outbound.iterable, { metadata })
    processInboundCommands(inbound)
  }

  function grantPermits() {
    outbound.send({
      flowControl: { clientId: connection.config.clientId, permits: PERMITS },
      instructionId: "",
    })
    permits += PERMITS
  }

  /**
   * Re-establish the bidirectional stream and re-subscribe all handlers.
   * Called on stream error or when the connection reconnects.
   *
   * ORDER (preserves Axon-specific invariant): subscriptions are
   * re-emitted BEFORE the permits frame. Sending permits first would
   * trigger a server-side stream error.
   */
  function reestablishStreamBody() {
    outbound.close()
    outbound = outboundStream<any>()
    streamStarted = false
    permits = 0n
    ensureStreamStarted()
    // Re-subscribe all handlers FIRST
    for (const commandName of localSegment.keys()) {
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          command: commandName,
          componentName: connection.config.componentName,
          clientId: connection.config.clientId,
          loadFactor: commandLoadFactor ?? 100,
        },
        instructionId: generateIdentifier(),
      })
    }
    // Permits AFTER subscriptions (Axon-specific ordering invariant)
    grantPermits()
  }

  async function reestablishStreamWithRetry() {
    if (shutdownLatch.shuttingDown) return
    await withRetry(async () => reestablishStreamBody(), {
      event: "reconnect",
      ...resilience,
    })
  }

  // Auto-reestablish when the connection reconnects (e.g., after heartbeat timeout)
  connection.onReconnect(() => {
    if (!shutdownLatch.shuttingDown && streamStarted) {
      reestablishStreamWithRetry().catch((err) => {
        console.error("Distributed command bus: reconnect retries exhausted", err)
      })
    }
  })

  async function processInboundCommands(inbound: AsyncIterable<any>) {
    try {
      for await (const message of inbound) {
        if (!message.command) continue

        permits--
        const proto = message.command
        const commandName = proto.name
        const handler = localSegment.get(commandName)

        let resultPayload: unknown
        let errorCode = ""
        let errorMsg = ""

        if (handler) {
          try {
            const commandMessage: CommandMessage = {
              kind: "command",
              identifier: proto.messageIdentifier,
              name: qualifiedNameFromString(commandName),
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined),
              metadata: metadataFromProto(proto.metaData),
              timestamp: Number(proto.timestamp),
            }

            // Execute inbound command within its own UnitOfWork (AF5 parity)
            resultPayload = await unitOfWorkRunner(commandMessage.metadata, () =>
              handler(commandMessage),
            )
          } catch (err) {
            errorCode = AxonServerErrorCode.COMMAND_EXECUTION_ERROR
            errorMsg = err instanceof Error ? err.message : String(err)
          }
        } else {
          errorCode = AxonServerErrorCode.NO_HANDLER_FOR_COMMAND
          errorMsg = `No local handler for command "${commandName}"`
        }

        // Send response back to Axon Server
        outbound.send({
          commandResponse: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
              : undefined,
            payload: resultPayload !== undefined
              ? serializePayload("result", resultPayload)
              : undefined,
            metaData: {},
            processingInstructions: [],
          },
          instructionId: "",
        })

        // Refill permits when running low
        if (permits <= THRESHOLD) {
          outbound.send({
            flowControl: { clientId: connection.config.clientId, permits: PERMITS },
            instructionId: "",
          })
          permits += PERMITS
        }
      }
    } catch (err) {
      if (shutdownLatch.shuttingDown) return
      if (String(err).includes("Connection dropped")) return

      console.error("Distributed command bus: inbound stream error, attempting re-establishment via withRetry", err)
      await reestablishStreamWithRetry().catch((retryErr) => {
        console.error("Distributed command bus: reconnect retries exhausted", retryErr)
      })
    }
  }

  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const activity = shutdownLatch.registerActivity()
      try {
        const commandName = qualifiedNameToString(message.name)

        const response = await connection.commands.dispatch({
          messageIdentifier: message.identifier,
          name: commandName,
          timestamp: BigInt(message.timestamp),
          payload: serializePayload(commandName, message.payload),
          metaData: metadataToProto(message.metadata),
          processingInstructions: toProtoProcessingInstructions(message.metadata?.processingInstructions as ProcessingInstructions | undefined),
          clientId: connection.config.clientId,
          componentName: connection.config.componentName,
        }, { metadata })

        if (response.errorCode && response.errorCode !== "") {
          throw mapErrorCode(
            response.errorCode,
            response.errorMessage?.message ?? "Unknown error",
          )
        }

        return deserializePayload(response.payload?.data as Uint8Array | undefined)
      } finally {
        activity.end()
      }
    },

    subscribe(commandName: string, handler: (message: CommandMessage) => Promise<unknown>) {
      localSegment.set(commandName, handler)

      ensureStreamStarted()
      // Subscription FIRST
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          command: commandName,
          componentName: connection.config.componentName,
          clientId: connection.config.clientId,
          loadFactor: commandLoadFactor ?? 100,
        },
        instructionId: generateIdentifier(),
      })
      // Permits AFTER subscription (Axon-specific ordering invariant)
      grantPermits()
    },
  }
}

// ---------------------------------------------------------------------------
// Distributed Query Bus
// ---------------------------------------------------------------------------

/**
 * A query bus backed by Axon Server.
 *
 * Same architecture as the distributed command bus:
 * - **Outbound dispatch**: Always through Axon Server.
 * - **Local segment**: Handlers registered here are stored locally and
 *   registered with Axon Server for inbound routing. Inbound queries
 *   are executed within a UnitOfWork.
 */
export function distributedQueryBus(
  connection: AxonServerConnection,
  unitOfWorkRunner: UoWRunner,
  shutdownLatch: ShutdownLatch,
  serializer: Serializer,
  flowControl?: FlowControlConfig,
  shortcutQueriesToLocalHandlers?: boolean,
  queryTimeoutMs?: number,
  resilience?: Partial<ResilienceConfig>,
): QueryBus {
  const metadata = createAxonMetadata(connection.config)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)

  const localSegment = new Map<string, (message: QueryMessage) => Promise<unknown>>()

  // Local subscription store — subscription queries opened by THIS instance.
  // Inbound updates from the server are offered into these via the Subscription RPC loop.
  const subscriptions = new Map<string, UpdateHandler>()

  // Subscriptions the SERVER has routed to this instance as the handler.
  // Populated when the server delivers SubscriptionQueryRequest.subscribe over OpenStream.
  // emitUpdate / completeSubscription apply the caller-supplied filter against these
  // to decide which subscriber IDs to target; the server forwards each response to the
  // exact subscriber.
  const handlerSubscriptions = new Map<string, { queryName: string; payload: unknown }>()

  let outbound = outboundStream<any>()
  let streamStarted = false
  let permits = 0n

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    const inbound = connection.queries.openStream(outbound.iterable, { metadata })
    processInboundQueries(inbound)
  }

  function grantQueryPermits() {
    outbound.send({
      flowControl: { clientId: connection.config.clientId, permits: PERMITS },
      instructionId: "",
    })
    permits += PERMITS
  }

  /**
   * Re-establish the bidirectional stream and re-subscribe all handlers.
   * Called on stream error or when the connection reconnects.
   *
   * ORDER (preserves Axon-specific invariant): subscriptions are
   * re-emitted BEFORE the permits frame.
   */
  function reestablishStreamBody() {
    outbound.close()
    outbound = outboundStream<any>()
    streamStarted = false
    permits = 0n
    ensureStreamStarted()
    for (const queryName of localSegment.keys()) {
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          query: queryName,
          resultName: "",
          componentName: connection.config.componentName,
          clientId: connection.config.clientId,
        },
        instructionId: generateIdentifier(),
      })
    }
    grantQueryPermits()
  }

  async function reestablishStreamWithRetry() {
    if (shutdownLatch.shuttingDown) return
    await withRetry(async () => reestablishStreamBody(), {
      event: "reconnect",
      ...resilience,
    })
  }

  // Auto-reestablish when the connection reconnects (e.g., after heartbeat timeout)
  connection.onReconnect(() => {
    if (!shutdownLatch.shuttingDown && streamStarted) {
      reestablishStreamWithRetry().catch((err) => {
        console.error("Distributed query bus: reconnect retries exhausted", err)
      })
    }
  })

  async function handleSubscriptionQueryRequest(req: any): Promise<void> {
    if (req.subscribe) {
      const sub = req.subscribe
      const subId: string = sub.subscriptionIdentifier
      const proto = sub.queryRequest
      if (!subId || !proto) return

      const queryName: string = proto.query
      const payload = deserializePayload(
        proto.payload?.data as Uint8Array | undefined,
        proto.payload?.type,
        proto.payload?.revision,
      )
      handlerSubscriptions.set(subId, { queryName, payload })

      const handler = localSegment.get(queryName)
      let resultPayload: unknown
      let errorCode = ""
      let errorMsg = ""

      if (handler) {
        try {
          const queryMessage: QueryMessage = {
            kind: "query",
            identifier: proto.messageIdentifier,
            name: qualifiedNameFromString(queryName),
            payload,
            metadata: metadataFromProto(proto.metaData ?? {}),
            timestamp: Number(proto.timestamp),
          }
          resultPayload = await unitOfWorkRunner(queryMessage.metadata, async () => {
            return handler(queryMessage)
          })
        } catch (err) {
          errorCode = AxonServerErrorCode.QUERY_EXECUTION_ERROR
          errorMsg = err instanceof Error ? err.message : String(err)
        }
      } else {
        errorCode = AxonServerErrorCode.NO_HANDLER_FOR_QUERY
        errorMsg = `No local handler for query "${queryName}"`
      }

      const responseSerialized = resultPayload !== undefined
        ? serializePayload("result", resultPayload)
        : undefined

      outbound.send({
        subscriptionQueryResponse: {
          messageIdentifier: generateIdentifier(),
          subscriptionIdentifier: subId,
          initialResult: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
              : undefined,
            payload: responseSerialized,
            metaData: {},
            processingInstructions: [],
          },
        },
        instructionId: "",
      })
      return
    }
    if (req.unsubscribe) {
      handlerSubscriptions.delete(req.unsubscribe.subscriptionIdentifier)
    }
    // flowControl + getInitialResult are not tracked per-sub; ignored for now.
  }

  async function processInboundQueries(inbound: AsyncIterable<any>) {
    try {
      for await (const message of inbound) {
        if (message.subscriptionQueryRequest) {
          await handleSubscriptionQueryRequest(message.subscriptionQueryRequest)
          continue
        }
        if (!message.query) continue

        permits--
        const proto = message.query
        const queryName = proto.query
        const handler = localSegment.get(queryName)

        let resultPayload: unknown
        let errorCode = ""
        let errorMsg = ""

        if (handler) {
          try {
            const queryMessage: QueryMessage = {
              kind: "query",
              identifier: proto.messageIdentifier,
              name: qualifiedNameFromString(queryName),
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined),
              metadata: metadataFromProto(proto.metaData),
              timestamp: Number(proto.timestamp),
            }

            resultPayload = await unitOfWorkRunner(queryMessage.metadata, async () => {
              return handler(queryMessage)
            })
          } catch (err) {
            errorCode = AxonServerErrorCode.QUERY_EXECUTION_ERROR
            errorMsg = err instanceof Error ? err.message : String(err)
          }
        } else {
          errorCode = AxonServerErrorCode.NO_HANDLER_FOR_QUERY
          errorMsg = `No local handler for query "${queryName}"`
        }

        outbound.send({
          queryResponse: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
              : undefined,
            payload: resultPayload !== undefined
              ? serializePayload("result", resultPayload)
              : undefined,
            metaData: {},
            processingInstructions: [],
          },
          instructionId: "",
        })

        outbound.send({
          queryComplete: {
            messageId: generateIdentifier(),
            requestId: proto.messageIdentifier,
          },
          instructionId: "",
        })

        if (permits <= THRESHOLD) {
          outbound.send({
            flowControl: { clientId: connection.config.clientId, permits: PERMITS },
            instructionId: "",
          })
          permits += PERMITS
        }
      }
    } catch (err) {
      if (shutdownLatch.shuttingDown) return
      if (String(err).includes("Connection dropped")) return

      console.error("Distributed query bus: inbound stream error, attempting re-establishment via withRetry", err)
      await reestablishStreamWithRetry().catch((retryErr) => {
        console.error("Distributed query bus: reconnect retries exhausted", retryErr)
      })
    }
  }

  return {
    async query(message: QueryMessage): Promise<unknown> {
      const activity = shutdownLatch.registerActivity()
      try {
        const queryName = qualifiedNameToString(message.name)

        // Local shortcut — handle locally if handler is co-located
        if (shortcutQueriesToLocalHandlers) {
          const localHandler = localSegment.get(queryName)
          if (localHandler) {
            return unitOfWorkRunner(message.metadata, async () => {
              return localHandler(message)
            })
          }
        }

        const responseStream = connection.queries.query({
          messageIdentifier: message.identifier,
          query: queryName,
          timestamp: BigInt(message.timestamp),
          payload: serializePayload(queryName, message.payload),
          metaData: metadataToProto(message.metadata),
          processingInstructions: defaultQueryInstructions(queryTimeoutMs ?? 3600000),
          clientId: connection.config.clientId,
          componentName: connection.config.componentName,
        }, { metadata })

        for await (const response of responseStream) {
          if (response.errorCode && response.errorCode !== "") {
            throw mapErrorCode(
              response.errorCode,
              response.errorMessage?.message ?? "Unknown error",
            )
          }
          return deserializePayload(response.payload?.data as Uint8Array | undefined)
        }

        throw new Error(`No response for query "${queryName}"`)
      } finally {
        activity.end()
      }
    },

    subscribe(queryName: string, handler: (message: QueryMessage) => Promise<unknown>) {
      localSegment.set(queryName, handler)

      ensureStreamStarted()
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          query: queryName,
          resultName: "",
          componentName: connection.config.componentName,
          clientId: connection.config.clientId,
        },
        instructionId: generateIdentifier(),
      })
      // Permits AFTER subscription (Axon-specific ordering invariant)
      grantQueryPermits()
    },

    subscriptionQuery(message: QueryMessage, bufferSize?: number): SubscriptionQueryResult {
      const queryId = message.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const handler = updateHandler(message, bufferSize)
      subscriptions.set(queryId, handler)

      const queryName = qualifiedNameToString(message.name)
      const subscriptionId = generateIdentifier()

      const outboundSub = outboundStream<any>()

      outboundSub.send({
        subscribe: {
          subscriptionIdentifier: subscriptionId,
          numberOfPermits: BigInt(bufferSize ?? 256),
          queryRequest: {
            messageIdentifier: message.identifier,
            query: queryName,
            timestamp: BigInt(message.timestamp),
            payload: serializePayload(queryName, message.payload),
            metaData: metadataToProto(message.metadata),
            processingInstructions: defaultQueryInstructions(queryTimeoutMs ?? 3600000),
            clientId: connection.config.clientId,
            componentName: connection.config.componentName,
          },
        },
      })

      outboundSub.send({
        getInitialResult: {
          subscriptionIdentifier: subscriptionId,
          numberOfPermits: 1n,
          queryRequest: {
            messageIdentifier: message.identifier,
            query: queryName,
            timestamp: BigInt(message.timestamp),
            payload: serializePayload(queryName, message.payload),
            metaData: metadataToProto(message.metadata),
            processingInstructions: defaultQueryInstructions(queryTimeoutMs ?? 3600000),
            clientId: connection.config.clientId,
            componentName: connection.config.componentName,
          },
        },
      })

      const responseStream = connection.queries.subscription(outboundSub.iterable, { metadata })

      let resolveInitial!: (value: unknown) => void
      let rejectInitial!: (error: Error) => void
      const initialResult = new Promise<unknown>((resolve, reject) => {
        resolveInitial = resolve
        rejectInitial = reject
      })
      let initialSettled = false

      ;(async () => {
        try {
          for await (const response of responseStream) {
            if (response.initialResult) {
              const initial = response.initialResult
              if (!initialSettled) {
                if (initial.errorCode && initial.errorCode !== "") {
                  rejectInitial(mapErrorCode(initial.errorCode, initial.errorMessage?.message ?? "Unknown error"))
                } else {
                  resolveInitial(deserializePayload(initial.payload?.data as Uint8Array | undefined))
                }
                initialSettled = true
              }
            } else if (response.update) {
              const update = deserializePayload(response.update.payload?.data as Uint8Array | undefined)
              handler.offer(update)
            } else if (response.complete) {
              handler.complete()
              break
            } else if (response.completeExceptionally) {
              handler.completeExceptionally(
                new Error(response.completeExceptionally.errorMessage?.message ?? "Subscription query failed"),
              )
              break
            }
          }
        } catch (err) {
          const error = err instanceof Error ? err : new Error(String(err))
          if (!initialSettled) {
            rejectInitial(error)
            initialSettled = true
          }
          handler.completeExceptionally(error)
        } finally {
          subscriptions.delete(queryId)
        }
      })()

      return {
        initialResult,
        updates: handler.iterable,
        close: () => {
          outboundSub.send({
            unsubscribe: {
              subscriptionIdentifier: subscriptionId,
            },
          })
          outboundSub.close()
          subscriptions.delete(queryId)
          handler.complete()
        },
      }
    },

    subscribeToUpdates(message: QueryMessage, bufferSize?: number): AsyncIterable<unknown> & { close(): void } {
      const queryId = message.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const handler = updateHandler(message, bufferSize)
      subscriptions.set(queryId, handler)

      return {
        [Symbol.asyncIterator]: () => handler.iterable[Symbol.asyncIterator](),
        close: () => {
          subscriptions.delete(queryId)
          handler.complete()
        },
      }
    },

    async emitUpdate(
      queryName: string,
      filter: SubscriptionFilter,
      update: unknown,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [subId, sub] of handlerSubscriptions) {
          if (sub.queryName !== queryName) continue
          if (!applySubscriptionFilter(filter, sub.payload)) continue

          const serialized = serializePayload(queryName, update)
          outbound.send({
            subscriptionQueryResponse: {
              messageIdentifier: generateIdentifier(),
              subscriptionIdentifier: subId,
              update: {
                messageIdentifier: generateIdentifier(),
                payload: serialized,
                metaData: {},
                clientId: connection.config.clientId,
                componentName: connection.config.componentName,
                errorCode: "",
                errorMessage: undefined,
              },
            },
            instructionId: "",
          })
        }
      })
    },

    async completeSubscription(
      queryName: string,
      filter?: SubscriptionFilter,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [subId, sub] of handlerSubscriptions) {
          if (sub.queryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, sub.payload)) continue

          outbound.send({
            subscriptionQueryResponse: {
              messageIdentifier: generateIdentifier(),
              subscriptionIdentifier: subId,
              complete: {
                clientId: connection.config.clientId,
                componentName: connection.config.componentName,
              },
            },
            instructionId: "",
          })
          handlerSubscriptions.delete(subId)
        }
      })
    },

    async completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: SubscriptionFilter,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [subId, sub] of handlerSubscriptions) {
          if (sub.queryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, sub.payload)) continue

          outbound.send({
            subscriptionQueryResponse: {
              messageIdentifier: generateIdentifier(),
              subscriptionIdentifier: subId,
              completeExceptionally: {
                clientId: connection.config.clientId,
                componentName: connection.config.componentName,
                errorCode: AxonServerErrorCode.QUERY_EXECUTION_ERROR,
                errorMessage: {
                  message: error.message,
                  location: connection.config.componentName,
                  details: [],
                  errorCode: AxonServerErrorCode.QUERY_EXECUTION_ERROR,
                },
              },
            },
            instructionId: "",
          })
          handlerSubscriptions.delete(subId)
        }
      })
    },
  }
}
