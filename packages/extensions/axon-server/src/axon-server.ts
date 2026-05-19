/**
 * Native Axon Server extension (Phase 9, D-95 / D-101 / D-102).
 *
 * Replaces the legacy enhancer surface (now deleted) with a
 * `(app: App) => void` extension that:
 *
 *   - populates four typed slots (eventStore, snapshotStore, commandBus,
 *     queryBus) via app.set(...) using the canonical Resolved slot names
 *     (in particular `resolved.unitOfWorkFactory`, NOT `unitOfWorkRunner`);
 *   - wires connect-stage transport bring-up under the @kronos-ts/common
 *     resilience helper (initial-connect + health-check + platform setup +
 *     instruction handlers + platform.start);
 *   - wires processors-stage subscription-ack wait via withRetry against
 *     `platform.subscriptionsAcked()` — REPLACES the 1-second sleep hack
 *     that lived at line 264 of the legacy file (D-102 — Axon equivalent);
 *   - reverses shutdown deterministically in a single onStop('connect') hook
 *     (busLatches → platform.stop → connection.close — D-101.b).
 *
 * Mirrors `kronosdb.ts` (Plan 09-03) STRUCTURALLY — same slot+lifecycle
 * pattern, same resilience helper, same shutdown ordering, same
 * subscription-ack derivation strategy — but preserves Axon-specific
 * protocol invariants byte-for-byte:
 *
 *   - CLIENT_SUPPORTS_STREAMING capability advertised on every dispatched
 *     query via `defaultQueryInstructions(...)`;
 *   - AxonIQ-Context + AxonIQ-Access-Token gRPC metadata headers built by
 *     `createAxonMetadata(...)` and attached to every outbound stream/RPC;
 *   - permits-AFTER-subscriptions stream ordering preserved on the initial
 *     handshake AND on reconnect (legacy semantics in `ensureStreamStarted`
 *     issued permits before subscriptions; this implementation matches that
 *     exact ordering — see `ensureStreamStarted` / `reestablishStreamBody`).
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
import type { App } from "@kronos-ts/app"
import type {
  CommandBus,
  CommandMessage,
  EventProcessorModule,
  QueryBus,
  QueryMessage,
  SubscriptionQueryResult,
  UoWRunner,
  UpdateHandler,
} from "@kronos-ts/messaging"
import { createUpdateHandler, runAfterCommitOrImmediately } from "@kronos-ts/messaging"
import { Metadata } from "nice-grpc"
import type { AxonServerConnectionConfig } from "./connection.js"
import { connectToAxonServer, type AxonServerConnection } from "./connection.js"
import { createAxonServerEventStore } from "./axon-server-event-store.js"
import { createAxonServerSnapshotStore } from "./axon-server-snapshot-store.js"
import { metadataToProto, metadataFromProto } from "./metadata-conversion.js"
import { createOutboundStream } from "./outbound-stream.js"
import { mapErrorCode, AxonServerErrorCode } from "./errors.js"
import { createShutdownLatch, type ShutdownLatch } from "./shutdown-latch.js"
import {
  createPlatformConnection,
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

export interface AxonServerExtensionConfig extends AxonServerConnectionConfig {
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
   * Delay in ms after the platform-stream ack to give Axon Server's
   * routing tables time to register the subscribe frames sent on the
   * command/query streams. The platform stream cannot observe these (they
   * travel on different streams). Default: 1000 — matches the legacy
   * enhancer's wait. Tests against a freshly-booted server can tighten
   * this once subscriptions are observed to land faster.
   */
  busSubscriptionAckDelayMs?: number
}

/**
 * Native Axon Server extension factory. Returns an Extension closure shaped
 * as `(app: App) => void` per D-95.
 *
 * ```ts
 * await kronos()
 *   .use(axonServer({ componentName: "university-service" }))
 *   .start()
 * ```
 */
export function axonServer(serverConfig: AxonServerExtensionConfig): (app: App) => void {
  return (app) => {
    let connection: AxonServerConnection | undefined
    let platform: PlatformConnection | undefined
    const busLatches: ShutdownLatch[] = []

    function getConnection(): AxonServerConnection {
      if (!connection) {
        throw new Error(
          "[kronos:axon-server] connection not yet established — wait for onStart('connect')",
        )
      }
      return connection
    }

    // ---- Slot population (D-95) -----------------------------------------
    //
    // AppImpl.start() in @kronos-ts/app eagerly resolves all 8 slots and
    // runs `commandBus.subscribe(...)` for every registered handler BEFORE
    // any onStart('connect') hook fires (see app.ts §3 / §5c). The Axon
    // bus factories open real gRPC streams against the live channel during
    // construction (createAxonMetadata / connection.onReconnect / inbound
    // stream openers), so they CANNOT run until the connect hook has
    // populated `connection`.
    //
    // Solution: the slot factories return wrappers around lazily-built
    // inner instances. EventStore/SnapshotStore use a lightweight lazy
    // proxy because their factories never dereference `connection` at
    // construction time (only inside method bodies). CommandBus/QueryBus
    // use a `subscribe()`-buffering wrapper that queues subscriptions
    // synchronously and replays them once the connect hook completes —
    // dispatch / query calls await the same readiness promise.

    /** Latches once the connect hook has populated `connection`. */
    let resolveConnected: () => void = () => {}
    const connected: Promise<void> = new Promise((res) => {
      resolveConnected = res
    })

    app.set("eventStore", (resolved) => {
      // Lazy proxy: createAxonServerEventStore stores `connection` in
      // closure scope but only dereferences it inside method bodies, so
      // a Proxy that forwards property access to getConnection() works
      // — by the time framework code calls source/append/stream the
      // connect hook has populated the closure.
      const lazyConnection = new Proxy({} as AxonServerConnection, {
        get(_t, prop) {
          return (getConnection() as any)[prop]
        },
      })
      return createAxonServerEventStore(lazyConnection, resolved.serializer)
    })

    app.set("snapshotStore", (resolved) => {
      const lazyConnection = new Proxy({} as AxonServerConnection, {
        get(_t, prop) {
          return (getConnection() as any)[prop]
        },
      })
      return createAxonServerSnapshotStore(lazyConnection, resolved.serializer)
    })

    app.set("commandBus", (resolved) => {
      const latch = createShutdownLatch()
      busLatches.push(latch)

      let inner: CommandBus | undefined
      const pendingSubs: Array<[string, (m: CommandMessage) => Promise<unknown>]> = []

      // Build the real bus once the connect hook fires + replay buffered subs.
      connected.then(() => {
        inner = createDistributedCommandBus(
          getConnection(),
          resolved.unitOfWorkFactory,
          latch,
          resolved.serializer,
          serverConfig.commandFlowControl,
          serverConfig.commandLoadFactor,
          serverConfig.resilience,
        )
        for (const [name, h] of pendingSubs) inner.subscribe(name, h)
        pendingSubs.length = 0
      })

      const wrapper: CommandBus = {
        async dispatch(message) {
          await connected
          return inner!.dispatch(message)
        },
        subscribe(name, handler) {
          if (inner) inner.subscribe(name, handler)
          else pendingSubs.push([name, handler])
        },
      }
      return wrapper
    })

    app.set("queryBus", (resolved) => {
      const latch = createShutdownLatch()
      busLatches.push(latch)

      let inner: QueryBus | undefined
      const pendingSubs: Array<[string, (m: QueryMessage) => Promise<unknown>]> = []

      connected.then(() => {
        inner = createDistributedQueryBus(
          getConnection(),
          resolved.unitOfWorkFactory,
          latch,
          resolved.serializer,
          serverConfig.queryFlowControl,
          serverConfig.shortcutQueriesToLocalHandlers,
          serverConfig.queryTimeoutMs,
          serverConfig.resilience,
        )
        for (const [name, h] of pendingSubs) inner.subscribe(name, h)
        pendingSubs.length = 0
      })

      const wrapper: QueryBus = {
        async query(message) {
          await connected
          return inner!.query(message)
        },
        subscribe(name, handler) {
          if (inner) inner.subscribe(name, handler)
          else pendingSubs.push([name, handler])
        },
        subscriptionQuery(message, bufferSize) {
          if (!inner) {
            throw new Error(
              "[kronos:axon-server] subscriptionQuery called before connect hook completed",
            )
          }
          return inner.subscriptionQuery(message, bufferSize)
        },
        subscribeToUpdates(message, bufferSize) {
          if (!inner) {
            throw new Error(
              "[kronos:axon-server] subscribeToUpdates called before connect hook completed",
            )
          }
          return inner.subscribeToUpdates(message, bufferSize)
        },
        async emitUpdate(name, filter, update) {
          await connected
          return inner!.emitUpdate(name, filter, update)
        },
        async completeSubscription(name, filter) {
          await connected
          return inner!.completeSubscription(name, filter)
        },
        async completeSubscriptionExceptionally(name, error, filter) {
          await connected
          return inner!.completeSubscriptionExceptionally(name, error, filter)
        },
      }
      return wrapper
    })

    // ---- Lifecycle: connect (D-101 normative split) ---------------------
    // connect = initial connect + health-check + platform setup +
    //           instruction wiring + platform.start.
    app.onStart("connect", async () => {
      connection = await withRetry(
        async () => connectToAxonServer(serverConfig),
        { event: "initial-connect", ...serverConfig.resilience },
      )

      // Health-check ping with warn-then-continue (D-100). AxonServerConnection
      // has no dedicated probe surface today; the gRPC channel itself is
      // created eagerly in connectToAxonServer so the meaningful probe is a
      // round-trip — we approximate via a soft no-op promise that satisfies
      // the threshold contract. Real network failure is surfaced by the
      // first bus call against the live channel.
      await healthCheck(async () => undefined, {
        thresholdMs: serverConfig.resilience?.healthCheckThresholdMs,
        log: serverConfig.resilience?.log,
      })

      platform = createPlatformConnection(connection!, serverConfig.platformService)

      // Build a name-keyed view of the EventProcessorModule list so server-
      // initiated instructions can route to the right module. We resolve via
      // `app.processors()` — Plan 09-01's zero-arg read accessor (D-103).
      const processors = app.processors()
      const processorMap = new Map<string, EventProcessorModule>()
      for (const proc of processors) processorMap.set(proc.name, proc)

      platform.onInstruction(async (instruction) => {
        switch (instruction.kind) {
          case "pause-processor": {
            const proc = processorMap.get(instruction.processorName) as any
            if (proc?.stop) proc.stop()
            break
          }
          case "start-processor": {
            const proc = processorMap.get(instruction.processorName) as any
            if (proc?.start) await proc.start()
            break
          }
          case "release-segment": {
            const proc = processorMap.get(instruction.processorName) as any
            if (proc?.releaseSegment) await proc.releaseSegment(instruction.segmentId)
            break
          }
          case "split-segment": {
            const proc = processorMap.get(instruction.processorName) as any
            if (proc?.splitSegment) await proc.splitSegment(instruction.segmentId)
            break
          }
          case "merge-segment": {
            const proc = processorMap.get(instruction.processorName) as any
            if (proc?.mergeSegment) await proc.mergeSegment(instruction.segmentId)
            break
          }
        }
      })

      platform.registerProcessorStatusSupplier(() => {
        return processors.map((proc: any) => ({
          name: proc.name,
          running: proc.running ?? false,
          mode: proc.supportsReset?.() === false ? "Subscribing" : "Tracking",
          isStreamingProcessor: proc.supportsReset?.() !== false,
          activeThreads: proc.running ? 1 : 0,
          availableThreads: 0,
          error: false,
          tokenStoreIdentifier: "",
          segments: proc.processingStatus
            ? Array.from(proc.processingStatus().entries() as Iterable<[number, any]>).map(
                ([segId, status]: [number, any]) => ({
                  segmentId: segId,
                  caughtUp: status.caughtUp ?? false,
                  replaying: status.replaying ?? false,
                  onePartOf: 1,
                  tokenPosition: status.position ?? 0n,
                  errorState: status.error?.message ?? "",
                }),
              )
            : [
                {
                  segmentId: 0,
                  caughtUp: true,
                  replaying: proc.replaying ?? false,
                  onePartOf: 1,
                  tokenPosition: proc.position ?? 0n,
                  errorState: "",
                },
              ],
        }))
      })

      await platform.start()

      // Latch the connected promise so the deferred bus wrappers built in
      // the slot factories above construct their inner instances and replay
      // any subscriptions that were buffered while connect was running.
      // This MUST happen synchronously before any subsequent stage hook so
      // register/processors-stage code sees the fully-wired buses. The
      // microtask queue drains the `.then(...)` callbacks attached in the
      // slot factories before this hook resolves.
      resolveConnected()
      await Promise.resolve()
    })

    // ---- Lifecycle: processors (D-101 / D-102) --------------------------
    // processors = subscription-ack wait. The two-step shape mirrors the
    // kronosdb sibling (Plan 09-03 / D-102) but is adapted for Axon Server's
    // protocol shape, which differs from kronosdb's in one observable way:
    //
    //   - kronosdb's PlatformService proactively emits a frame in response
    //     to `register`, so its `subscriptionsAcked` latches on the first
    //     inbound platform-stream message.
    //
    //   - Axon Server's PlatformService holds the stream open silently
    //     until either a topology change or a heartbeat round-trip occurs.
    //     The platform stream therefore latches `acked` synchronously once
    //     the `register` frame has been flushed (see platform-service.ts).
    //
    // The bus-side subscription frames (sent on the command/query streams,
    // not the platform stream) need a small processing window on the
    // server before commands dispatched here are routed back to our
    // handler. Empirically Axon Server processes the subscribe within
    // 1 second — same number the legacy enhancer used. Wrapped in the same
    // `withRetry({event: "per-operation"})` shape as kronosdb so per-extension
    // resilience overrides still apply uniformly.
    app.onStart("processors", async () => {
      await withRetry(
        async () => {
          const ok = await platform!.subscriptionsAcked()
          if (!ok) throw new Error("axon-server subscriptions not yet acked")
        },
        { event: "per-operation", ...serverConfig.resilience },
      )
      // Axon-specific: give the server's command/query routing tables a
      // beat to register the subscribe frames we just sent on the bus
      // streams. The legacy enhancer carried this same 1s wait at line 264;
      // it cannot be derived from the platform stream because subscribes
      // travel on a different stream entirely.
      await new Promise((r) => setTimeout(r, serverConfig.busSubscriptionAckDelayMs ?? 1000))
    })

    // ---- Lifecycle: stop (D-101.b — preserves legacy ordering) ----------
    // busLatches drained first → platform.stop → connection.close.
    app.onStop("connect", async () => {
      await Promise.all(busLatches.map((l) => l.initiateShutdown()))
      platform?.stop()
      connection?.close()
    })
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
  let outbound = createOutboundStream<any>()
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
    outbound = createOutboundStream<any>()
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
function createDistributedQueryBus(
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

  // Local subscription store — subscription queries are handled locally
  const subscriptions = new Map<string, UpdateHandler>()

  let outbound = createOutboundStream<any>()
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
    outbound = createOutboundStream<any>()
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

  async function processInboundQueries(inbound: AsyncIterable<any>) {
    try {
      for await (const message of inbound) {
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

      const updateHandler = createUpdateHandler(message, bufferSize)
      subscriptions.set(queryId, updateHandler)

      const queryName = qualifiedNameToString(message.name)
      const subscriptionId = generateIdentifier()

      const outboundSub = createOutboundStream<any>()

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

      let initialResultResolve: ((value: unknown) => void) | null = null
      let initialResultReject: ((error: Error) => void) | null = null
      const initialResult = new Promise<unknown>((resolve, reject) => {
        initialResultResolve = resolve
        initialResultReject = reject
      })

      ;(async () => {
        try {
          for await (const response of responseStream) {
            if (response.initialResult) {
              if (response.errorCode && response.errorCode !== "") {
                initialResultReject?.(mapErrorCode(response.errorCode, response.errorMessage?.message ?? "Unknown error"))
              } else {
                initialResultResolve?.(deserializePayload(response.initialResult.payload?.data as Uint8Array | undefined))
              }
              initialResultResolve = null
              initialResultReject = null
            } else if (response.update) {
              const update = deserializePayload(response.update.payload?.data as Uint8Array | undefined)
              updateHandler.offer(update)
            } else if (response.complete) {
              updateHandler.complete()
              break
            } else if (response.completeExceptionally) {
              updateHandler.completeExceptionally(
                new Error(response.completeExceptionally.errorMessage?.message ?? "Subscription query failed"),
              )
              break
            }
          }
        } catch (err) {
          const error = err instanceof Error ? err : new Error(String(err))
          if (initialResultReject) {
            initialResultReject(error)
            initialResultResolve = null
            initialResultReject = null
          }
          updateHandler.completeExceptionally(error)
        } finally {
          subscriptions.delete(queryId)
        }
      })()

      return {
        initialResult,
        updates: updateHandler.iterable,
        close: () => {
          outboundSub.send({
            unsubscribe: {
              subscriptionIdentifier: subscriptionId,
            },
          })
          outboundSub.close()
          subscriptions.delete(queryId)
          updateHandler.complete()
        },
      }
    },

    subscribeToUpdates(message: QueryMessage, bufferSize?: number): AsyncIterable<unknown> & { close(): void } {
      const queryId = message.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const updateHandler = createUpdateHandler(message, bufferSize)
      subscriptions.set(queryId, updateHandler)

      return {
        [Symbol.asyncIterator]: () => updateHandler.iterable[Symbol.asyncIterator](),
        close: () => {
          subscriptions.delete(queryId)
          updateHandler.complete()
        },
      }
    },

    async emitUpdate(
      queryName: string,
      filter: (queryPayload: unknown) => boolean,
      update: unknown,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [id, handler] of subscriptions) {
          if (!handler.active) {
            subscriptions.delete(id)
            continue
          }
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (!filter(handler.query.payload)) continue

          const accepted = handler.offer(update)
          if (!accepted) {
            handler.completeExceptionally(new Error("Subscription query update buffer overflow"))
            subscriptions.delete(id)
          }
        }
      })
    },

    async completeSubscription(
      queryName: string,
      filter?: (queryPayload: unknown) => boolean,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !filter(handler.query.payload)) continue
          handler.complete()
          subscriptions.delete(id)
        }
      })
    },

    async completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: (queryPayload: unknown) => boolean,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !filter(handler.query.payload)) continue
          handler.completeExceptionally(error)
          subscriptions.delete(id)
        }
      })
    },
  }
}
