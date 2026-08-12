/**
 * KronosDB backend for @kronos-ts.
 *
 * An async factory that connects eagerly and hands back the components it
 * provides plus a `start`/`close` pair. There is no lifecycle framework and no
 * container: what used to be `onStart("connect")` now happens inside this
 * function, what used to be `onStart("processors")` is `start()`, and what used
 * to be `onStop("connect")` is `close()`. Dependencies that used to be resolved
 * lazily out of other slots (serializer, unitOfWorkFactory) are ordinary
 * arguments.
 *
 * ```ts
 * const kdb = await kronosDb({
 *   componentName: "university-service",
 *   serializer,
 *   unitOfWorkFactory,
 * })
 * const app = kronos({
 *   components: { ...inMemoryComponents(), ...kdb.components },
 *   modules,
 * })
 * await kdb.start()                   // subscription-ack wait, after handlers subscribe
 * // …
 * await app.stop(); await kdb.close()
 * ```
 *
 * Remote administration — KronosDB pushing pause / start / split / merge at this
 * client's processors, and this client reporting their status back for the admin
 * UI — is NOT part of the backend. It is opt-in, in `./control-plane.js`:
 *
 * ```ts
 * const control = kronosDbControlPlane(kdb.platform, app.processors)
 * ```
 *
 * Because the connection exists before any component is built, the stores and
 * buses are constructed against a live channel — the lazy proxies and the
 * subscribe()-buffering wrappers the container era needed are gone.
 */
import { generateIdentifier, qualifiedNameFromString, qualifiedNameToString, type Serializer, withRetry, healthCheck, type ResilienceConfig } from "@kronos-ts/common"
import type { CommandBus, CommandMessage, QueryBus, QueryMessage, SubscriptionFilter, SubscriptionQueryResult, UoWRunner, UpdateHandler } from "@kronos-ts/messaging"
import { applySubscriptionFilter, createUpdateHandler, runAfterCommitOrImmediately } from "@kronos-ts/messaging"
import type { KronosDbConnectionConfig } from "./connection.js"
import { connectToKronosDb, createKronosMetadata, type KronosDbConnection } from "./connection.js"
import { KronosDbErrorCode, mapErrorCode } from "./errors.js"
import { metadataFromProto, metadataToProto } from "./metadata-conversion.js"
import { createOutboundStream } from "./outbound-stream.js"
import { createPlatformConnection, type PlatformConnection, type PlatformServiceOptions } from "./platform-service.js"
import { createKronosDbEventStore } from "./kronosdb-event-store.js"
import { createKronosDbSnapshotStore } from "./kronosdb-snapshot-store.js"
import { createShutdownLatch, type ShutdownLatch } from "./shutdown-latch.js"

const DEFAULT_PERMITS = 5000n
const DEFAULT_THRESHOLD = 2500n

export interface FlowControlConfig {
  permits?: number
  refillThreshold?: number
}

export interface ProcessingInstructions {
  routingKey?: string
  priority?: number
  timeoutMs?: number
}

const INSTRUCTION_KEY = {
  ROUTING_KEY: 0,
  PRIORITY: 1,
  TIMEOUT: 2,
  NR_OF_RESULTS: 3,
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

function defaultQueryInstructions(timeoutMs: number): any[] {
  return [
    { key: INSTRUCTION_KEY.TIMEOUT, value: { numberValue: BigInt(timeoutMs) } },
    { key: INSTRUCTION_KEY.NR_OF_RESULTS, value: { numberValue: 1n } },
  ]
}

export interface KronosDbConfig extends KronosDbConnectionConfig {
  /**
   * Wire the distributed command/query buses backed by KronosDB.
   * Default: true.
   *
   * Set to false to use KronosDB purely as an event/snapshot store and bring
   * your own messaging transport. With `messaging: false` the returned
   * `components` carry only eventStore/snapshotStore, so spreading them over
   * your defaults leaves the local buses in place. The platform stream is
   * unaffected either way — it is neither persistence nor command/query routing,
   * and remote administration on top of it is opt-in via `kronosDbControlPlane`.
   */
  messaging?: boolean
  commandFlowControl?: FlowControlConfig
  queryFlowControl?: FlowControlConfig
  platformService?: PlatformServiceOptions
  shortcutQueriesToLocalHandlers?: boolean
  commandLoadFactor?: number
  commandTimeoutMs?: number
  queryTimeoutMs?: number
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
}

/**
 * The components a KronosDB backend provides. Spread over your defaults:
 *
 * ```ts
 * kronos({ components: { ...inMemoryComponents(), ...kdb.components }, modules })
 * ```
 *
 * `commandBus` / `queryBus` are absent when `messaging: false`, so the spread
 * leaves whatever transport you already had in place.
 */
export interface KronosDbComponents {
  eventStore: ReturnType<typeof createKronosDbEventStore>
  snapshotStore: ReturnType<typeof createKronosDbSnapshotStore>
  commandBus?: CommandBus
  queryBus?: QueryBus
}

/** A connected KronosDB backend. See {@link kronosDb}. */
export interface KronosDbBackend {
  readonly components: KronosDbComponents
  /** The live connection, for callers that need the raw client. */
  readonly connection: KronosDbConnection
  /**
   * The platform stream. Persistence and transport do not use it; it is public
   * so the optional control plane can be handed it:
   * `kronosDbControlPlane(kdb.platform, app.processors)`.
   */
  readonly platform: PlatformConnection
  /**
   * Wait until KronosDB has acknowledged this client's registration, i.e. until
   * handler subscriptions are routable. Call AFTER every handler is subscribed
   * (after `kronos`). This is the D-102 replacement for the legacy
   * 1-second sleep — it waits exactly long enough, no longer.
   *
   * This is the readiness barrier and nothing else. Remote administration is
   * `kronosDbControlPlane`, which is opt-in and takes no part in startup.
   */
  start(): Promise<void>
  /** Drain in-flight bus work, stop the platform stream, close the connection. */
  close(): Promise<void>
}

/** Arguments a KronosDB backend cannot make up for itself. */
/** Everything kronosDb() needs: its own config plus the framework values it borrows. */
export type KronosDbOptions = KronosDbConfig & KronosDbDependencies

export interface KronosDbDependencies {
  serializer: Serializer
  /** The same UoW runner the app runs on — inbound commands/queries run inside it. */
  unitOfWorkFactory: UoWRunner
}

/**
 * Connect to KronosDB and build its components.
 *
 * Everything the connect stage used to do — connect under `withRetry`,
 * health-check and platform setup — happens
 * here, awaited, before the function returns. Ordering that used to be encoded
 * in framework stages is now written down in your composition root.
 *
 * ```ts
 * const kdb = await kronosDb({
 *   componentName: "university-service",
 *   serializer, unitOfWorkFactory,
 * })
 * const app = kronos({
 *   components: { ...inMemoryComponents(), ...kdb.components },
 *   modules,
 * })
 * await kdb.start()
 * ```
 *
 * With `messaging: false` only the stores come back, so another transport (or
 * the in-memory defaults) keeps the buses:
 *
 * ```ts
 * const kdb = await kronosDb({ componentName: "svc", messaging: false, serializer, unitOfWorkFactory })
 * const app = kronos({ components: { ...inMemoryComponents(), ...kdb.components }, modules })
 * ```
 */
export async function kronosDb(options: KronosDbOptions): Promise<KronosDbBackend> {
  const config = options
  const { serializer, unitOfWorkFactory, resilience } = config
  const busLatches: ShutdownLatch[] = []

  const connection: KronosDbConnection = await withRetry(
    async () => connectToKronosDb(config),
    { event: "initial-connect", ...resilience },
  )

  // Health-check ping with warn-then-continue (D-100). KronosDbConnection has
  // no dedicated probe surface today; the gRPC channel itself is created
  // eagerly in connectToKronosDb so the meaningful probe is a round-trip — we
  // approximate via a soft no-op promise that satisfies the threshold
  // contract. Real network failure is surfaced by the first bus call against
  // the live channel.
  await healthCheck(async () => undefined, {
    thresholdMs: resilience?.healthCheckThresholdMs,
    log: resilience?.log,
  })

  // ---- Platform control plane -------------------------------------------
  const platform = createPlatformConnection(connection, config.platformService)


  // ---- Components --------------------------------------------------------
  // The connection is live by now, so these are the real things: no lazy
  // proxy around the stores, no subscribe()-buffering wrapper around the buses.
  const components: KronosDbComponents = {
    eventStore: createKronosDbEventStore(connection, serializer),
    snapshotStore: createKronosDbSnapshotStore(connection, serializer),
  }

  if (config.messaging !== false) {
    const commandLatch = createShutdownLatch()
    busLatches.push(commandLatch)
    components.commandBus = createDistributedCommandBus(
      connection,
      unitOfWorkFactory,
      commandLatch,
      serializer,
      config.commandFlowControl,
      config.commandLoadFactor,
      resilience,
    )

    const queryLatch = createShutdownLatch()
    busLatches.push(queryLatch)
    components.queryBus = createDistributedQueryBus(
      connection,
      unitOfWorkFactory,
      queryLatch,
      serializer,
      config.queryFlowControl,
      config.shortcutQueriesToLocalHandlers,
      config.queryTimeoutMs,
      resilience,
    )
  }

  return {
    components,
    connection,
    platform,
    async start() {
      // The readiness barrier needs a LIVE platform stream: the ack signal is
      // the first server-originated frame on it, so `subscriptionsAcked()` is
      // `false` until the stream is open. `platform.start()` is idempotent — if
      // a control plane was created first (the recommended order) it already
      // brought the stream live AFTER registering its handlers, and this is a
      // no-op. Without a control plane the backend still needs the stream, so it
      // starts it here. Instructions that land before a control plane registers
      // are buffered by the platform connection, not dropped.
      await platform.start()

      await withRetry(
        async () => {
          const ok = await platform.subscriptionsAcked()
          if (!ok) throw new Error("subscriptions not yet acked")
        },
        { event: "per-operation", ...resilience },
      )
    },
    async close() {
      // Ordering preserved from D-101.b: drain buses, then platform, then socket.
      await Promise.all(busLatches.map((l) => l.initiateShutdown()))
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
// ---------------------------------------------------------------------------

function createDistributedCommandBus(
  connection: KronosDbConnection,
  unitOfWorkRunner: UoWRunner,
  shutdownLatch: ShutdownLatch,
  serializer: Serializer,
  flowControl?: FlowControlConfig,
  commandLoadFactor?: number,
  resilience?: Partial<ResilienceConfig>,
): CommandBus {
  const metadata = createKronosMetadata(connection.config)
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))

  const localSegment = new Map<string, (message: CommandMessage) => Promise<unknown>>()

  let outbound = createOutboundStream<any>()
  let streamStarted = false
  let permits = 0n

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

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

  function reestablishStreamBody() {
    outbound.close()
    outbound = createOutboundStream<any>()
    streamStarted = false
    permits = 0n
    ensureStreamStarted()
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
    grantPermits()
  }

  async function reestablishStreamWithRetry() {
    if (shutdownLatch.shuttingDown) return
    await withRetry(async () => reestablishStreamBody(), {
      event: "reconnect",
      ...resilience,
    })
  }

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
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined, proto.payload?.type, proto.payload?.revision),
              metadata: metadataFromProto(proto.metadata ?? {}),
              timestamp: Number(proto.timestamp),
            }

            resultPayload = await unitOfWorkRunner(commandMessage.metadata, () =>
              handler(commandMessage),
            )
          } catch (err) {
            errorCode = KronosDbErrorCode.COMMAND_EXECUTION_ERROR
            errorMsg = err instanceof Error ? err.message : String(err)
          }
        } else {
          errorCode = KronosDbErrorCode.NO_HANDLER_FOR_COMMAND
          errorMsg = `No local handler for command "${commandName}"`
        }

        const responseSerialized = resultPayload !== undefined
          ? serializePayload("result", resultPayload)
          : undefined

        outbound.send({
          commandResponse: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
              : undefined,
            payload: responseSerialized,
            metadata: {},
            processingInstructions: [],
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
        const serialized = serializePayload(commandName, message.payload)

        const response = await connection.commands.dispatch({
          messageIdentifier: message.identifier,
          name: commandName,
          timestamp: BigInt(message.timestamp),
          payload: serialized,
          metadata: metadataToProto(message.metadata),
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

        return deserializePayload(response.payload?.data as Uint8Array | undefined, response.payload?.type, response.payload?.revision)
      } finally {
        activity.end()
      }
    },

    subscribe(commandName: string, handler: (message: CommandMessage) => Promise<unknown>) {
      localSegment.set(commandName, handler)

      ensureStreamStarted()
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
      grantPermits()
    },
  }
}

// ---------------------------------------------------------------------------
// Distributed Query Bus
// ---------------------------------------------------------------------------

export function createDistributedQueryBus(
  connection: KronosDbConnection,
  unitOfWorkRunner: UoWRunner,
  shutdownLatch: ShutdownLatch,
  serializer: Serializer,
  flowControl?: FlowControlConfig,
  shortcutQueriesToLocalHandlers?: boolean,
  queryTimeoutMs?: number,
  resilience?: Partial<ResilienceConfig>,
): QueryBus {
  const metadata = createKronosMetadata(connection.config)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)

  const localSegment = new Map<string, (message: QueryMessage) => Promise<unknown>>()
  const subscriptions = new Map<string, UpdateHandler>()
  // Subscriptions the SERVER has routed to this instance as the handler. Each
  // entry was opened by some subscriber (possibly remote) for a query name we
  // registered as a handler for. emitUpdate / completeSubscription apply the
  // caller-supplied filter against these to decide which subscriber IDs to
  // target. The server then routes each response back to that exact subscriber.
  const handlerSubscriptions = new Map<string, { queryName: string; payload: unknown }>()

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
            metadata: metadataFromProto(proto.metadata ?? {}),
            timestamp: Number(proto.timestamp),
          }
          resultPayload = await unitOfWorkRunner(queryMessage.metadata, async () => {
            return handler(queryMessage)
          })
        } catch (err) {
          errorCode = KronosDbErrorCode.QUERY_EXECUTION_ERROR
          errorMsg = err instanceof Error ? err.message : String(err)
        }
      } else {
        errorCode = KronosDbErrorCode.NO_HANDLER_FOR_QUERY
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
            metadata: {},
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
    // flowControl is silently ignored; the bus doesn't track per-sub permits today.
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
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined, proto.payload?.type, proto.payload?.revision),
              metadata: metadataFromProto(proto.metadata ?? {}),
              timestamp: Number(proto.timestamp),
            }

            resultPayload = await unitOfWorkRunner(queryMessage.metadata, async () => {
              return handler(queryMessage)
            })
          } catch (err) {
            errorCode = KronosDbErrorCode.QUERY_EXECUTION_ERROR
            errorMsg = err instanceof Error ? err.message : String(err)
          }
        } else {
          errorCode = KronosDbErrorCode.NO_HANDLER_FOR_QUERY
          errorMsg = `No local handler for query "${queryName}"`
        }

        const responseSerialized = resultPayload !== undefined
          ? serializePayload("result", resultPayload)
          : undefined

        outbound.send({
          queryResponse: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
              : undefined,
            payload: responseSerialized,
            metadata: {},
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

        if (shortcutQueriesToLocalHandlers) {
          const localHandler = localSegment.get(queryName)
          if (localHandler) {
            return unitOfWorkRunner(message.metadata, async () => {
              return localHandler(message)
            })
          }
        }

        const serialized = serializePayload(queryName, message.payload)

        const responseStream = connection.queries.query({
          messageIdentifier: message.identifier,
          query: queryName,
          timestamp: BigInt(message.timestamp),
          payload: serialized,
          metadata: metadataToProto(message.metadata),
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
          return deserializePayload(response.payload?.data as Uint8Array | undefined, response.payload?.type, response.payload?.revision)
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
      const serialized = serializePayload(queryName, message.payload)

      const outboundSub = createOutboundStream<any>()

      outboundSub.send({
        subscribe: {
          subscriptionIdentifier: subscriptionId,
          numberOfPermits: BigInt(bufferSize ?? 256),
          queryRequest: {
            messageIdentifier: message.identifier,
            query: queryName,
            timestamp: BigInt(message.timestamp),
            payload: serialized,
            metadata: metadataToProto(message.metadata),
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
              if (!initialSettled) {
                if (response.initialResult.errorCode && response.initialResult.errorCode !== "") {
                  rejectInitial(mapErrorCode(response.initialResult.errorCode, response.initialResult.errorMessage?.message ?? "Unknown error"))
                } else {
                  resolveInitial(deserializePayload(response.initialResult.payload?.data as Uint8Array | undefined, response.initialResult.payload?.type, response.initialResult.payload?.revision))
                }
                initialSettled = true
              }
            } else if (response.update) {
              const update = deserializePayload(response.update.payload?.data as Uint8Array | undefined, response.update.payload?.type, response.update.payload?.revision)
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
          if (!initialSettled) {
            rejectInitial(error)
            initialSettled = true
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
                metadata: {},
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
                errorCode: KronosDbErrorCode.QUERY_EXECUTION_ERROR,
                errorMessage: {
                  message: error.message,
                  location: connection.config.componentName,
                  details: [],
                  errorCode: KronosDbErrorCode.QUERY_EXECUTION_ERROR,
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
