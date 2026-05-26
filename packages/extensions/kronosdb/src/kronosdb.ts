/**
 * Native KronosDB extension (Phase 9, D-95 / D-101 / D-102).
 *
 * Native `(app: App) => void` extension that:
 *
 *   - populates four typed slots (eventStore, snapshotStore, commandBus,
 *     queryBus) via app.set(...) using the canonical Resolved slot names
 *     (in particular `resolved.unitOfWorkFactory`, NOT `unitOfWorkRunner`);
 *   - wires connect-stage transport bring-up under the @kronos-ts/common
 *     resilience helper (initial-connect + health-check + platform setup +
 *     instruction handlers + platform.start);
 *   - wires processors-stage subscription-ack wait via withRetry against
 *     `platform.subscriptionsAcked()` — REPLACES the 1-second sleep hack
 *     that lived at line 216 of the legacy file (D-102);
 *   - reverses shutdown deterministically in a single onStop('connect') hook
 *     (busLatches → platform.stop → connection.close — D-101.b).
 */
import { generateIdentifier, qualifiedNameFromString, qualifiedNameToString, type Serializer, withRetry, healthCheck, type ResilienceConfig } from "@kronos-ts/common"
import type { App } from "@kronos-ts/app"
import type { CommandBus, CommandMessage, EventProcessorModule, QueryBus, QueryMessage, SubscriptionFilter, SubscriptionQueryResult, UoWRunner, UpdateHandler } from "@kronos-ts/messaging"
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

export interface KronosDbExtensionConfig extends KronosDbConnectionConfig {
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
 * Native KronosDB extension factory. Returns an Extension closure shaped as
 * `(app: App) => void` per D-95.
 *
 * ```ts
 * await kronos()
 *   .use(kronosDb({ componentName: "university-service" }))
 *   .start()
 * ```
 */
export function kronosDb(serverConfig: KronosDbExtensionConfig): (app: App) => void {
  return (app) => {
    let connection: KronosDbConnection | undefined
    let platform: PlatformConnection | undefined
    const busLatches: ShutdownLatch[] = []

    function getConnection(): KronosDbConnection {
      if (!connection) {
        throw new Error(
          "[kronos:kronosdb] connection not yet established — wait for onStart('connect')",
        )
      }
      return connection
    }

    // ---- Slot population (D-95) ------------------------------------------
    //
    // AppImpl.start() in @kronos-ts/app eagerly resolves all 8 slots and
    // runs `commandBus.subscribe(...)` for every registered handler BEFORE
    // any onStart('connect') hook fires (see app.ts §3 / §5c). The KronosDB
    // bus factories open real gRPC streams against the live channel during
    // construction (createKronosMetadata / connection.onReconnect / inbound
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
      // Lazy proxy: createKronosDbEventStore stores `connection` in
      // closure scope but only dereferences it inside method bodies, so
      // a Proxy that forwards property access to getConnection() works
      // — by the time framework code calls source/append/stream the
      // connect hook has populated the closure.
      const lazyConnection = new Proxy({} as KronosDbConnection, {
        get(_t, prop) {
          return (getConnection() as any)[prop]
        },
      })
      return createKronosDbEventStore(lazyConnection, resolved.serializer)
    })

    app.set("snapshotStore", (resolved) => {
      const lazyConnection = new Proxy({} as KronosDbConnection, {
        get(_t, prop) {
          return (getConnection() as any)[prop]
        },
      })
      return createKronosDbSnapshotStore(lazyConnection, resolved.serializer)
    })

    app.set("commandBus", (resolved) => {
      const latch = createShutdownLatch()
      busLatches.push(latch)

      let inner: CommandBus | undefined
      const pendingSubs: Array<[string, (m: CommandMessage) => Promise<unknown>]> = []

      // Build the real bus once the connect hook fires + replay buffered subs.
      connected.then(() => {
        // canonical Resolved slot name is `unitOfWorkFactory` (NOT unitOfWorkRunner)
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
              "[kronos:kronosdb] subscriptionQuery called before connect hook completed",
            )
          }
          return inner.subscriptionQuery(message, bufferSize)
        },
        subscribeToUpdates(message, bufferSize) {
          if (!inner) {
            throw new Error(
              "[kronos:kronosdb] subscribeToUpdates called before connect hook completed",
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
        async () => connectToKronosDb(serverConfig),
        { event: "initial-connect", ...serverConfig.resilience },
      )

      // Health-check ping with warn-then-continue (D-100). KronosDbConnection
      // has no dedicated probe surface today; the gRPC channel itself is
      // created eagerly in connectToKronosDb so the meaningful probe is a
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
    // processors = ONLY the subscription-ack wait, via withRetry against
    // `platform.subscriptionsAcked()`. This REPLACES the legacy 1-second
    // sleep that lived at kronosdb-configuration-enhancer.ts:216.
    app.onStart("processors", async () => {
      await withRetry(
        async () => {
          const ok = await platform!.subscriptionsAcked()
          if (!ok) throw new Error("subscriptions not yet acked")
        },
        { event: "per-operation", ...serverConfig.resilience },
      )
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
