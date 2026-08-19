/**
 * KronosDB backend for @kronos-ts.
 *
 * The gRPC channel is a RESOURCE — one socket, one platform stream, one
 * readiness barrier — so it is named: {@link kronosDbConnection}. Everything
 * else is a FUNCTION over it, one per seam:
 *
 * ```ts
 * const kdb = await kronosDbConnection({ componentName: "university-service", serializer })
 *
 * const eventStore    = kronosDbEventStore(kdb, "billing")
 * const snapshotStore = kronosDbSnapshotStore(kdb, "billing")
 * const commandBus    = interceptingCommandBus(
 *   kronosDbCommandBus(kdb, simpleCommandBus(unitOfWork)), lineage)
 * const queryBus      = interceptingQueryBus(
 *   kronosDbQueryBus(kdb, simpleQueryBus(unitOfWork)), lineage)
 *
 * const app = kronos({ commandHandlers, queryHandlers, states })
 * await kdb.start()                   // subscription-ack wait, after handlers subscribe
 * // …
 * await app.stop(); await kdb.close()
 * ```
 *
 * There used to be a `kronosDbContext(kdb, options)` in the middle, returning a
 * record of four components at once. It is gone: a context is not a thing you
 * build, it is a STRING two of these functions take. `kronosDbEventStore(kdb,
 * "billing")` and `kronosDbEventStore(kdb, "catalog")` still share the one
 * socket — the per-call `kronosdb-context` header is the whole difference — so
 * a process addressing nineteen contexts still opens one channel, and now the
 * caller who wants only an event store builds only an event store.
 *
 * There is no lifecycle framework and no container: what used to be
 * `onStart("connect")` happens inside `kronosDbConnection`, what used to be
 * `onStart("processors")` is its `start()`, and what used to be
 * `onStop("connect")` is its `close()`.
 *
 * Remote administration — KronosDB pushing pause / start / split / merge at this
 * client's processors, and this client reporting their status back for the admin
 * UI — is NOT part of either. It is opt-in, in `./control-plane.js`:
 *
 * ```ts
 * const control = kronosDbControlPlane(kdb, app.processors)
 * ```
 *
 * Because the connection exists before any component is built, the stores and
 * buses are constructed against a live channel — the lazy proxies and the
 * subscribe()-buffering wrappers the container era needed are gone.
 */
import { generateIdentifier, qualifiedNameFromString, qualifiedNameToString, type Serializer, withRetry, healthCheck, type ResilienceConfig } from "@kronos-ts/core"
import type { CommandBus, CommandMessage, QueryBus, QueryMessage, SubscriptionFilter, SubscriptionQueryResult, UnitOfWork, Unstamped, UpdateHandler } from "@kronos-ts/core"
import {
  applySubscriptionFilter,
  stamped,
  updateHandler,
  runAfterCommitOrImmediately,
} from "@kronos-ts/core"
import type { KronosDbConnectionConfig } from "./connection.js"
import { connectToKronosDb, contextView, kronosMetadata, type KronosDbConnection } from "./connection.js"
import { KronosDbErrorCode, mapErrorCode } from "./errors.js"
import { metadataFromProto, metadataToProto } from "./metadata-conversion.js"
import { outboundStream } from "./outbound-stream.js"
import { platformConnection, type PlatformConnection, type PlatformServiceOptions } from "./platform-service.js"
import { shutdownLatch as shutdownLatchValue, type ShutdownLatch } from "./shutdown-latch.js"

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

/** Per-bus routing knobs. Everything shared lives on the connection. */
export interface KronosDbCommandBusOptions {
  /** Which KronosDB context to address. Defaults to the connection's own. */
  context?: string
  flowControl?: FlowControlConfig
  /** Relative share of routed work this instance advertises. Default: 100. */
  loadFactor?: number
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
}

/** @see KronosDbCommandBusOptions */
export interface KronosDbQueryBusOptions {
  /** Which KronosDB context to address. Defaults to the connection's own. */
  context?: string
  flowControl?: FlowControlConfig
  /** Answer from a co-located handler instead of going out to the server. */
  shortcutQueriesToLocalHandlers?: boolean
  /** Server-side query timeout. Default: 3600000. */
  timeoutMs?: number
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
}

/**
 * Connection-level options: the socket, the platform stream, how payloads are
 * encoded on this wire, and how hard to retry.
 *
 * The SERIALIZER is here rather than on each store and bus because it is a
 * property of this client's wire — one channel, one encoding — and because a
 * store keyed by `(connection, context)` has nowhere honest to put it.
 */
export interface KronosDbConnectionOptions extends KronosDbConnectionConfig {
  serializer: Serializer
  platformService?: PlatformServiceOptions
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
}

/**
 * The RESOURCE a KronosDB deployment shares: ONE gRPC channel, the platform
 * stream layered on it, and the lifecycle that arms and drains them.
 *
 * Contexts are functions over this — `kronosDbContext(connection, { context })`
 * — so a process addressing nineteen contexts opens one socket, not nineteen.
 * The per-call `kronosdb-context` header is what separates them, and it is set
 * per context handle rather than baked into the channel.
 */
export interface KronosDbConnectionHandle {
  /** The live connection, for callers that need the raw gRPC clients. */
  readonly connection: KronosDbConnection
  /** How payloads are encoded on this wire — shared by every store and bus. */
  readonly serializer: Serializer
  /**
   * The platform stream. Persistence and transport do not use it; it is public
   * so the optional control plane can be handed it:
   * `kronosDbControlPlane(kdb, app.processors)`.
   */
  readonly platform: PlatformConnection
  /**
   * Contexts register their bus drain latches here so {@link close} covers every
   * context opened on this connection.
   *
   * @internal
   */
  readonly registerShutdownLatch: (latch: ShutdownLatch) => void
  /**
   * Wait until KronosDB has acknowledged this client's handler, i.e. until
   * handler subscriptions are routable. Call AFTER every handler is subscribed
   * (after `kronos`), ONCE, no matter how many contexts you opened — the ack is
   * a property of the connection, not of a context. Idempotent: concurrent and
   * repeat calls share the first barrier. This is the D-102 replacement for the
   * legacy 1-second sleep — it waits exactly long enough, no longer.
   *
   * This is the readiness barrier and nothing else. Remote administration is
   * `kronosDbControlPlane`, which is opt-in and takes no part in startup.
   */
  start(): Promise<void>
  /** Drain every context's in-flight bus work, stop the platform stream, close the channel. */
  close(): Promise<void>
}

/**
 * Open the KronosDB connection.
 *
 * Everything the connect stage used to do — connect under `withRetry`,
 * health-check and platform setup — happens here, awaited, before the function
 * returns. Ordering that used to be encoded in framework stages is now written
 * down in your composition root.
 *
 * ```ts
 * const kdb = await kronosDbConnection({ componentName: "university-service", serializer })
 *
 * const eventStore = kronosDbEventStore(kdb, "billing")
 * const commandBus = interceptingCommandBus(
 *   kronosDbCommandBus(kdb, simpleCommandBus(unitOfWork)), lineage)
 * const queryBus = interceptingQueryBus(
 *   kronosDbQueryBus(kdb, simpleQueryBus(unitOfWork)), lineage)
 *
 * const app = kronos({ commandHandlers, queryHandlers, states })
 * await kdb.start()                   // subscription-ack wait, after handlers subscribe
 * // …
 * await app.stop(); await kdb.close()
 * ```
 */
export async function kronosDbConnection(
  options: KronosDbConnectionOptions,
): Promise<KronosDbConnectionHandle> {
  const { resilience, serializer } = options
  const busLatches: ShutdownLatch[] = []

  const connection: KronosDbConnection = await withRetry(
    async () => connectToKronosDb(options),
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

  const platform = platformConnection(connection, options.platformService)

  // Memoised so N contexts (or a caller who simply calls it twice) share ONE
  // barrier. The ack is connection-scoped: it says KronosDB accepted this
  // client's handler, which covers every subscription frame sent on the
  // channel regardless of which context it named.
  let started: Promise<void> | undefined

  return {
    connection,
    serializer,
    platform,
    registerShutdownLatch(latch) {
      busLatches.push(latch)
    },
    async start() {
      // ASYMMETRY WITH axon-server, ON PURPOSE. The axon backend has a
      // `platform.armConnectionMonitoring()` split — a data-path entry point
      // that opens the platform stream and arms the heartbeat WITHOUT arming
      // processor status reporting. KronosDB does not need one: its readiness
      // barrier (`subscriptionsAcked()`) can only be answered by a live platform
      // stream, so this connection has always called `platform.start()` itself,
      // and the heartbeat that drives `connection.reconnect()` on timeout has
      // always been armed on the data path regardless of whether anyone built a
      // `kronosDbControlPlane`.
      //
      // Axon's exposure came from the opposite coupling: its barrier is a plain
      // settle wait on the BUS streams, so nothing on its data path had reason
      // to touch the platform stream, and after the control-plane extraction an
      // un-administered service ended up with no reconnect detection at all.
      //
      // The cost of not splitting here is one idle timer: `platform.start()`
      // also arms status reporting, and `reportProcessorStatus()` returns
      // immediately while `processorStatusSuppliers` is empty. Harmless, and a
      // control plane created later is picked up live because the reporter reads
      // the supplier list on every tick rather than capturing it.
      //
      // The readiness barrier needs a LIVE platform stream: the ack signal is
      // the first server-originated frame on it, so `subscriptionsAcked()` is
      // `false` until the stream is open. `platform.start()` is idempotent — if
      // a control plane was created first (the recommended order) it already
      // brought the stream live AFTER registering its handlers, and this is a
      // no-op. Without a control plane the connection still needs the stream, so
      // it starts it here. Instructions that land before a control plane
      // registers are buffered by the platform connection, not dropped.
      started ??= (async () => {
        await platform.start()
        await withRetry(
          async () => {
            const ok = await platform.subscriptionsAcked()
            if (!ok) throw new Error("subscriptions not yet acked")
          },
          { event: "per-operation", ...resilience },
        )
      })()
      return started
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
// KronosDB Command Bus
//
// Bus implementation moved verbatim from the legacy enhancer with TWO
// behavioural additions per D-97:
//   1) reestablishStream() body wrapped in withRetry({ event: "reconnect" })
//   2) inbound-stream backoff replaced by the same withRetry path
// ---------------------------------------------------------------------------

/**
 * A command bus backed by KronosDB.
 *
 * ## Correlation lineage and the interceptor layer
 *
 * The returned bus stamps no lineage of its own. A host that wants it wraps
 * the OUTERMOST bus with `interceptingCommandBus(bus, lineage)`, so whatever
 * metadata providers a host adds run BEFORE the message is serialized onto the
 * wire. Lineage itself is already on `message.metadata` by then — `ctx.send`
 * stamps the unit of work's correlation data before any bus sees the message.
 *
 * This mirrors AxonFramework, where dispatch interception always sits outside
 * the routing bus. AF4's `AxonServerCommandBus.dispatch` is
 * `doDispatch(dispatchInterceptors.intercept(commandMessage), cb)`; AF5 expresses
 * the same thing through decorator order —
 * `DISTRIBUTED_COMMAND_BUS_ORDER = InterceptingCommandBus.DECORATION_ORDER - 50`
 * stacks the buses `InterceptingCommandBus → DistributedCommandBus → SimpleCommandBus`.
 *
 * Without this, remote lineage was simply lost: the interceptor was registered
 * only inside `@kronos-ts/core`'s default in-memory bus, and this bus REPLACES
 * that one in `components` — every command left the process with no
 * `correlationId` / `causationId`, even though the inbound side below faithfully
 * rebuilds a UnitOfWork from `message.metadata`.
 *
 * Double application of `lineage` is harmless: both of its fields are `??`
 * seeds, so a `local` that is itself intercepting simply sees them already set.
 *
 * ## The local segment is a real bus
 *
 * `local` is a `CommandBus`, not a private handler map. `subscribe` registers
 * on it AND announces the name to the server; a command the SERVER routes back
 * here is dispatched into it. That is what makes the unit-of-work policy you
 * chose for `local` — `postgresUnitOfWork(pg, unitOfWork)`, say — apply to
 * server-routed work exactly as it applies to anything else. It also removes
 * the `unitOfWork` parameter this function used to take: `local` carries that
 * policy now, and having it twice was a way to disagree with yourself.
 *
 * Server-side routing is unchanged: KronosDB is a smart hub, so an outbound
 * dispatch ALWAYS goes to the server, even for a command this instance handles.
 * There is no client-side prefer-local fork here (that is RabbitMQ's model).
 */
export function kronosDbCommandBus(
  kdb: Pick<KronosDbConnectionHandle, "connection" | "serializer" | "registerShutdownLatch">,
  local: CommandBus,
  options: KronosDbCommandBusOptions = {},
): CommandBus {
  const { flowControl, loadFactor: commandLoadFactor, resilience } = options
  const serializer = kdb.serializer
  const connection = contextView(kdb.connection, options.context ?? kdb.connection.config.context)
  const shutdownLatch = shutdownLatchValue()
  kdb.registerShutdownLatch(shutdownLatch)

  const metadata = kronosMetadata(connection.config)
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))

  // Names this instance announced. Kept so a reconnect can re-announce them and
  // so an unroutable inbound command still gets NO_HANDLER_FOR_COMMAND rather
  // than whatever `local.dispatch` happens to throw for an unknown name.
  const localHandlers = new Set<string>()

  let outbound = outboundStream<any>()
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
    outbound = outboundStream<any>()
    streamStarted = false
    permits = 0n
    ensureStreamStarted()
    for (const commandName of localHandlers) {
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
        console.error("KronosDB command bus: reconnect retries exhausted", err)
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

        let resultPayload: unknown
        let errorCode = ""
        let errorMsg = ""

        if (localHandlers.has(commandName)) {
          try {
            const commandMessage: CommandMessage = {
              kind: "command",
              identifier: proto.messageIdentifier,
              name: qualifiedNameFromString(commandName),
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined, proto.payload?.type, proto.payload?.revision),
              metadata: metadataFromProto(proto.metadata ?? {}),
              timestamp: Number(proto.timestamp),
            }

            // Into the LOCAL BUS, not a privately-held handler reference: the
            // unit-of-work policy `local` was built with governs server-routed
            // work exactly as it governs everything else.
            resultPayload = await local.dispatch(commandMessage)
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

      console.error("KronosDB command bus: inbound stream error, attempting re-establishment via withRetry", err)
      await reestablishStreamWithRetry().catch((retryErr) => {
        console.error("KronosDB command bus: reconnect retries exhausted", retryErr)
      })
    }
  }

  const routing: CommandBus = {
    async dispatch(unstamped: Unstamped<CommandMessage>): Promise<unknown> {
      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire still {@link Unstamped} is therefore
      // stamped from system time here — the envelope crosses a process boundary
      // and must be fully formed. A locally-shortcut message is handed to
      // `local` unstamped instead, so the task that handles it supplies the
      // instant.
      const message = stamped(unstamped, Date.now)
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

    subscribe(commandName, handler) {
      localHandlers.add(commandName)
      local.subscribe(commandName, handler)

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

  // Interception OUTSIDE routing — see the note on this function.
  return routing
}

// ---------------------------------------------------------------------------
// KronosDB Query Bus
// ---------------------------------------------------------------------------

/**
 * A query bus backed by KronosDB.
 *
 * Lineage, if wanted, is `interceptingQueryBus(bus, lineage)` at the host, for the same reason
 * {@link kronosDbCommandBus} is wrapped — AF runs dispatch interception at the
 * top of `query` / `scatterGather` /
 * `subscriptionQuery`, before anything is sent. `query()` below can also shortcut
 * to a co-located handler, and wrapping outside means lineage is stamped
 * identically on both branches.
 *
 * KNOWN GAP: `subscriptionQuery` / `subscribeToUpdates` build their proto
 * straight from `message.metadata`, and `interceptingQueryBus` (in
 * `@kronos-ts/core`) forwards those two calls to the delegate without
 * running the dispatch chain. Subscription handlers therefore still travel
 * without lineage. Closing that needs a change in the messaging package.
 */
export function kronosDbQueryBus(
  kdb: Pick<KronosDbConnectionHandle, "connection" | "serializer" | "registerShutdownLatch">,
  local: QueryBus,
  options: KronosDbQueryBusOptions = {},
): QueryBus {
  const {
    flowControl,
    shortcutQueriesToLocalHandlers,
    timeoutMs: queryTimeoutMs,
    resilience,
  } = options
  const serializer = kdb.serializer
  const connection = contextView(kdb.connection, options.context ?? kdb.connection.config.context)
  const shutdownLatch = shutdownLatchValue()
  kdb.registerShutdownLatch(shutdownLatch)

  const metadata = kronosMetadata(connection.config)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)

  // As on the command side: names announced to the server, kept so a reconnect
  // can re-announce them and an unroutable inbound query still answers
  // NO_HANDLER_FOR_QUERY.
  const localHandlers = new Set<string>()
  const subscriptions = new Map<string, UpdateHandler>()
  // Subscriptions the SERVER has routed to this instance as the handler. Each
  // entry was opened by some subscriber (possibly remote) for a query name we
  // registered as a handler for. emitUpdate / completeSubscription apply the
  // caller-supplied filter against these to decide which subscriber IDs to
  // target. The server then routes each response back to that exact subscriber.
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

  function reestablishStreamBody() {
    outbound.close()
    outbound = outboundStream<any>()
    streamStarted = false
    permits = 0n
    ensureStreamStarted()
    for (const queryName of localHandlers) {
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
        console.error("KronosDB query bus: reconnect retries exhausted", err)
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

      let resultPayload: unknown
      let errorCode = ""
      let errorMsg = ""

      if (localHandlers.has(queryName)) {
        try {
          const queryMessage: QueryMessage = {
            kind: "query",
            identifier: proto.messageIdentifier,
            name: qualifiedNameFromString(queryName),
            payload,
            metadata: metadataFromProto(proto.metadata ?? {}),
            timestamp: Number(proto.timestamp),
          }
          // Into the LOCAL BUS — see the note on kronosDbCommandBus.
          resultPayload = await local.query(queryMessage)
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

        let resultPayload: unknown
        let errorCode = ""
        let errorMsg = ""

        if (localHandlers.has(queryName)) {
          try {
            const queryMessage: QueryMessage = {
              kind: "query",
              identifier: proto.messageIdentifier,
              name: qualifiedNameFromString(queryName),
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined, proto.payload?.type, proto.payload?.revision),
              metadata: metadataFromProto(proto.metadata ?? {}),
              timestamp: Number(proto.timestamp),
            }

            // Into the LOCAL BUS — see the note on kronosDbCommandBus.
            resultPayload = await local.query(queryMessage)
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

      console.error("KronosDB query bus: inbound stream error, attempting re-establishment via withRetry", err)
      await reestablishStreamWithRetry().catch((retryErr) => {
        console.error("KronosDB query bus: reconnect retries exhausted", retryErr)
      })
    }
  }

  const routing: QueryBus = {
    async query(unstamped: Unstamped<QueryMessage>, uow?: UnitOfWork): Promise<unknown> {
      const activity = shutdownLatch.registerActivity()
      try {
        const queryName = qualifiedNameToString(unstamped.name)

        if (shortcutQueriesToLocalHandlers && localHandlers.has(queryName)) {
          // Hand the unit of work through, so a `ctx.query` that shortcuts to a
          // co-located handler still nests in the caller's UoW exactly as the
          // in-process bus does — otherwise the local and remote branches
          // differ. `local` owns the nest-or-open decision now; that used to be
          // duplicated here against a separately-supplied `unitOfWork`, which
          // was one more place for the two to disagree.
          return local.query(unstamped, uow)
        }

      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire still {@link Unstamped} is therefore
      // stamped from system time here — the envelope crosses a process boundary
      // and must be fully formed. A locally-shortcut message is handed to
      // `local` unstamped instead, so the task that handles it supplies the
      // instant.
        const message = stamped(unstamped, Date.now)
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

    subscribe(queryName, handler) {
      localHandlers.add(queryName)
      local.subscribe(queryName, handler)

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

    subscriptionQuery(
      unstamped: Unstamped<QueryMessage>,
      bufferSize?: number,
    ): SubscriptionQueryResult {
      const message = stamped(unstamped, Date.now)
      const queryId = message.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const handler = updateHandler(message, bufferSize)
      subscriptions.set(queryId, handler)

      const queryName = qualifiedNameToString(message.name)
      const subscriptionId = generateIdentifier()
      const serialized = serializePayload(queryName, message.payload)

      const outboundSub = outboundStream<any>()

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

    subscribeToUpdates(
      unstamped: Unstamped<QueryMessage>,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      const message = stamped(unstamped, Date.now)
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

  return routing
}
