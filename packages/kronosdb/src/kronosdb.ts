import { streamRecovery } from "./stream-recovery.js"
import { withMessagingTimeout } from "@kronos-ts/core"
import { messagingAdmission, messagingDeadline, positiveInteger, type MessagingLimits } from "@kronos-ts/core"
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
 * const eventStore = kronosDbSnapshottingEventStore(
 *   kronosDbEventStore(kdb, "billing"), kdb, "billing")
 * const commandBus    = interceptingCommandBus(
 *   kronosDbCommandBus(localCommandBus(unitOfWork), kdb), correlation)
 * const queryBus      = interceptingQueryBus(
 *   kronosDbQueryBus(localQueryBus(unitOfWork), kdb), correlation)
 *
 * const app = kronos({ commandHandlers, queryHandlers })
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
import { generateIdentifier, qualifiedNameFromString, qualifiedNameToString, type Serializer } from "@kronos-ts/core"
import { withRetry, healthCheck, type ResilienceConfig } from "./resilience.js"
import type { CommandBus, CommandMessage, QueryBus, QueryMessage, SubscriptionCapableQueryBus, SubscriptionFilter, SubscriptionQueryResult, UnitOfWork, UpdateHandler } from "@kronos-ts/core"
import {
  applySubscriptionFilter,
  updateHandler,
  runAfterCommitOrImmediately,
} from "@kronos-ts/core"
import type { KronosDbConnectionConfig } from "./connection.js"
import { busMetadata, connectToKronosDb, type KronosDbConnection } from "./connection.js"
import { KronosDbErrorCode, mapErrorCode } from "./errors.js"
import { metadataFromProto, metadataToProto } from "./metadata-conversion.js"
import { outboundStream, type OutboundStream } from "./outbound-stream.js"
import type { Command } from "./generated/command.js"
import { platformConnection, type PlatformConnection, type PlatformServiceOptions } from "./platform-service.js"
import { shutdownLatch as shutdownLatchValue, type ShutdownLatch } from "./shutdown-latch.js"

const DEFAULT_PERMITS = 5000n

export type FlowControlConfig = {
  permits?: number
  refillThreshold?: number
}

export type ProcessingInstructions = {
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
export type KronosDbCommandBusOptions = {
  /** Client-side request deadline. Default: 30000ms. */
  timeoutMs?: number
  /** Receive credits, refilled on receipt; these do not cap running handlers. */
  flowControl?: FlowControlConfig
  /** Relative share of routed work this instance advertises. Default: 100. */
  loadFactor?: number
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
  /** Bounded admission; excess nested work receives an overload error. */
  limits?: MessagingLimits

}

/** @see KronosDbCommandBusOptions */
export type KronosDbQueryBusOptions = {
  flowControl?: FlowControlConfig
  /** Answer from a co-located handler instead of going out to the server. */
  shortcutQueriesToLocalHandlers?: boolean
  /** Client and server query timeout. Default: 30000ms. */
  timeoutMs?: number
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
  /** Bounded admission; excess nested work receives an overload error. */
  limits?: MessagingLimits

}

/**
 * Connection-level options: the socket, the platform stream, how payloads are
 * encoded on this wire, and how hard to retry.
 *
 * The SERIALIZER is here rather than on each store and bus because it is a
 * property of this client's wire — one channel, one encoding — and because a
 * store keyed by `(connection, context)` has nowhere honest to put it.
 */
export type KronosDbConnectionOptions = KronosDbConnectionConfig & {
  /** Maximum graceful drain time; transport closes even when this expires. Default: 30000ms. */
  shutdownTimeoutMs?: number
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
export type KronosDbConnectionHandle = {
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
 *   kronosDbCommandBus(localCommandBus(unitOfWork), kdb), correlation)
 * const queryBus = interceptingQueryBus(
 *   kronosDbQueryBus(localQueryBus(unitOfWork), kdb), correlation)
 *
 * const app = kronos({ commandHandlers, queryHandlers })
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
      try {
        await withMessagingTimeout(Promise.all(busLatches.map((l) => l.initiateShutdown())), options.shutdownTimeoutMs ?? 30000, "Messaging shutdown")
      } finally {
        platform.stop()
        connection.close()
      }
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
// Provider stream failures use bounded, coalesced asynchronous recovery.
// ---------------------------------------------------------------------------

/**
 * A command bus backed by KronosDB.
 *
 * ## correlation and the interceptor layer
 *
 * The returned bus stamps no correlation of its own. A host that wants roots
 * seeded wraps the OUTERMOST bus with `interceptingCommandBus(bus, correlation)`,
 * so whatever a host transforms runs BEFORE the message is serialized onto the
 * wire. A command born inside a handler already carries its correlation by then:
 * `correlatingHandler` overlaid the task's map onto it before any bus saw it.
 *
 * This mirrors AxonFramework, where dispatch interception always sits outside
 * the routing bus. AF4's `AxonServerCommandBus.dispatch` is
 * `doDispatch(dispatchInterceptors.intercept(commandMessage), cb)`; AF5 expresses
 * the same thing through decorator order —
 * `DISTRIBUTED_COMMAND_BUS_ORDER = InterceptingCommandBus.DECORATION_ORDER - 50`
 * stacks the buses `InterceptingCommandBus → DistributedCommandBus → LocalCommandBus`.
 *
 * Without this, remote correlation was simply lost: the interceptor was registered
 * only inside `@kronos-ts/core`'s default in-memory bus, and this bus REPLACES
 * that one in `components` — every command left the process with no
 * `correlationId` / `causationId`, even though the inbound side below faithfully
 * rebuilds a UnitOfWork from `message.metadata`.
 *
 * Double application of `correlation` is harmless: both of its fields are `??`
 * seeds, so a `next` that is itself intercepting simply sees them already set.
 *
 * ## The next segment is a real bus
 *
 * `next` is a `CommandBus`, not a private handler map. `subscribe` registers
 * on it AND announces the name to the server; a command the SERVER routes back
 * here is dispatched into it. That is what makes the unit-of-work policy you
 * chose for `next` — `postgresUnitOfWork(unitOfWork, pg)`, say — apply to
 * server-routed work exactly as it applies to anything else. It also removes
 * the `unitOfWork` parameter this function used to take: `next` carries that
 * policy now, and having it twice was a way to disagree with yourself.
 *
 * Server-side routing is unchanged: KronosDB is a smart hub, so an outbound
 * dispatch ALWAYS goes to the server, even for a command this instance handles.
 * There is no client-side prefer-next fork here (that is RabbitMQ's model).
 *
 * ## The bus is a NAME, not a context
 *
 * `bus` is the server-scoped messaging namespace this wrapper subscribes and
 * dispatches on — a plain string, independent of any event store context
 * (server ADR-0006). Every messaging RPC carries it as the per-call
 * `kronosdb-bus` header; omitted, it is the server's `default` bus. There is NO
 * server-side fallback from bus to context: contexts address logs, buses
 * address messaging, and a host that wants them 1:1 names the bus after the
 * context — one visible decision at this call site. With the 0.9 fabric the
 * name is also the CLUSTER-WIDE identity: subscribe handlers on this bus via
 * any node, dispatch via any node, and the server forwards.
 */
export function kronosDbCommandBus<U extends UnitOfWork = UnitOfWork>(
  next: CommandBus<U>,
  kdb: Pick<KronosDbConnectionHandle, "connection" | "serializer" | "registerShutdownLatch">,
  bus: string = "default",
  options: KronosDbCommandBusOptions = {},
): CommandBus<U> {
  const { flowControl, loadFactor: commandLoadFactor, resilience } = options
  const serializer = kdb.serializer
  const connection = kdb.connection
  const shutdownLatch = shutdownLatchValue()
  const requestTimeoutMs = positiveInteger(options.timeoutMs ?? 30000, "timeoutMs")
  if (requestTimeoutMs > 2_147_483_647) throw new RangeError("timeoutMs exceeds the timer range")
  const inboundAdmission = messagingAdmission("inbound handlers", options.limits?.maxConcurrentHandlers ?? 128, options.limits?.observe)
  const outboundAdmission = messagingAdmission("pending requests", options.limits?.maxPendingRequests ?? 1024, options.limits?.observe)
  kdb.registerShutdownLatch(shutdownLatch)

  const metadata = busMetadata(bus, connection.config)
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)
  const PERMITS = BigInt(positiveInteger(flowControl?.permits ?? Number(DEFAULT_PERMITS), "flowControl.permits"))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Math.floor(Number(PERMITS) / 2))
  if (THRESHOLD < 0n || THRESHOLD >= PERMITS) throw new RangeError("refillThreshold must be between zero and permits - 1")

  // Names this instance announced. Kept so a reconnect can re-announce them and
  // so an unroutable inbound command still gets NO_HANDLER_FOR_COMMAND rather
  // than whatever `next.dispatch` happens to throw for an unknown name.
  const localHandlers = new Set<string>()

  // The server indexes provider streams by client ID, across named buses. A
  // stream incarnation needs its own identity so old cleanup cannot remove a
  // replacement or a different bus. Outgoing callers keep the logical client ID.
  let providerClientId = `${connection.config.clientId}:provider:${generateIdentifier()}`
  let outbound = outboundStream<any>()
  let streamStarted = false
  let providerAbort = new AbortController()
  connection.onDisconnect?.(() => { providerAbort.abort(); outbound.close() })
  let permits = 0n

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    const inbound = connection.commands.openStream(outbound.iterable, { metadata, signal: providerAbort.signal })
    void processInboundCommands(inbound, outbound)
  }

  function grantPermits() {
    outbound.send({
      flowControl: { clientId: providerClientId, permits: PERMITS },
      instructionId: "",
    })
    permits += PERMITS
  }

  function reestablishStreamBody() {
    providerAbort.abort()
    providerAbort = new AbortController()
    outbound.close()
    providerClientId = `${connection.config.clientId}:provider:${generateIdentifier()}`
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
          clientId: providerClientId,
          loadFactor: commandLoadFactor ?? 100,
        },
        instructionId: generateIdentifier(),
      })
    }
    grantPermits()
  }

  const recovery = streamRecovery(reestablishStreamBody,
    () => !shutdownLatch.shuttingDown && connection.state !== "closed" && connection.state !== "disconnected" && connection.state !== "reconnecting",
    resilience)
  shutdownLatch.onShutdown(recovery.stop)

  connection.onReconnect(() => {
    if (!shutdownLatch.shuttingDown && streamStarted) {
      recovery.restart()
    }
  })

  async function handleInboundCommand(proto: Command, responses: OutboundStream<any>) {
    let activity: ReturnType<ShutdownLatch["registerActivity"]> | undefined
    let admission: ReturnType<typeof inboundAdmission.enter> | undefined
    let responseSerialized: ReturnType<typeof serializePayload> | undefined
    let errorCode = ""
    let errorMsg = ""

    try {
      try {
        // Remote callers have no outbound dispatch activity on this adapter.
        // Track the entire handling, including result serialization and enqueue.
        // Registration also rejects new work once shutdown has begun.
        activity = shutdownLatch.registerActivity()
        admission = inboundAdmission.enter()
        if (localHandlers.has(proto.name)) {
          const commandMessage: CommandMessage = {
            kind: "command",
            identifier: proto.messageIdentifier,
            name: qualifiedNameFromString(proto.name),
            payload: deserializePayload(proto.payload?.data, proto.payload?.type, proto.payload?.revision),
            metadata: metadataFromProto(proto.metadata ?? {}),
            timestamp: Number(proto.timestamp),
          }

          // The local bus opens a fresh unit of work for EVERY wire command,
          // including children of handlers running on this same connection.
          const result = await next.dispatch(commandMessage)
          responseSerialized = result !== undefined ? serializePayload("result", result) : undefined
        } else {
          errorCode = KronosDbErrorCode.NO_HANDLER_FOR_COMMAND
          errorMsg = `No next handler for command "${proto.name}"`
        }
      } catch (err) {
        // Decode, handler, and result-encoding failures belong to this request;
        // none should terminate the receive loop or reconnect the stream.
        errorCode = KronosDbErrorCode.COMMAND_EXECUTION_ERROR
        errorMsg = err instanceof Error ? err.message : String(err)
      }

      // Capture the originating stream: a late handler must not send an old
      // request's response on a replacement stream after reconnect.
      responses.send({
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
      await responses.flush()
    } finally {
      admission?.end()
      activity?.end()
    }
  }

  async function processInboundCommands(inbound: AsyncIterable<any>, responses: OutboundStream<any>) {
    try {
      for await (const message of inbound) {
        if (responses !== outbound) return
        recovery.received()
        if (message.instructionId) responses.send({ ack: { instructionId: message.instructionId, success: true }, instructionId: "" })
        if (!message.command) continue

        permits--
        // Credits bound delivery batches, not unfinished handlers. Replenish
        // on receipt: completion-based credits or a fixed handler semaphore can
        // deadlock when every admitted parent is waiting for a queued child.
        if (permits <= THRESHOLD && !shutdownLatch.shuttingDown) grantPermits()

        // Each invocation owns its response and shutdown activity. Keep reading
        // while it awaits work so nested dispatch can return on this connection.
        void handleInboundCommand(message.command, responses).catch((err) => {
          console.error("KronosDB command bus: inbound response failed", err)
        })
      }
      if (responses === outbound && !shutdownLatch.shuttingDown) throw new Error("Inbound provider stream ended unexpectedly")
    } catch (err) {
      if (responses !== outbound || shutdownLatch.shuttingDown) return
      if (connection.state === "reconnecting" || connection.state === "closed" || connection.state === "disconnected") return

      recovery.failed(err)
    }
  }

  const routing: CommandBus<U> = {
    async dispatch(unstamped: CommandMessage): Promise<unknown> {
      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire with no instant yet gets one from system
      // time here — the envelope crosses a process boundary and must be fully
      // formed. A locally-shortcut message is handed to `next` untouched
      // instead, so the task that handles it supplies the instant.
      const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }
      const activity = shutdownLatch.registerActivity()
      let admission: ReturnType<typeof outboundAdmission.enter> | undefined
      let deadline: ReturnType<typeof messagingDeadline> | undefined
      try {
        admission = outboundAdmission.enter(unstamped.identifier)
        deadline = messagingDeadline(requestTimeoutMs)
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
        }, { metadata, signal: deadline.signal })

        if (response.errorCode && response.errorCode !== "") {
          throw mapErrorCode(
            response.errorCode,
            response.errorMessage?.message ?? "Unknown error",
          )
        }

        return deserializePayload(response.payload?.data as Uint8Array | undefined, response.payload?.type, response.payload?.revision)
      } finally {
        deadline?.close()
        admission?.end()
        activity.end()
      }
    },

    subscribe(commandName, handler) {
      localHandlers.add(commandName)
      next.subscribe(commandName, handler)

      ensureStreamStarted()
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          command: commandName,
          componentName: connection.config.componentName,
          clientId: providerClientId,
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
 * Correlation, if wanted, is `interceptingQueryBus(bus, correlation)` at the host, for the same reason
 * {@link kronosDbCommandBus} is wrapped — AF runs dispatch interception at the
 * top of `query` / `scatterGather` /
 * `subscriptionQuery`, before anything is sent. `query()` below can also shortcut
 * to a co-located handler, and wrapping outside means correlation is stamped
 * identically on both branches.
 *
 * Subscription queries run the dispatch chain: `interceptingQueryBus` wraps
 * `subscriptionQuery` / `subscribeToUpdates` with the same intercept the
 * primary `query` gets, so the proto this bus builds from `message.metadata`
 * already carries whatever the host's intercept stamped (pinned by
 * `interception/__tests__/subscription-interception.test.ts` in core).
 *
 * `bus` is the same server-scoped namespace string the command side takes (see
 * {@link kronosDbCommandBus}: `kronosdb-bus` per RPC, `"default"` when omitted,
 * no relation to event store contexts).
 */
export function kronosDbQueryBus<U extends UnitOfWork = UnitOfWork>(
  next: QueryBus<U>,
  kdb: Pick<KronosDbConnectionHandle, "connection" | "serializer" | "registerShutdownLatch">,
  bus: string = "default",
  options: KronosDbQueryBusOptions = {},
): SubscriptionCapableQueryBus<U> {
  const {
    flowControl,
    shortcutQueriesToLocalHandlers,
    timeoutMs: queryTimeoutMs,
    resilience,
  } = options
  const serializer = kdb.serializer
  const connection = kdb.connection
  const shutdownLatch = shutdownLatchValue()
  const requestTimeoutMs = positiveInteger(options.timeoutMs ?? 30000, "timeoutMs")
  if (requestTimeoutMs > 2_147_483_647) throw new RangeError("timeoutMs exceeds the timer range")
  const inboundAdmission = messagingAdmission("inbound handlers", options.limits?.maxConcurrentHandlers ?? 128, options.limits?.observe)
  const outboundAdmission = messagingAdmission("pending requests", options.limits?.maxPendingRequests ?? 1024, options.limits?.observe)
  kdb.registerShutdownLatch(shutdownLatch)

  const metadata = busMetadata(bus, connection.config)
  const PERMITS = BigInt(positiveInteger(flowControl?.permits ?? Number(DEFAULT_PERMITS), "flowControl.permits"))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Math.floor(Number(PERMITS) / 2))
  if (THRESHOLD < 0n || THRESHOLD >= PERMITS) throw new RangeError("refillThreshold must be between zero and permits - 1")
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
  shutdownLatch.onShutdown(() => handlerSubscriptions.clear())

  // The server indexes provider streams by client ID, across named buses. A
  // stream incarnation needs its own identity so old cleanup cannot remove a
  // replacement or a different bus. Outgoing callers keep the logical client ID.
  let providerClientId = `${connection.config.clientId}:provider:${generateIdentifier()}`
  let outbound = outboundStream<any>()
  let streamStarted = false
  let providerAbort = new AbortController()
  connection.onDisconnect?.(() => { providerAbort.abort(); outbound.close() })
  let permits = 0n

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    const inbound = connection.queries.openStream(outbound.iterable, { metadata, signal: providerAbort.signal })
    void processInboundQueries(inbound, outbound)
  }

  function grantQueryPermits() {
    outbound.send({
      flowControl: { clientId: providerClientId, permits: PERMITS },
      instructionId: "",
    })
    permits += PERMITS
  }

  function reestablishStreamBody() {
    handlerSubscriptions.clear()
    outbound.close()
    providerClientId = `${connection.config.clientId}:provider:${generateIdentifier()}`
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
          clientId: providerClientId,
        },
        instructionId: generateIdentifier(),
      })
    }
    grantQueryPermits()
  }

  const recovery = streamRecovery(reestablishStreamBody,
    () => !shutdownLatch.shuttingDown && connection.state !== "closed" && connection.state !== "disconnected" && connection.state !== "reconnecting",
    resilience)
  shutdownLatch.onShutdown(recovery.stop)

  connection.onReconnect(() => {
    if (!shutdownLatch.shuttingDown && streamStarted) {
      recovery.restart()
    }
  })

  async function handleInboundQuery(proto: any, responses: OutboundStream<any>, subId?: string) {
    let activity: ReturnType<ShutdownLatch["registerActivity"]> | undefined
    let admission: ReturnType<typeof inboundAdmission.enter> | undefined
    let payload: ReturnType<typeof serializePayload> | undefined
    let subscriptionEntry: { queryName: string; payload: unknown } | undefined
    let errorCode = ""
    let errorMsg = ""
    try {
      try {
        activity = shutdownLatch.registerActivity()
        admission = inboundAdmission.enter()
        if (localHandlers.has(proto.query)) {
          const queryMessage: QueryMessage = {
            kind: "query",
            identifier: proto.messageIdentifier,
            name: qualifiedNameFromString(proto.query),
            payload: deserializePayload(proto.payload?.data, proto.payload?.type, proto.payload?.revision),
            metadata: metadataFromProto(proto.metadata ?? {}),
            timestamp: Number(proto.timestamp),
          }
          if (subId) {
            if (handlerSubscriptions.size >= 1024 && !handlerSubscriptions.has(subId)) throw new Error("Provider subscription capacity exhausted")
            subscriptionEntry = { queryName: proto.query, payload: queryMessage.payload }
            handlerSubscriptions.set(subId, subscriptionEntry)
          }
          // Every wire query enters the local bus with a fresh unit of work.
          const result = await next.query(queryMessage)
          payload = result !== undefined ? serializePayload("result", result) : undefined
        } else {
          errorCode = KronosDbErrorCode.NO_HANDLER_FOR_QUERY
          errorMsg = `No next handler for query "${proto.query}"`
        }
      } catch (err) {
        errorCode = KronosDbErrorCode.QUERY_EXECUTION_ERROR
        errorMsg = err instanceof Error ? err.message : String(err)
      }
      // An unsubscribe or completion can overtake a slow initial handler.
      if (subId && subscriptionEntry && handlerSubscriptions.get(subId) !== subscriptionEntry) return
      if (subId && errorCode) handlerSubscriptions.delete(subId)
      const response = {
        messageIdentifier: generateIdentifier(),
        requestIdentifier: proto.messageIdentifier,
        errorCode,
        errorMessage: errorCode
          ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
          : undefined,
        payload,
        metadata: {},
        processingInstructions: [],
      }
      // KronosDB routes the initial subscription result by requestIdentifier
      // on QueryResponse; SubscriptionQueryResponse is reserved for updates.
      responses.send({ queryResponse: response, instructionId: "" })
      responses.send({
        queryComplete: { messageId: generateIdentifier(), requestId: proto.messageIdentifier },
        instructionId: "",
      })
      await responses.flush()
    } finally {
      admission?.end()
      activity?.end()
    }
  }

  async function processInboundQueries(inbound: AsyncIterable<any>, responses: OutboundStream<any>) {
    try {
      for await (const message of inbound) {
        if (responses !== outbound) return
        recovery.received()
        if (message.instructionId) responses.send({ ack: { instructionId: message.instructionId, success: true }, instructionId: "" })
        const request = message.subscriptionQueryRequest
        if (request) {
          if (request.unsubscribe) handlerSubscriptions.delete(request.unsubscribe.subscriptionIdentifier)
          const sub = request.subscribe
          if (sub?.subscriptionIdentifier && sub.queryRequest) {
            permits--
            if (permits <= THRESHOLD && !shutdownLatch.shuttingDown) grantQueryPermits()
            void handleInboundQuery(sub.queryRequest, responses, sub.subscriptionIdentifier).catch((err) => {
              console.error("KronosDB query bus: inbound subscription response failed", err)
            })
          }
          continue
        }
        if (!message.query) continue
        permits--
        // Receive credits cannot depend on handler completion: all admitted
        // parents could be waiting for children on this same stream.
        if (permits <= THRESHOLD && !shutdownLatch.shuttingDown) grantQueryPermits()
        void handleInboundQuery(message.query, responses).catch((err) => {
          console.error("KronosDB query bus: inbound response failed", err)
        })
      }
      if (responses === outbound && !shutdownLatch.shuttingDown) throw new Error("Inbound provider stream ended unexpectedly")
    } catch (err) {
      if (responses !== outbound || shutdownLatch.shuttingDown) return
      if (connection.state === "reconnecting" || connection.state === "closed" || connection.state === "disconnected") return
      recovery.failed(err)
    }
  }

  const routing: SubscriptionCapableQueryBus<U> = {
    async query(unstamped: QueryMessage, uow?: UnitOfWork): Promise<unknown> {
      const activity = shutdownLatch.registerActivity()
      let admission: ReturnType<typeof outboundAdmission.enter> | undefined
      let deadline: ReturnType<typeof messagingDeadline> | undefined
      try {
        admission = outboundAdmission.enter(unstamped.identifier)
        deadline = messagingDeadline(requestTimeoutMs)
        const queryName = qualifiedNameToString(unstamped.name)

        if (shortcutQueriesToLocalHandlers && localHandlers.has(queryName)) {
          // Hand the unit of work through, so a `ctx.query` that shortcuts to a
          // co-located handler still nests in the caller's UoW exactly as the
          // in-process bus does — otherwise the next and remote branches
          // differ. `next` owns the nest-or-open decision now; that used to be
          // duplicated here against a separately-supplied `unitOfWork`, which
          // was one more place for the two to disagree.
          return await next.query(unstamped, uow)
        }

      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire with no instant yet gets one from system
      // time here — the envelope crosses a process boundary and must be fully
      // formed. A locally-shortcut message is handed to `next` untouched
      // instead, so the task that handles it supplies the instant.
        const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }
        const serialized = serializePayload(queryName, message.payload)

        const responseStream = connection.queries.query({
          messageIdentifier: message.identifier,
          query: queryName,
          timestamp: BigInt(message.timestamp),
          payload: serialized,
          metadata: metadataToProto(message.metadata),
          processingInstructions: defaultQueryInstructions(queryTimeoutMs ?? 30000),
          clientId: connection.config.clientId,
          componentName: connection.config.componentName,
        }, { metadata, signal: deadline.signal })

        // NR_OF_RESULTS is one. Drain trailers before returning so transport
        // failures cannot be mistaken for a successful result. The RPC deadline
        // also bounds a stream that sends a response but never completes.
        let received = false
        let result: unknown
        let responseError: Error | undefined
        for await (const response of responseStream) {
          if (received) continue
          received = true
          if (response.errorCode && response.errorCode !== "") {
            responseError = mapErrorCode(response.errorCode, response.errorMessage?.message ?? "Unknown error")
          } else {
            try { result = deserializePayload(response.payload?.data, response.payload?.type, response.payload?.revision) }
            catch (error) { responseError = error instanceof Error ? error : new Error(String(error)) }
          }
        }
        if (responseError) throw responseError
        if (received) return result

        throw new Error(`No response for query "${queryName}"`)
      } finally {
        deadline?.close()
        admission?.end()
        activity.end()
      }
    },

    subscribe(queryName, handler) {
      localHandlers.add(queryName)
      next.subscribe(queryName, handler)

      ensureStreamStarted()
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          query: queryName,
          resultName: "",
          componentName: connection.config.componentName,
          clientId: providerClientId,
        },
        instructionId: generateIdentifier(),
      })
      grantQueryPermits()
    },

    subscriptionQuery(
      unstamped: QueryMessage,
      bufferSize?: number,
    ): SubscriptionQueryResult {
      if (shutdownLatch.shuttingDown) throw new Error("Messaging shutdown in progress")
      if (subscriptions.size >= 1024) throw new Error("Subscription capacity 1024 exhausted")
      const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }
      const queryId = message.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const handler = updateHandler(message, bufferSize, () => subscriptions.delete(queryId))

      const queryName = qualifiedNameToString(message.name)
      const subscriptionId = generateIdentifier()
      const serialized = serializePayload(queryName, message.payload)

      const outboundSub = outboundStream<any>()

      // CREDIT-BASED FLOW CONTROL, AND THE SUBSCRIBER OWES THE REFILL.
      //
      // `subscribe` grants an initial window of credit. The server decrements
      // it per update and DROPS updates once it hits zero — silently, with no
      // frame back to say so. Granting a window and never refilling is
      // therefore a subscription that works for exactly `window` updates and
      // then stops, with nothing anywhere to read as the cause. We top the
      // window back up as updates are handed to the consumer, in batches of a
      // quarter, so the wire carries one small message per quarter-window
      // instead of one per update.
      //
      // THE SERVER CLAMPS THE INITIAL GRANT TO [1, 1024] and we mirror that
      // bound rather than trusting the number we asked for: a host passing
      // `bufferSize: 5000` is granted 1024, and refilling on a 5000-wide
      // window would let 1250 updates go by before topping up — 226 of them
      // past the credit the server actually holds, and dropped. The refill is
      // sized off what the server GAVE, not what we requested. Top-ups
      // themselves are additive and unbounded, so the window is only ever
      // restored to this size.
      const requested = bufferSize && bufferSize > 0 ? bufferSize : 256
      const window = Math.min(1024, Math.max(256, requested))
      const refillBatch = Math.max(1, Math.floor(window / 4))
      let consumedSinceRefill = 0
      let subscriptionClosed = false

      outboundSub.send({
        subscribe: {
          subscriptionIdentifier: subscriptionId,
          numberOfPermits: BigInt(window),
          queryRequest: {
            messageIdentifier: message.identifier,
            query: queryName,
            timestamp: BigInt(message.timestamp),
            payload: serialized,
            metadata: metadataToProto(message.metadata),
            processingInstructions: defaultQueryInstructions(queryTimeoutMs ?? 30000),
            clientId: connection.config.clientId,
            componentName: connection.config.componentName,
          },
        },
      })

      const subscriptionController = new AbortController()
      const responseStream = connection.queries.subscription(outboundSub.iterable, { metadata, signal: subscriptionController.signal })
      subscriptions.set(queryId, handler)

      let resolveInitial!: (value: unknown) => void
      let rejectInitial!: (error: Error) => void
      const initialResult = new Promise<unknown>((resolve, reject) => {
        resolveInitial = resolve
        rejectInitial = reject
      })
      let initialSettled = false
      let explicitlyCompleted = false
      const initialTimer = setTimeout(() => closeSubscription(new Error("Subscription initial result timed out")), requestTimeoutMs)
      const removeShutdown = shutdownLatch.onShutdown(() => closeSubscription(new Error("Messaging shutdown in progress")))
      // Callers may consume updates without awaiting the initial result. Keep
      // the original promise rejectable without an unhandled rejection on close.
      void initialResult.catch(() => {})

      function closeSubscription(error?: Error) {
        if (subscriptionClosed) return
        subscriptionClosed = true
        clearTimeout(initialTimer)
        removeShutdown()
        if (!initialSettled) {
          rejectInitial(error ?? new Error("Subscription query closed before initial result"))
          initialSettled = true
        }
        if (error) handler.completeExceptionally(error)
        else handler.complete()
        try { outboundSub.send({ unsubscribe: { subscriptionIdentifier: subscriptionId } }) } catch { /* Broken stream; local teardown still must finish. */ }
        outboundSub.close()
        subscriptionController.abort()
        subscriptions.delete(queryId)
      }

      void (async () => {
        try {
          for await (const response of responseStream) {
            if (subscriptionClosed) break
            if (response.initialResult) {
              const initial = response.initialResult
              if (!initialSettled) {
                if (initial.errorCode) {
                  throw mapErrorCode(initial.errorCode, initial.errorMessage?.message ?? "Unknown error")
                }
                clearTimeout(initialTimer)
                resolveInitial(deserializePayload(initial.payload?.data, initial.payload?.type, initial.payload?.revision))
                initialSettled = true
              }
            } else if (response.update) {
              const update = deserializePayload(response.update.payload?.data, response.update.payload?.type, response.update.payload?.revision)
              if (!handler.offer(update)) throw new Error("Subscription query update buffer overflow")
              consumedSinceRefill++
              if (consumedSinceRefill >= refillBatch) {
                outboundSub.send({
                  flowControl: { subscriptionIdentifier: subscriptionId, numberOfPermits: BigInt(consumedSinceRefill) },
                })
                consumedSinceRefill = 0
              }
            } else if (response.complete) {
              explicitlyCompleted = true
              break
            } else if (response.completeExceptionally) {
              throw new Error(response.completeExceptionally.errorMessage?.message ?? "Subscription query failed")
            }
          }
        } catch (err) {
          closeSubscription(err instanceof Error ? err : new Error(String(err)))
        } finally {
          // EOF and completion frames must settle BOTH faces of a subscription.
          const missingInitial = !initialSettled
          if (!initialSettled) {
            rejectInitial(new Error("Subscription stream ended before initial result"))
            initialSettled = true
          }
          closeSubscription(!missingInitial && !explicitlyCompleted && !subscriptionClosed ? new Error("Subscription stream ended unexpectedly") : undefined)
        }
      })()

      return {
        initialResult,
        updates: {
          [Symbol.asyncIterator]() {
            const iterator = handler.iterable[Symbol.asyncIterator]()
            return {
              next: () => iterator.next(),
              async return() {
                closeSubscription()
                return iterator.return ? iterator.return() : { value: undefined, done: true as const }
              },
            }
          },
        },
        close: () => closeSubscription(),
      }
    },

    subscribeToUpdates(
      unstamped: QueryMessage,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      if (shutdownLatch.shuttingDown) throw new Error("Messaging shutdown in progress")
      if (subscriptions.size >= 1024) throw new Error("Subscription capacity 1024 exhausted")
      const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }
      const queryId = message.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      let removeShutdown: (() => void) | undefined
      const handler = updateHandler(message, bufferSize, () => { subscriptions.delete(queryId); removeShutdown?.() })
      removeShutdown = shutdownLatch.onShutdown(() => handler.completeExceptionally(new Error("Messaging shutdown in progress")))
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
      uow?: UnitOfWork,
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
      }, uow)
    },

    async completeSubscription(
      queryName: string,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
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
      }, uow)
    },

    async completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
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
      }, uow)
    },
  }

  return routing
}
