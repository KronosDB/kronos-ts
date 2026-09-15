import { streamRecovery } from "./stream-recovery.js"
import { messagingAdmission, messagingDeadline, positiveInteger, type MessagingLimits } from "@kronos-ts/core"
/**
 * The Axon Server command and query buses.
 *
 * Axon Server is a SMART HUB: outbound dispatch always goes to the server, and
 * the server decides which node handles it — there is no client-side
 * prefer-next fork here, which is the whole difference from the dumb-pipe
 * broker in `@kronos-ts/rabbitmq`.
 *
 * Both buses are plain functions over the shared connection and YOUR next bus:
 *
 * ```ts
 * const commandBus = interceptingCommandBus(
 *   axonServerCommandBus(localCommandBus(unitOfWork), axon), correlation)
 * const queryBus = interceptingQueryBus(
 *   axonServerQueryBus(localQueryBus(unitOfWork), axon), correlation)
 * ```
 *
 * Axon-specific protocol invariants:
 *
 *   - CLIENT_SUPPORTS_STREAMING capability advertised on every dispatched
 *     query via `defaultQueryInstructions(...)`;
 *   - AxonIQ-Context + AxonIQ-Access-Token gRPC metadata headers built by
 *     `contextView(...)` and attached to every outbound stream/RPC;
 *   - permits-AFTER-subscriptions stream ordering preserved on the initial
 *     handshake AND on reconnect (see `ensureStreamStarted` /
 *     `reestablishStreamBody`).
 */
import {
  qualifiedNameToString,
  qualifiedNameFromString,
  generateIdentifier,
  type Serializer,
} from "@kronos-ts/core"
import { type ResilienceConfig } from "./resilience.js"
import type {
  CommandBus,
  CommandMessage,
  QueryBus,
  SubscriptionCapableQueryBus,
  QueryMessage,
  SubscriptionFilter,
  SubscriptionQueryResult,
  UnitOfWork,
  UpdateHandler,
} from "@kronos-ts/core"
import {
  applySubscriptionFilter,
  updateHandler,
  runAfterCommitOrImmediately,
} from "@kronos-ts/core"
import type { AxonServerBusSource } from "./connection.js"
import { contextView } from "./context-view.js"
import { metadataToProto, metadataFromProto } from "./metadata-conversion.js"
import { outboundStream, type OutboundStream } from "./outbound-stream.js"
import type { Command } from "./generated/command.js"
import type { ShutdownLatch } from "./shutdown-latch.js"
import { mapErrorCode, AxonServerErrorCode } from "./errors.js"

/** Default flow control settings — aligned with Java's 5000 permits. */
const DEFAULT_PERMITS = 5000n

/** Default query dispatch timeout — aligned with Java's one hour. */
const DEFAULT_QUERY_TIMEOUT_MS = 30_000

/** Default command handler load factor — aligned with Java's 100. */
const DEFAULT_LOAD_FACTOR = 100

/**
 * Flow control configuration for a bus channel.
 */
export type FlowControlConfig = {
  /** Initial permits granted to Axon Server. Default: 5000 (aligned with Java). */
  permits?: number
  /** Threshold at which to request more permits. Default: 2500 (aligned with Java). */
  refillThreshold?: number
}

/**
 * Processing instructions attached to outbound messages.
 * Controls routing, priority, and timeout behavior on Axon Server.
 */
export type ProcessingInstructions = {
  /** Routing key for consistent hashing (e.g., aggregate ID). */
  routingKey?: string
  /** Priority (higher = processed first). Default: 0 */
  priority?: number
  /** Timeout in ms. Axon Server cancels the command/query if not handled in time. */
  timeoutMs?: number
}

/**
 * Tuning for {@link axonServerCommandBus}. Every field has a working default;
 * the two arguments that carry meaning — the connection and your next bus —
 * are positional, and this record is the trailing remainder.
 */
export type AxonServerCommandBusOptions = {
  /** Client-side request deadline. Default: 30000ms. */
  timeoutMs?: number
  /** Axon Server context for this bus's stream. Default: the connection's. */
  context?: string
  /** Flow control for the command stream. */
  flowControl?: FlowControlConfig
  /**
   * Load factor for this command handler. Signals to Axon Server how much
   * capacity this node has — higher value = more commands routed here.
   * Aligned with Java's `commandLoadFactor`. Default: 100.
   */
  loadFactor?: number
  /** Retry policy for stream re-establishment. Default: the connection's. */
  resilience?: Partial<ResilienceConfig>
  /** Bounded admission; excess nested work receives an overload error. */
  limits?: MessagingLimits

}

/**
 * Tuning for {@link axonServerQueryBus}. See {@link AxonServerCommandBusOptions}.
 */
export type AxonServerQueryBusOptions = {
  /** Axon Server context for this bus's stream. Default: the connection's. */
  context?: string
  /** Flow control for the query stream. */
  flowControl?: FlowControlConfig
  /**
   * When true, queries are first checked against locally subscribed handlers
   * before being dispatched through Axon Server. Avoids a network round-trip
   * when the handler is co-located.
   *
   * This is NOT the rabbitmq `preferLocal` fork by another name: it is Java's
   * `shortcutQueriesToLocalHandlers`, it is off by default, and commands have
   * no equivalent — Axon Server routes those, always.
   */
  shortcutQueriesToLocalHandlers?: boolean
  /**
   * Default timeout for query dispatch in ms. Default: 30000ms.
   * Aligned with Java's processing instruction timeout.
   */
  timeoutMs?: number
  /** Retry policy for stream re-establishment. Default: the connection's. */
  resilience?: Partial<ResilienceConfig>
  /** Bounded admission; excess nested work receives an overload error. */
  limits?: MessagingLimits

}

// Processing instruction keys — aligned with proto ProcessingKey enum.
// CLIENT_SUPPORTS_STREAMING (key=8) is an Axon-Server-specific capability
// advertisement that MUST survive verbatim — see file-level JSDoc above and
// `defaultQueryInstructions` below.
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
    result.push({
      key: INSTRUCTION_KEY.PRIORITY,
      value: { numberValue: BigInt(instructions.priority) },
    })
  }
  if (instructions.timeoutMs !== undefined) {
    result.push({
      key: INSTRUCTION_KEY.TIMEOUT,
      value: { numberValue: BigInt(instructions.timeoutMs) },
    })
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

// ---------------------------------------------------------------------------
// Shared payload helpers
// ---------------------------------------------------------------------------

function createPayloadHelpers(serializer: Serializer) {
  return {
    serializePayload(name: string, payload: unknown, revision: string = "") {
      return serializer.serialize(payload, name, revision)
    },
    deserializePayload(
      data: Uint8Array | undefined,
      type: string = "",
      revision: string = "",
    ): unknown {
      if (!data || data.length === 0) return undefined
      return serializer.deserialize({ data, type, revision })
    },
  }
}

// ---------------------------------------------------------------------------
// Axon Server Command Bus
// ---------------------------------------------------------------------------

/**
 * A command bus backed by Axon Server, over YOUR next bus.
 *
 * - **Outbound dispatch**: ALWAYS through Axon Server, via the unary Dispatch
 *   RPC. Axon Server routes the command to the appropriate node (which may be
 *   this one). There is deliberately no client-side prefer-next fork: the hub
 *   is the router, and short-circuiting it would silently defeat load factors,
 *   priorities and routing keys.
 * - **Inbound**: a command the server routes here is dispatched into `next` —
 *   not into a privately-held handler map. That is what makes the unit-of-work
 *   policy you chose for `next` (say `postgresUnitOfWork(unitOfWork, pg)`)
 *   apply to server-routed work exactly as it applies to work this process
 *   originated. It is also why this function takes no `unitOfWork` argument:
 *   `next` carries that policy now.
 * - **subscribe**: registers the handler on `next` AND announces the name to
 *   Axon Server, so other nodes can route to us.
 *
 * ## correlation and the interceptor layer
 *
 * The returned bus stamps no correlation of its own. A host that wants it wraps the
 * OUTERMOST bus:
 *
 * ```ts
 * interceptingCommandBus(axonServerCommandBus(next, conn), correlation)
 * ```
 *
 * so whatever a host adds runs BEFORE the message is serialized onto the wire.
 * Correlation itself is usually already on `message.metadata` by then — `ctx.send`
 * stamps the unit of work's correlation data before any bus sees the message.
 *
 * This is precisely how the Java client does it. AF4's `AxonServerCommandBus`
 * holds its own `DispatchInterceptors` and dispatches as
 * `doDispatch(dispatchInterceptors.intercept(commandMessage), cb)` — one call
 * site, at the top, ahead of any routing. AF5 keeps the property via decorator
 * order: `DISTRIBUTED_COMMAND_BUS_ORDER = InterceptingCommandBus.DECORATION_ORDER - 50`
 * stacks `InterceptingCommandBus → DistributedCommandBus → LocalCommandBus`.
 *
 * If `next` is itself an intercepting bus, a server-routed command sees
 * `correlation` twice. That is harmless: both of its fields are `??` seeds, so the
 * second application finds them set and changes nothing.
 */
export function axonServerCommandBus<U extends UnitOfWork = UnitOfWork>(
  next: CommandBus<U>,
  conn: AxonServerBusSource,
  options: AxonServerCommandBusOptions = {},
): CommandBus<U> {
  const {
    connection,
    serializer,
    metadata: axonMetadata,
  } = contextView(conn, options.context ?? conn.connection.config.context)
  const shutdownLatch = conn.shutdown
  const requestTimeoutMs = positiveInteger(options.timeoutMs ?? 30000, "timeoutMs")
  if (requestTimeoutMs > 2_147_483_647) throw new RangeError("timeoutMs exceeds the timer range")
  const inboundAdmission = messagingAdmission("inbound handlers", options.limits?.maxConcurrentHandlers ?? 128, options.limits?.observe)
  const outboundAdmission = messagingAdmission("pending requests", options.limits?.maxPendingRequests ?? 1024, options.limits?.observe)
  const resilience = options.resilience ?? conn.resilience
  const metadata = axonMetadata()
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)
  const PERMITS = BigInt(positiveInteger(options.flowControl?.permits ?? Number(DEFAULT_PERMITS), "flowControl.permits"))
  const THRESHOLD = BigInt(options.flowControl?.refillThreshold ?? Math.floor(Number(PERMITS) / 2))
  if (THRESHOLD < 0n || THRESHOLD >= PERMITS) throw new RangeError("refillThreshold must be between zero and permits - 1")
  const loadFactor = options.loadFactor ?? DEFAULT_LOAD_FACTOR

  /**
   * The names this node announced to Axon Server. The handlers themselves live
   * on `next`; this set exists so an inbound command for a name we never
   * subscribed still answers NO_HANDLER_FOR_COMMAND rather than whatever
   * `next.dispatch` happens to throw — and so a reconnect can re-announce.
   */
  const subscribedNames = new Set<string>()

  // Bidirectional stream for handler subscription + inbound command handling
  let outbound = outboundStream<any>()
  let streamStarted = false
  let providerAbort = new AbortController()
  connection.onDisconnect?.(() => { providerAbort.abort(); outbound.close() })
  let permits = 0n

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    // Open stream using connection.commands (always gets current client after reconnect)
    const inbound = connection.commands.openStream(outbound.iterable, { metadata, signal: providerAbort.signal })
    void processInboundCommands(inbound, outbound)
  }

  function grantPermits() {
    outbound.send({
      flowControl: { clientId: connection.config.clientId, permits: PERMITS },
      instructionId: "",
    })
    permits += PERMITS
  }

  function sendSubscribe(commandName: string) {
    outbound.send({
      subscribe: {
        messageId: generateIdentifier(),
        command: commandName,
        componentName: connection.config.componentName,
        clientId: connection.config.clientId,
        loadFactor,
      },
      instructionId: generateIdentifier(),
    })
  }

  /**
   * Re-establish the bidirectional stream and re-announce all handlers.
   * Called on stream error or when the connection reconnects.
   *
   * ORDER (preserves Axon-specific invariant): subscriptions are
   * re-emitted BEFORE the permits frame. Sending permits first would
   * trigger a server-side stream error.
   */
  function reestablishStreamBody() {
    providerAbort.abort()
    providerAbort = new AbortController()
    outbound.close()
    outbound = outboundStream<any>()
    streamStarted = false
    permits = 0n
    ensureStreamStarted()
    // Re-subscribe all handlers FIRST
    for (const commandName of subscribedNames) sendSubscribe(commandName)
    // Permits AFTER subscriptions (Axon-specific ordering invariant)
    grantPermits()
  }

  const recovery = streamRecovery(reestablishStreamBody,
    () => !shutdownLatch.shuttingDown && connection.state !== "closed" && connection.state !== "disconnected" && connection.state !== "reconnecting",
    resilience)
  shutdownLatch.onShutdown(recovery.stop)

  // Auto-reestablish when the connection reconnects (e.g., after heartbeat timeout)
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
        if (subscribedNames.has(proto.name)) {
          const commandMessage: CommandMessage = {
            kind: "command",
            identifier: proto.messageIdentifier,
            name: qualifiedNameFromString(proto.name),
            payload: deserializePayload(proto.payload?.data, proto.payload?.type, proto.payload?.revision),
            metadata: metadataFromProto(proto.metaData ?? {}),
            timestamp: Number(proto.timestamp),
          }

          // The local bus opens a fresh unit of work for EVERY wire command,
          // including children of handlers running on this same connection.
          const result = await next.dispatch(commandMessage)
          responseSerialized = result !== undefined ? serializePayload("result", result) : undefined
        } else {
          errorCode = AxonServerErrorCode.NO_HANDLER_FOR_COMMAND
          errorMsg = `No next handler for command "${proto.name}"`
        }
      } catch (err) {
        // Decode, handler, and result-encoding failures belong to this request;
        // none should terminate the receive loop or reconnect the stream.
        errorCode = AxonServerErrorCode.COMMAND_EXECUTION_ERROR
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
          metaData: {},
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
        permits--
        // Credits bound delivery batches, not unfinished handlers. Replenish
        // on receipt: completion-based credits or a fixed handler semaphore can
        // deadlock when every admitted parent is waiting for a queued child.
        if (permits <= THRESHOLD && !shutdownLatch.shuttingDown) grantPermits()

        if (!message.command) continue

        // Each invocation owns its response and shutdown activity. Keep reading
        // while it awaits work so nested dispatch can return on this connection.
        void handleInboundCommand(message.command, responses).catch((err) => {
          console.error("Axon Server command bus: inbound response failed", err)
        })
      }
      if (responses === outbound && !shutdownLatch.shuttingDown) throw new Error("Inbound provider stream ended unexpectedly")
    } catch (err) {
      if (responses !== outbound || shutdownLatch.shuttingDown) return
      if (connection.state === "reconnecting" || connection.state === "closed" || connection.state === "disconnected") return

      recovery.failed(err)
    }
  }

  return {
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

        const response = await connection.commands.dispatch(
          {
            messageIdentifier: message.identifier,
            name: commandName,
            timestamp: BigInt(message.timestamp),
            payload: serializePayload(commandName, message.payload),
            metaData: metadataToProto(message.metadata),
            processingInstructions: toProtoProcessingInstructions(
              message.metadata?.processingInstructions as ProcessingInstructions | undefined,
            ),
            clientId: connection.config.clientId,
            componentName: connection.config.componentName,
          },
          { metadata, signal: deadline.signal },
        )

        if (response.errorCode && response.errorCode !== "") {
          throw mapErrorCode(response.errorCode, response.errorMessage?.message ?? "Unknown error")
        }

        return deserializePayload(response.payload?.data, response.payload?.type, response.payload?.revision)
      } finally {
        deadline?.close()
        admission?.end()
        activity.end()
      }
    },

    subscribe(
      commandName: string,
      handler: (message: CommandMessage, uow: U) => Promise<unknown>,
    ) {
      subscribedNames.add(commandName)
      next.subscribe(commandName, handler)

      ensureStreamStarted()
      // Subscription FIRST
      sendSubscribe(commandName)
      // Permits AFTER subscription (Axon-specific ordering invariant)
      grantPermits()
    },
  }
}

// ---------------------------------------------------------------------------
// Axon Server Query Bus
// ---------------------------------------------------------------------------

/**
 * A query bus backed by Axon Server, over YOUR next bus.
 *
 * Same architecture as {@link axonServerCommandBus}: outbound dispatch goes
 * through Axon Server, and a query the server routes here runs through `next`,
 * so your unit-of-work policy applies to server-routed reads too. `subscribe`
 * registers on `next` and announces the name to the server.
 *
 * The one asymmetry with commands is `shortcutQueriesToLocalHandlers` — Java
 * has it for queries and not for commands, and so do we. When it is on and this
 * node subscribed the name, `query()` goes straight to `next` and the caller's
 * unit of work is passed through, so the next branch nests exactly as the
 * in-process bus does.
 *
 * Correlation, if wanted, is `interceptingQueryBus(bus, correlation)` at the host,
 * matching AF4's `AxonServerQueryBus`, which calls
 * `dispatchInterceptors.intercept(...)` at the top of `query`, `streamingQuery`,
 * `scatterGather` and `subscriptionQuery`. Because the wrap is outside, the
 * shortcut branch gets identical correlation to the remote branch.
 *
 * Subscription queries run the dispatch chain: `interceptingQueryBus` wraps
 * `subscriptionQuery` / `subscribeToUpdates` with the same intercept the
 * primary `query` gets, so the proto built from `message.metadata` already
 * carries whatever the host's intercept stamped (pinned in core by
 * `interception/__tests__/subscription-interception.test.ts`).
 */
export function axonServerQueryBus<U extends UnitOfWork = UnitOfWork>(
  next: QueryBus<U>,
  conn: AxonServerBusSource,
  options: AxonServerQueryBusOptions = {},
): SubscriptionCapableQueryBus<U> {
  const {
    connection,
    serializer,
    metadata: axonMetadata,
  } = contextView(conn, options.context ?? conn.connection.config.context)
  const shutdownLatch = conn.shutdown
  const requestTimeoutMs = positiveInteger(options.timeoutMs ?? 30000, "timeoutMs")
  if (requestTimeoutMs > 2_147_483_647) throw new RangeError("timeoutMs exceeds the timer range")
  const inboundAdmission = messagingAdmission("inbound handlers", options.limits?.maxConcurrentHandlers ?? 128, options.limits?.observe)
  const outboundAdmission = messagingAdmission("pending requests", options.limits?.maxPendingRequests ?? 1024, options.limits?.observe)
  const resilience = options.resilience ?? conn.resilience
  const metadata = axonMetadata()
  const PERMITS = BigInt(positiveInteger(options.flowControl?.permits ?? Number(DEFAULT_PERMITS), "flowControl.permits"))
  const THRESHOLD = BigInt(options.flowControl?.refillThreshold ?? Math.floor(Number(PERMITS) / 2))
  if (THRESHOLD < 0n || THRESHOLD >= PERMITS) throw new RangeError("refillThreshold must be between zero and permits - 1")
  const shortcutQueriesToLocalHandlers = options.shortcutQueriesToLocalHandlers ?? false
  const queryTimeoutMs = options.timeoutMs ?? DEFAULT_QUERY_TIMEOUT_MS
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)

  /** Query names announced to Axon Server; the handlers live on `next`. */
  const subscribedNames = new Set<string>()

  // Local subscription store — subscription queries opened by THIS instance.
  // Inbound updates from the server are offered into these via the Subscription RPC loop.
  const subscriptions = new Map<string, UpdateHandler>()

  // Subscriptions the SERVER has routed to this instance as the handler.
  // Populated when the server delivers SubscriptionQueryRequest.subscribe over OpenStream.
  // emitUpdate / completeSubscription apply the caller-supplied filter against these
  // to decide which subscriber IDs to target; the server forwards each response to the
  // exact subscriber.
  const handlerSubscriptions = new Map<string, { queryName: string; payload: unknown }>()
  type ResponseCredit = { ready: Promise<void>; grant(): void; cancel(): void; cancelled: boolean; timer: ReturnType<typeof setTimeout> }
  const responseCredits = new Map<string, ResponseCredit>()
  function cancelResponseCredits() {
    for (const credit of responseCredits.values()) { clearTimeout(credit.timer); credit.cancel() }
    responseCredits.clear()
  }
  function responseCredit(identifier: string): ResponseCredit | undefined {
    const existing = responseCredits.get(identifier)
    if (existing) return existing
    if (responseCredits.size >= (options.limits?.maxPendingRequests ?? 1024)) return undefined
    let grant!: () => void
    const ready = new Promise<void>((resolve) => { grant = resolve })
    const credit: ResponseCredit = {
      ready, grant, cancelled: false,
      cancel() { this.cancelled = true; grant() },
      timer: setTimeout(() => {
        credit.cancel()
        if (responseCredits.get(identifier) === credit) responseCredits.delete(identifier)
      }, requestTimeoutMs),
    }
    credit.timer.unref?.()
    responseCredits.set(identifier, credit)
    return credit
  }


  shutdownLatch.onShutdown(() => handlerSubscriptions.clear())

  let outbound = outboundStream<any>()
  let streamStarted = false
  let providerAbort = new AbortController()
  connection.onDisconnect?.(() => { cancelResponseCredits(); providerAbort.abort(); outbound.close() })
  let permits = 0n

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    const inbound = connection.queries.openStream(outbound.iterable, { metadata, signal: providerAbort.signal })
    void processInboundQueries(inbound, outbound)
  }

  function grantQueryPermits() {
    outbound.send({
      flowControl: { clientId: connection.config.clientId, permits: PERMITS },
      instructionId: "",
    })
    permits += PERMITS
  }

  function sendSubscribe(queryName: string) {
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

  /**
   * Re-establish the bidirectional stream and re-announce all handlers.
   * Called on stream error or when the connection reconnects.
   *
   * ORDER (preserves Axon-specific invariant): subscriptions are
   * re-emitted BEFORE the permits frame.
   */
  function reestablishStreamBody() {
    cancelResponseCredits()
    handlerSubscriptions.clear()
    outbound.close()
    outbound = outboundStream<any>()
    streamStarted = false
    permits = 0n
    ensureStreamStarted()
    for (const queryName of subscribedNames) sendSubscribe(queryName)
    grantQueryPermits()
  }

  const recovery = streamRecovery(reestablishStreamBody,
    () => !shutdownLatch.shuttingDown && connection.state !== "closed" && connection.state !== "disconnected" && connection.state !== "reconnecting",
    resilience)
  shutdownLatch.onShutdown(recovery.stop)

  // Auto-reestablish when the connection reconnects (e.g., after heartbeat timeout)
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
    let credit: ResponseCredit | undefined
    const supports = (key: number) => proto.processingInstructions?.some((instruction: any) => instruction.key === key && instruction.value?.booleanValue)
    if (!subId && supports(7) && supports(8)) {
      // Response credits may precede the query on Axon's provider stream.
      // Retain those credits by request ID, within the same bounded table.
      credit = responseCredit(proto.messageIdentifier)
      if (!credit) { responses.close(); return }
    }
    let errorCode = ""
    let errorMsg = ""
    try {
      try {
        activity = shutdownLatch.registerActivity()
        admission = inboundAdmission.enter()
        if (subscribedNames.has(proto.query)) {
          const queryMessage: QueryMessage = {
            kind: "query",
            identifier: proto.messageIdentifier,
            name: qualifiedNameFromString(proto.query),
            payload: deserializePayload(proto.payload?.data, proto.payload?.type, proto.payload?.revision),
            metadata: metadataFromProto(proto.metaData ?? {}),
            timestamp: Number(proto.timestamp),
          }
          if (subId) {
            subscriptionEntry = handlerSubscriptions.get(subId)
            if (!subscriptionEntry) return
          }
          // Every wire query enters the local bus with a fresh unit of work.
          const result = await next.query(queryMessage)
          payload = result !== undefined ? serializePayload("result", result) : undefined
        } else {
          errorCode = AxonServerErrorCode.NO_HANDLER_FOR_QUERY
          errorMsg = `No next handler for query "${proto.query}"`
        }
      } catch (err) {
        errorCode = AxonServerErrorCode.QUERY_EXECUTION_ERROR
        errorMsg = err instanceof Error ? err.message : String(err)
      }
      // An unsubscribe or completion can overtake a slow initial handler.
      if (subId && subscriptionEntry && handlerSubscriptions.get(subId) !== subscriptionEntry) return
      if (subId && errorCode) handlerSubscriptions.delete(subId)
      if (credit) {
        await credit.ready
        if (credit.cancelled) return
      }
      const response = {
        messageIdentifier: generateIdentifier(),
        requestIdentifier: proto.messageIdentifier,
        errorCode,
        errorMessage: errorCode
          ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
          : undefined,
        payload,
        metaData: {},
        processingInstructions: [],
      }
      if (subId) {
        responses.send({
          subscriptionQueryResponse: {
            messageIdentifier: generateIdentifier(), subscriptionIdentifier: subId, initialResult: response,
          },
          instructionId: "",
        })
      } else {
        responses.send({ queryResponse: response, instructionId: "" })
        responses.send({
          queryComplete: { messageId: generateIdentifier(), requestId: proto.messageIdentifier },
          instructionId: "",
        })
      }
      await responses.flush()
    } finally {
      if (credit) clearTimeout(credit.timer)
      if (credit && responseCredits.get(proto.messageIdentifier) === credit) responseCredits.delete(proto.messageIdentifier)
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
        // Every Axon query instruction consumes a provider credit, including
        // acknowledgements. Ignoring a late subscription ack can exhaust a
        // one-credit window between otherwise successful requests.
        permits--
        if (permits <= THRESHOLD && !shutdownLatch.shuttingDown) grantQueryPermits()
        if (message.queryFlowControl?.permits > 0n) {
          const identifier = message.queryFlowControl.queryReference?.requestId
          if (identifier) {
            const credit = responseCredit(identifier)
            if (!credit) { responses.close(); return }
            credit.grant()
          }
        }
        if (message.queryCancel) responseCredits.get(message.queryCancel.requestId)?.cancel()
        const request = message.subscriptionQueryRequest
        if (request) {
          if (request.unsubscribe) handlerSubscriptions.delete(request.unsubscribe.subscriptionIdentifier)
          const sub = request.subscribe
          if (sub?.subscriptionIdentifier && sub.queryRequest) {
            try {
              if (shutdownLatch.shuttingDown) throw new Error("Shutdown in progress")
              if (handlerSubscriptions.size >= 1024 && !handlerSubscriptions.has(sub.subscriptionIdentifier)) throw new Error("Provider subscription capacity exhausted")
              const proto = sub.queryRequest
              handlerSubscriptions.set(sub.subscriptionIdentifier, {
                queryName: proto.query,
                payload: deserializePayload(proto.payload?.data, proto.payload?.type, proto.payload?.revision),
              })
            } catch (err) {
              responses.send({
                subscriptionQueryResponse: {
                  messageIdentifier: generateIdentifier(), subscriptionIdentifier: sub.subscriptionIdentifier,
                  completeExceptionally: {
                    errorCode: AxonServerErrorCode.QUERY_EXECUTION_ERROR,
                    errorMessage: { message: err instanceof Error ? err.message : String(err) },
                  },
                },
                instructionId: "",
              })
            }
          }
          // Axon separates update registration from requesting the initial
          // result. Running the handler on Subscribe answers the wrong phase.
          const initial = request.getInitialResult
          if (initial?.subscriptionIdentifier && initial.queryRequest) {
            void handleInboundQuery(initial.queryRequest, responses, initial.subscriptionIdentifier).catch((err) => {
              console.error("Axon Server query bus: inbound subscription response failed", err)
            })
          }
          continue
        }
        if (!message.query) continue
        void handleInboundQuery(message.query, responses).catch((err) => {
          console.error("Axon Server query bus: inbound response failed", err)
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

        // Local shortcut — handle locally if a handler is co-located. The
        // caller's unit of work is passed straight through, so `next` makes the
        // nest-or-open decision on the HANDLE exactly as it does for an
        // in-process read: a live unit of work handed in by `ctx.query` is
        // reused so the consulting read shares the caller's transaction.
        if (shortcutQueriesToLocalHandlers && subscribedNames.has(queryName)) {
          return await next.query(unstamped, uow)
        }

      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire with no instant yet gets one from system
      // time here — the envelope crosses a process boundary and must be fully
      // formed. A locally-shortcut message is handed to `next` untouched
      // instead, so the task that handles it supplies the instant.
        const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }

        const responseStream = connection.queries.query(
          {
            messageIdentifier: message.identifier,
            query: queryName,
            timestamp: BigInt(message.timestamp),
            payload: serializePayload(queryName, message.payload),
            metaData: metadataToProto(message.metadata),
            processingInstructions: defaultQueryInstructions(queryTimeoutMs),
            clientId: connection.config.clientId,
            componentName: connection.config.componentName,
          },
          { metadata, signal: deadline.signal },
        )

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

    subscribe(
      queryName: string,
      handler: (message: QueryMessage, uow: U) => Promise<unknown>,
    ) {
      subscribedNames.add(queryName)
      next.subscribe(queryName, handler)

      ensureStreamStarted()
      sendSubscribe(queryName)
      // Permits AFTER subscription (Axon-specific ordering invariant)
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
      const serialized = serializePayload(queryName, message.payload)
      const subscriptionId = generateIdentifier()

      const outboundSub = outboundStream<any>()

      const window = Math.min(1024, Math.max(256, Math.floor(bufferSize ?? 256)))
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
            metaData: metadataToProto(message.metadata),
            processingInstructions: defaultQueryInstructions(queryTimeoutMs),
            clientId: connection.config.clientId,
            componentName: connection.config.componentName,
          },
        },
      })

      // Subscribe does not grant update credits on Axon Server; a separate
      // FlowControl frame initializes the subscription stream's update window.
      outboundSub.send({ flowControl: { numberOfPermits: BigInt(window) } })

      outboundSub.send({
        getInitialResult: {
          subscriptionIdentifier: subscriptionId,
          numberOfPermits: 1n,
          queryRequest: {
            messageIdentifier: message.identifier,
            query: queryName,
            timestamp: BigInt(message.timestamp),
            payload: serialized,
            metaData: metadataToProto(message.metadata),
            processingInstructions: defaultQueryInstructions(queryTimeoutMs),
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
      }, uow)
    },

    async completeSubscription(queryName: string, filter?: SubscriptionFilter, uow?: UnitOfWork): Promise<void> {
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
      }, uow)
    },
  }

  return routing
}
