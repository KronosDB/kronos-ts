/**
 * The Axon Server command and query buses.
 *
 * Axon Server is a SMART HUB: outbound dispatch always goes to the server, and
 * the server decides which node handles it — there is no client-side
 * prefer-local fork here, which is the whole difference from the dumb-pipe
 * broker in `@kronos-ts/rabbitmq`.
 *
 * Both buses are plain functions over the shared connection and YOUR local bus:
 *
 * ```ts
 * const commandBus = interceptingCommandBus(
 *   axonServerCommandBus(axon, simpleCommandBus(unitOfWork)), lineage)
 * const queryBus = interceptingQueryBus(
 *   axonServerQueryBus(axon, simpleQueryBus(unitOfWork)), lineage)
 * ```
 *
 * Axon-specific protocol invariants are preserved byte-for-byte:
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
  withRetry,
  type ResilienceConfig,
} from "@kronos-ts/core"
import type {
  CommandBus,
  CommandMessage,
  QueryBus,
  QueryMessage,
  SubscriptionFilter,
  SubscriptionQueryResult,
  UnitOfWork,
  Unstamped,
  UpdateHandler,
} from "@kronos-ts/core"
import {
  applySubscriptionFilter,
  stamped,
  updateHandler,
  runAfterCommitOrImmediately,
} from "@kronos-ts/core"
import type { AxonServerBusSource } from "./connection.js"
import { contextView } from "./context-view.js"
import { metadataToProto, metadataFromProto } from "./metadata-conversion.js"
import { outboundStream } from "./outbound-stream.js"
import { mapErrorCode, AxonServerErrorCode } from "./errors.js"

/** Default flow control settings — aligned with Java's 5000 permits. */
const DEFAULT_PERMITS = 5000n
const DEFAULT_THRESHOLD = 2500n

/** Default query dispatch timeout — aligned with Java's one hour. */
const DEFAULT_QUERY_TIMEOUT_MS = 3_600_000

/** Default command handler load factor — aligned with Java's 100. */
const DEFAULT_LOAD_FACTOR = 100

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

/**
 * Tuning for {@link axonServerCommandBus}. Every field has a working default;
 * the two arguments that carry meaning — the connection and your local bus —
 * are positional, and this record is the trailing remainder.
 */
export interface AxonServerCommandBusOptions {
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
}

/**
 * Tuning for {@link axonServerQueryBus}. See {@link AxonServerCommandBusOptions}.
 */
export interface AxonServerQueryBusOptions {
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
   * Default timeout for query dispatch in ms. Default: 3600000 (1 hour).
   * Aligned with Java's processing instruction timeout.
   */
  timeoutMs?: number
  /** Retry policy for stream re-establishment. Default: the connection's. */
  resilience?: Partial<ResilienceConfig>
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
 * A command bus backed by Axon Server, over YOUR local bus.
 *
 * - **Outbound dispatch**: ALWAYS through Axon Server, via the unary Dispatch
 *   RPC. Axon Server routes the command to the appropriate node (which may be
 *   this one). There is deliberately no client-side prefer-local fork: the hub
 *   is the router, and short-circuiting it would silently defeat load factors,
 *   priorities and routing keys.
 * - **Inbound**: a command the server routes here is dispatched into `local` —
 *   not into a privately-held handler map. That is what makes the unit-of-work
 *   policy you chose for `local` (say `postgresUnitOfWork(pg, unitOfWork)`)
 *   apply to server-routed work exactly as it applies to work this process
 *   originated. It is also why this function takes no `unitOfWork` argument:
 *   `local` carries that policy now.
 * - **subscribe**: registers the handler on `local` AND announces the name to
 *   Axon Server, so other nodes can route to us.
 *
 * ## Correlation lineage and the interceptor layer
 *
 * The returned bus stamps no lineage of its own. A host that wants it wraps the
 * OUTERMOST bus:
 *
 * ```ts
 * interceptingCommandBus(axonServerCommandBus(conn, local), lineage)
 * ```
 *
 * so whatever a host adds runs BEFORE the message is serialized onto the wire.
 * Lineage itself is usually already on `message.metadata` by then — `ctx.send`
 * stamps the unit of work's correlation data before any bus sees the message.
 *
 * This is precisely how the Java client does it. AF4's `AxonServerCommandBus`
 * holds its own `DispatchInterceptors` and dispatches as
 * `doDispatch(dispatchInterceptors.intercept(commandMessage), cb)` — one call
 * site, at the top, ahead of any routing. AF5 keeps the property via decorator
 * order: `DISTRIBUTED_COMMAND_BUS_ORDER = InterceptingCommandBus.DECORATION_ORDER - 50`
 * stacks `InterceptingCommandBus → DistributedCommandBus → SimpleCommandBus`.
 *
 * If `local` is itself an intercepting bus, a server-routed command sees
 * `lineage` twice. That is harmless: both of its fields are `??` seeds, so the
 * second application finds them set and changes nothing.
 */
export function axonServerCommandBus(
  conn: AxonServerBusSource,
  local: CommandBus,
  options: AxonServerCommandBusOptions = {},
): CommandBus {
  const {
    connection,
    serializer,
    metadata: axonMetadata,
  } = contextView(conn, options.context ?? conn.connection.config.context)
  const shutdownLatch = conn.shutdown
  const resilience = options.resilience ?? conn.resilience
  const metadata = axonMetadata()
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)
  const PERMITS = BigInt(options.flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(options.flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))
  const loadFactor = options.loadFactor ?? DEFAULT_LOAD_FACTOR

  /**
   * The names this node announced to Axon Server. The handlers themselves live
   * on `local`; this set exists so an inbound command for a name we never
   * subscribed still answers NO_HANDLER_FOR_COMMAND rather than whatever
   * `local.dispatch` happens to throw — and so a reconnect can re-announce.
   */
  const subscribedNames = new Set<string>()

  // Bidirectional stream for handler subscription + inbound command handling
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
        console.error("Axon Server command bus: reconnect retries exhausted", err)
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

        if (subscribedNames.has(commandName)) {
          try {
            const commandMessage: CommandMessage = {
              kind: "command",
              identifier: proto.messageIdentifier,
              name: qualifiedNameFromString(commandName),
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined),
              metadata: metadataFromProto(proto.metaData),
              timestamp: Number(proto.timestamp),
            }

            // Through the LOCAL BUS, so the caller's unit-of-work policy runs.
            // AF parity is preserved: `CommandProcessingTask` runs the local
            // segment without re-running dispatch interceptors, and a `local`
            // that happens to carry `lineage` re-applies a pair of `??` seeds
            // that are already set.
            resultPayload = await local.dispatch(commandMessage)
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
              ? {
                  message: errorMsg,
                  location: connection.config.componentName,
                  details: [],
                  errorCode,
                }
              : undefined,
            payload:
              resultPayload !== undefined ? serializePayload("result", resultPayload) : undefined,
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

      console.error(
        "Axon Server command bus: inbound stream error, attempting re-establishment via withRetry",
        err,
      )
      await reestablishStreamWithRetry().catch((retryErr) => {
        console.error("Axon Server command bus: reconnect retries exhausted", retryErr)
      })
    }
  }

  return {
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
          { metadata },
        )

        if (response.errorCode && response.errorCode !== "") {
          throw mapErrorCode(response.errorCode, response.errorMessage?.message ?? "Unknown error")
        }

        return deserializePayload(response.payload?.data as Uint8Array | undefined)
      } finally {
        activity.end()
      }
    },

    subscribe(
      commandName: string,
      handler: (message: CommandMessage, uow: UnitOfWork) => Promise<unknown>,
    ) {
      subscribedNames.add(commandName)
      local.subscribe(commandName, handler)

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
 * A query bus backed by Axon Server, over YOUR local bus.
 *
 * Same architecture as {@link axonServerCommandBus}: outbound dispatch goes
 * through Axon Server, and a query the server routes here runs through `local`,
 * so your unit-of-work policy applies to server-routed reads too. `subscribe`
 * registers on `local` and announces the name to the server.
 *
 * The one asymmetry with commands is `shortcutQueriesToLocalHandlers` — Java
 * has it for queries and not for commands, and so do we. When it is on and this
 * node subscribed the name, `query()` goes straight to `local` and the caller's
 * unit of work is passed through, so the local branch nests exactly as the
 * in-process bus does.
 *
 * Lineage, if wanted, is `interceptingQueryBus(bus, lineage)` at the host,
 * matching AF4's `AxonServerQueryBus`, which calls
 * `dispatchInterceptors.intercept(...)` at the top of `query`, `streamingQuery`,
 * `scatterGather` and `subscriptionQuery`. Because the wrap is outside, the
 * shortcut branch gets identical lineage to the remote branch.
 *
 * KNOWN GAP: `subscriptionQuery` / `subscribeToUpdates` build their proto
 * straight from `message.metadata`, and `interceptingQueryBus` (in
 * `@kronos-ts/core`) forwards those two calls to the delegate without
 * running the dispatch chain. Closing that needs a core change.
 */
export function axonServerQueryBus(
  conn: AxonServerBusSource,
  local: QueryBus,
  options: AxonServerQueryBusOptions = {},
): QueryBus {
  const {
    connection,
    serializer,
    metadata: axonMetadata,
  } = contextView(conn, options.context ?? conn.connection.config.context)
  const shutdownLatch = conn.shutdown
  const resilience = options.resilience ?? conn.resilience
  const metadata = axonMetadata()
  const PERMITS = BigInt(options.flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(options.flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))
  const shortcutQueriesToLocalHandlers = options.shortcutQueriesToLocalHandlers ?? false
  const queryTimeoutMs = options.timeoutMs ?? DEFAULT_QUERY_TIMEOUT_MS
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)

  /** Query names announced to Axon Server; the handlers live on `local`. */
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
    outbound.close()
    outbound = outboundStream<any>()
    streamStarted = false
    permits = 0n
    ensureStreamStarted()
    for (const queryName of subscribedNames) sendSubscribe(queryName)
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
        console.error("Axon Server query bus: reconnect retries exhausted", err)
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

      if (subscribedNames.has(queryName)) {
        try {
          const queryMessage: QueryMessage = {
            kind: "query",
            identifier: proto.messageIdentifier,
            name: qualifiedNameFromString(queryName),
            payload,
            metadata: metadataFromProto(proto.metaData ?? {}),
            timestamp: Number(proto.timestamp),
          }
          resultPayload = await local.query(queryMessage)
        } catch (err) {
          errorCode = AxonServerErrorCode.QUERY_EXECUTION_ERROR
          errorMsg = err instanceof Error ? err.message : String(err)
        }
      } else {
        errorCode = AxonServerErrorCode.NO_HANDLER_FOR_QUERY
        errorMsg = `No local handler for query "${queryName}"`
      }

      const responseSerialized =
        resultPayload !== undefined ? serializePayload("result", resultPayload) : undefined

      outbound.send({
        subscriptionQueryResponse: {
          messageIdentifier: generateIdentifier(),
          subscriptionIdentifier: subId,
          initialResult: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? {
                  message: errorMsg,
                  location: connection.config.componentName,
                  details: [],
                  errorCode,
                }
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

        let resultPayload: unknown
        let errorCode = ""
        let errorMsg = ""

        if (subscribedNames.has(queryName)) {
          try {
            const queryMessage: QueryMessage = {
              kind: "query",
              identifier: proto.messageIdentifier,
              name: qualifiedNameFromString(queryName),
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined),
              metadata: metadataFromProto(proto.metaData),
              timestamp: Number(proto.timestamp),
            }

            // Through the LOCAL BUS: no unit of work is handed in, so `local`
            // opens one under whatever policy the caller gave it.
            resultPayload = await local.query(queryMessage)
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
              ? {
                  message: errorMsg,
                  location: connection.config.componentName,
                  details: [],
                  errorCode,
                }
              : undefined,
            payload:
              resultPayload !== undefined ? serializePayload("result", resultPayload) : undefined,
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

      console.error(
        "Axon Server query bus: inbound stream error, attempting re-establishment via withRetry",
        err,
      )
      await reestablishStreamWithRetry().catch((retryErr) => {
        console.error("Axon Server query bus: reconnect retries exhausted", retryErr)
      })
    }
  }

  const routing: QueryBus = {
    async query(unstamped: Unstamped<QueryMessage>, uow?: UnitOfWork): Promise<unknown> {
      const activity = shutdownLatch.registerActivity()
      try {
        const queryName = qualifiedNameToString(unstamped.name)

        // Local shortcut — handle locally if a handler is co-located. The
        // caller's unit of work is passed straight through, so `local` makes the
        // nest-or-open decision on the HANDLE exactly as it does for an
        // in-process read: a live unit of work handed in by `ctx.query` is
        // reused so the consulting read shares the caller's transaction.
        if (shortcutQueriesToLocalHandlers && subscribedNames.has(queryName)) {
          return local.query(unstamped, uow)
        }

      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire still {@link Unstamped} is therefore
      // stamped from system time here — the envelope crosses a process boundary
      // and must be fully formed. A locally-shortcut message is handed to
      // `local` unstamped instead, so the task that handles it supplies the
      // instant.
        const message = stamped(unstamped, Date.now)

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
          { metadata },
        )

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

    subscribe(
      queryName: string,
      handler: (message: QueryMessage, uow: UnitOfWork) => Promise<unknown>,
    ) {
      subscribedNames.add(queryName)
      local.subscribe(queryName, handler)

      ensureStreamStarted()
      sendSubscribe(queryName)
      // Permits AFTER subscription (Axon-specific ordering invariant)
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
            processingInstructions: defaultQueryInstructions(queryTimeoutMs),
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
            processingInstructions: defaultQueryInstructions(queryTimeoutMs),
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
                  rejectInitial(
                    mapErrorCode(
                      initial.errorCode,
                      initial.errorMessage?.message ?? "Unknown error",
                    ),
                  )
                } else {
                  resolveInitial(
                    deserializePayload(initial.payload?.data as Uint8Array | undefined),
                  )
                }
                initialSettled = true
              }
            } else if (response.update) {
              const update = deserializePayload(
                response.update.payload?.data as Uint8Array | undefined,
              )
              handler.offer(update)
            } else if (response.complete) {
              handler.complete()
              break
            } else if (response.completeExceptionally) {
              handler.completeExceptionally(
                new Error(
                  response.completeExceptionally.errorMessage?.message ??
                    "Subscription query failed",
                ),
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

    async completeSubscription(queryName: string, filter?: SubscriptionFilter): Promise<void> {
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

  return routing
}
