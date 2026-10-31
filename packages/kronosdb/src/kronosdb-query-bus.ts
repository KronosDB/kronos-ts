/**
 * The KronosDB query bus, over the native protocol: point-to-point and
 * scatter-gather queries, and subscription queries on both sides.
 *
 * A query goes out as one DispatchQuery on a stream of its own and comes back
 * as QueryResponse frames, one per answering handler, then QueryEnd. A
 * subscription query is a stream that stays open: the initial result, the
 * updates, a completion. Queries the server routes HERE arrive on a handler
 * stream (see `native-handlers.ts`), and a subscription we serve is answered
 * on the stream it was asked on.
 */
import {
  applySubscriptionFilter,
  generateIdentifier,
  messagingAdmission,
  qualifiedNameFromString,
  qualifiedNameToString,
  runAfterCommitOrImmediately,
  updateHandler,
  type MessagingLimits,
  type QualifiedName,
  type QueryBus,
  type QueryMessage,
  type SerializedObject,
  type SubscriptionCapableQueryBus,
  type SubscriptionFilter,
  type SubscriptionQueryResult,
  type UnitOfWork,
  type UpdateHandler,
} from "@kronos-ts/core"
import { KronosDbErrorCode, mapErrorCode } from "./errors.js"
import type { BusHost } from "./kronosdb.js"
import { handlerClientId, handlerStream, type HandlerSession } from "./native-handlers.js"
import {
  creditWindow,
  deserializeResult,
  instructionBytes,
  metadataFromEntries,
  readMessage,
  readResult,
  requestTimeout,
  writeMessage,
  writeResult,
  type FlowControlConfig,
  type IncomingMessage,
  type OutgoingMessage,
} from "./native-messaging.js"
import { busStream, unexpected } from "./native-requests.js"
import type { FrameSink, NativeStream } from "./native-transport.js"
import {
  FLAG_END,
  FrameKind,
  decodeHandleSubscriptionQuery,
  decodeSubscriptionEvent,
  decodeSubscriptionQueryPermits,
  decodeCancelSubscriptionQuery,
  encodeCredit,
  encodeOpenQueryHandlers,
  encodeQueryDone,
  encodeSubscribeQuery,
  encodeSubscriptionQueryHandled,
  reader,
  readStr16,
  writeStr16,
  writeU16,
  writeU64,
  type Payload,
  type SubscriptionEvent,
  type Writer,
} from "./protocol/kronosdb-protocol.js"
import type { ResilienceConfig } from "./resilience.js"
import { shutdownLatch as shutdownLatchValue, type ActivityHandle } from "./shutdown-latch.js"

/** Per-bus knobs for queries. */
export type KronosDbQueryBusOptions = {
  flowControl?: FlowControlConfig
  /** Client and server query timeout. Default: 30000ms. */
  timeoutMs?: number
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
  /** Bounded admission; excess nested work receives an overload error. */
  limits?: MessagingLimits
}

/** Updates a provider holds for a subscriber that has no credit left, before it gives the subscription up. */
const MAX_HELD_UPDATES = 1024
const MAX_SUBSCRIPTIONS = 1024

const wirePayload = (payload: SerializedObject): Payload => ({
  typeName: payload.type,
  revision: payload.revision,
  data: payload.data,
})

/** What this side serves of a subscription query somebody else subscribed to. */
type ProvidedSubscription = {
  readonly queryName: string
  readonly payload: unknown
  /** The handler-stream incarnation it was asked on, the one every event goes back on. */
  readonly session: HandlerSession
  /** Updates the subscriber will take before it grants more. */
  permits: number
  /** Updates waiting for credit, oldest first. */
  readonly held: SerializedObject[]
  /** A terminal event waiting behind the held updates. */
  ending: SubscriptionEvent | undefined
}

/**
 * A query bus backed by KronosDB.
 *
 * Correlation, if wanted, is `interceptingQueryBus(bus, correlation)` at the host, for the same reason
 * {@link kronosDbCommandBus} is wrapped — AF runs dispatch interception at the
 * top of `query` / `subscriptionQuery`, before anything is sent. A co-located
 * handler answers without the server under `localFirstQueryBus(bus, local)`;
 * wrapping correlation outside that stamps both branches identically.
 *
 * `bus` is the same server-scoped namespace string the command side takes (see
 * {@link kronosDbCommandBus}: bound to an id on the connection, `"default"`
 * when omitted, no relation to event store contexts).
 *
 * A query is asked of ONE handler (`nr_of_results` 1): the first answer is
 * the result. The server scatters a query to every handler when it is asked
 * for more; this bus is the point-to-point face of that.
 */
export function kronosDbQueryBus<U extends UnitOfWork = UnitOfWork>(
  next: QueryBus<U>,
  kdb: BusHost,
  bus: string = "default",
  options: KronosDbQueryBusOptions = {},
): SubscriptionCapableQueryBus<U> {
  const { flowControl, resilience } = options
  const serializer = kdb.serializer
  const connection = kdb.connection
  const link = connection.native
  const { clientId, componentName } = connection.config
  const handlerId = handlerClientId(connection, "query", bus)
  const shutdownLatch = shutdownLatchValue()
  const requestTimeoutMs = requestTimeout(options.timeoutMs)
  const inboundAdmission = messagingAdmission("inbound handlers", options.limits?.maxConcurrentHandlers ?? 128, options.limits?.observe)
  const outboundAdmission = messagingAdmission("pending requests", options.limits?.maxPendingRequests ?? 1024, options.limits?.observe)
  kdb.registerShutdownLatch(shutdownLatch)

  const { permits: PERMITS, threshold: THRESHOLD } = creditWindow(flowControl)
  const queryInstructions = instructionBytes({ timeoutMs: requestTimeoutMs, nrOfResults: 1 })

  // As on the command side: names announced to the server, kept so a reconnect
  // can re-announce them and an unroutable inbound query still answers
  // NO_HANDLER_FOR_QUERY.
  const localHandlers = new Set<string>()
  const names = new Map<string, QualifiedName>()
  const nameOf = (name: string): QualifiedName => {
    let known = names.get(name)
    if (known === undefined) {
      known = qualifiedNameFromString(name)
      names.set(name, known)
    }
    return known
  }
  const subscriptions = new Map<string, UpdateHandler>()
  // Subscriptions the SERVER has routed to this instance as the handler. Each
  // entry was opened by some subscriber (possibly remote) for a query name we
  // registered as a handler for. emitUpdate / completeSubscription apply the
  // caller-supplied filter against these to decide which subscribers to
  // target; the server routes each event back to the exact subscriber.
  const provided = new Map<string, ProvidedSubscription>()

  // The permits the server holds for this stream incarnation; undefined
  // until the first grant. Permits belong to the stream, not to a name.
  let credit: number | undefined

  function grant(session: HandlerSession): void {
    session.send(FrameKind.Credit, (w) => encodeCredit(w, { credits: PERMITS }))
    credit = (credit ?? 0) + PERMITS
  }

  function subscribeOn(session: HandlerSession, queryName: string): void {
    session.instruct(FrameKind.SubscribeQuery, (w, instructionId) =>
      encodeSubscribeQuery(w, { instructionId, query: queryName, resultName: "" }),
    )
  }

  /** A query or subscription arrived: one permit spent, and the window refilled when it runs low. */
  function spend(session: HandlerSession): void {
    if (credit === undefined) return
    credit -= 1
    // Receive credits cannot depend on handler completion: all admitted
    // parents could be waiting for children on this same stream.
    if (credit <= THRESHOLD && !shutdownLatch.shuttingDown) grant(session)
  }

  const stream = handlerStream({
    link,
    bus,
    openKind: FrameKind.OpenQueryHandlers,
    writeOpen: (w: Writer, busId: number) => encodeOpenQueryHandlers(w, { bus: busId, clientId: handlerId, componentName }),
    canReconnect: () =>
      !shutdownLatch.shuttingDown &&
      connection.state !== "closed" &&
      connection.state !== "disconnected" &&
      connection.state !== "reconnecting",
    resilience,
    announce(session) {
      credit = undefined
      for (const queryName of localHandlers) subscribeOn(session, queryName)
      if (localHandlers.size > 0) grant(session)
    },
    onFrame(session, kind, buf, start, end) {
      switch (kind) {
        case FrameKind.HandleQuery: {
          const incoming = readMessage(reader(buf, start, end))
          spend(session)
          void handleQuery(incoming, session).catch((err) => {
            console.error("KronosDB query bus: inbound response failed", err)
          })
          return
        }
        case FrameKind.HandleSubscriptionQuery: {
          const { subscription } = decodeHandleSubscriptionQuery(reader(buf, start, end))
          const query = subscription.query
          const incoming: IncomingMessage = {
            messageId: query.messageId,
            name: query.name,
            timestamp: query.timestamp,
            payload: { type: query.payload.typeName, revision: query.payload.revision, data: query.payload.data },
            metadata: metadataFromEntries(query.metadata),
          }
          spend(session)
          void handleSubscription(subscription.subscriptionId, Number(subscription.permits), incoming, session).catch((err) => {
            console.error("KronosDB query bus: inbound subscription response failed", err)
          })
          return
        }
        case FrameKind.SubscriptionQueryPermits: {
          const granted = decodeSubscriptionQueryPermits(reader(buf, start, end))
          const sub = provided.get(granted.subscriptionId)
          if (sub !== undefined) {
            sub.permits += Number(granted.permits)
            drain(granted.subscriptionId, sub)
          }
          return
        }
        case FrameKind.CancelSubscriptionQuery:
          provided.delete(decodeCancelSubscriptionQuery(reader(buf, start, end)).subscriptionId)
          return
        // CancelQuery (the dispatcher gave up) and QueryPermits (a streaming
        // query) say nothing this bus acts on: a handler runs to its answer,
        // and the server drops what nobody is waiting for.
        default:
          return
      }
    },
    onEnded() {
      credit = undefined
      provided.clear()
    },
  })

  shutdownLatch.onShutdown(() => {
    stream.retire()
    provided.clear()
  })
  connection.onDisconnect(() => stream.abandon())
  connection.onReconnect(() => {
    if (!shutdownLatch.shuttingDown) stream.restart()
  })
  kdb.registerReadiness?.(() => stream.acknowledged())

  // -------------------------------------------------------------------------
  // Serving
  // -------------------------------------------------------------------------

  function queryMessage(incoming: IncomingMessage): QueryMessage {
    return {
      kind: "query",
      identifier: incoming.messageId,
      name: nameOf(incoming.name),
      payload: deserializeResult(serializer, incoming.payload),
      metadata: incoming.metadata,
      timestamp: incoming.timestamp,
    }
  }

  async function handleQuery(incoming: IncomingMessage, session: HandlerSession): Promise<void> {
    let activity: ActivityHandle | undefined
    let admission: ReturnType<typeof inboundAdmission.enter> | undefined
    let result: SerializedObject | undefined
    let errorCode = ""
    let errorMessage = ""
    try {
      try {
        activity = shutdownLatch.registerActivity()
        admission = inboundAdmission.enter()
        if (localHandlers.has(incoming.name)) {
          // Every wire query enters the local bus with a fresh unit of work.
          const value = await next.query(queryMessage(incoming))
          result = value !== undefined ? serializer.serialize(value, "result", "") : undefined
        } else {
          errorCode = KronosDbErrorCode.NO_HANDLER_FOR_QUERY
          errorMessage = `No next handler for query "${incoming.name}"`
        }
      } catch (err) {
        errorCode = KronosDbErrorCode.QUERY_EXECUTION_ERROR
        errorMessage = err instanceof Error ? err.message : String(err)
        result = undefined
      }
      const respond = () => {
        session.send(FrameKind.QueryHandled, (w) =>
          writeResult(w, { requestId: incoming.messageId, errorCode, errorMessage, location: componentName, payload: result }),
        )
        session.send(FrameKind.QueryDone, (w) => encodeQueryDone(w, { messageId: "", requestId: incoming.messageId }))
      }
      try {
        respond()
      } catch (err) {
        // A response the frame cannot hold is still an answer.
        errorCode = KronosDbErrorCode.QUERY_EXECUTION_ERROR
        errorMessage = err instanceof Error ? err.message : String(err)
        result = undefined
        respond()
      }
    } finally {
      admission?.end()
      activity?.end()
    }
  }

  function sendEvent(session: HandlerSession, subscriptionId: string, event: SubscriptionEvent): void {
    session.send(FrameKind.SubscriptionQueryHandled, (w) => encodeSubscriptionQueryHandled(w, { subscriptionId, event }))
  }

  const updateEvent = (payload: SerializedObject): SubscriptionEvent => ({
    kind: "update",
    value: {
      messageId: "",
      payload: wirePayload(payload),
      metadata: [],
      clientId,
      componentName,
      errorCode: "",
      error: undefined,
    },
  })

  const failureEvent = (message: string): SubscriptionEvent => ({
    kind: "completeExceptionally",
    value: {
      clientId,
      componentName,
      errorCode: KronosDbErrorCode.QUERY_EXECUTION_ERROR,
      error: { message, location: componentName, details: [], errorCode: KronosDbErrorCode.QUERY_EXECUTION_ERROR },
    },
  })

  /** Send what credit allows of the held updates, then the terminal event if it was waiting behind them. */
  function drain(subscriptionId: string, sub: ProvidedSubscription): void {
    while (sub.permits > 0 && sub.held.length > 0) {
      sub.permits--
      sendEvent(sub.session, subscriptionId, updateEvent(sub.held.shift()!))
    }
    if (sub.held.length === 0 && sub.ending !== undefined) {
      sendEvent(sub.session, subscriptionId, sub.ending)
      provided.delete(subscriptionId)
    }
  }

  function offer(subscriptionId: string, sub: ProvidedSubscription, update: SerializedObject): void {
    if (sub.ending !== undefined) return
    if (sub.permits > 0 && sub.held.length === 0) {
      sub.permits--
      sendEvent(sub.session, subscriptionId, updateEvent(update))
    } else if (sub.held.length >= MAX_HELD_UPDATES) {
      // The subscriber is not taking updates. Say so instead of dropping them silently.
      sendEvent(sub.session, subscriptionId, failureEvent("Subscription update buffer overflow: the subscriber is not granting credit"))
      provided.delete(subscriptionId)
    } else {
      sub.held.push(update)
    }
  }

  function finish(subscriptionId: string, sub: ProvidedSubscription, event: SubscriptionEvent): void {
    sub.ending = event
    drain(subscriptionId, sub)
  }

  async function handleSubscription(
    subscriptionId: string,
    permits: number,
    incoming: IncomingMessage,
    session: HandlerSession,
  ): Promise<void> {
    let activity: ActivityHandle | undefined
    let admission: ReturnType<typeof inboundAdmission.enter> | undefined
    let entry: ProvidedSubscription | undefined
    let result: SerializedObject | undefined
    let errorCode = ""
    let errorMessage = ""
    try {
      try {
        activity = shutdownLatch.registerActivity()
        admission = inboundAdmission.enter()
        if (localHandlers.has(incoming.name)) {
          const message = queryMessage(incoming)
          if (provided.size >= MAX_SUBSCRIPTIONS && !provided.has(subscriptionId)) throw new Error("Provider subscription capacity exhausted")
          entry = { queryName: incoming.name, payload: message.payload, session, permits, held: [], ending: undefined }
          provided.set(subscriptionId, entry)
          // Every wire query enters the local bus with a fresh unit of work.
          const value = await next.query(message)
          result = value !== undefined ? serializer.serialize(value, "result", "") : undefined
        } else {
          errorCode = KronosDbErrorCode.NO_HANDLER_FOR_QUERY
          errorMessage = `No next handler for query "${incoming.name}"`
        }
      } catch (err) {
        errorCode = KronosDbErrorCode.QUERY_EXECUTION_ERROR
        errorMessage = err instanceof Error ? err.message : String(err)
        result = undefined
      }
      // An unsubscribe or completion can overtake a slow initial handler.
      if (entry !== undefined && provided.get(subscriptionId) !== entry) return
      if (errorCode) provided.delete(subscriptionId)
      const initial: SubscriptionEvent = {
        kind: "initialResult",
        value: {
          messageId: "",
          requestId: incoming.messageId,
          errorCode,
          error: errorCode ? { message: errorMessage, location: componentName, details: [], errorCode } : undefined,
          payload: result === undefined ? undefined : wirePayload(result),
          metadata: [],
          instructions: [],
        },
      }
      sendEvent(session, subscriptionId, initial)
    } finally {
      admission?.end()
      activity?.end()
    }
  }

  // -------------------------------------------------------------------------
  // Asking
  // -------------------------------------------------------------------------

  /** Send one query and wait for its answer, and for the server to say the rest are done. */
  function request(outgoing: OutgoingMessage): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      let native: NativeStream | undefined
      let done = false
      let received = false
      let result: unknown
      let failure: Error | undefined
      const timer = setTimeout(() => {
        if (done) return
        done = true
        native?.cancel()
        reject(new Error(`Messaging deadline exceeded after ${requestTimeoutMs}ms`))
      }, requestTimeoutMs)

      const sink: FrameSink = (kind, flags, buf, start, end) => {
        if (done) return
        if (kind === FrameKind.QueryResponse) {
          // NR_OF_RESULTS is one. Take the first answer, and read to the end of
          // the stream so a transport failure cannot pass for a result.
          if (!received) {
            received = true
            try {
              const answer = readResult(reader(buf, start, end))
              if (answer.errorCode !== "") failure = mapErrorCode(answer.errorCode, answer.errorMessage || "Unknown error")
              else result = deserializeResult(serializer, answer.payload)
            } catch (error) {
              failure = error instanceof Error ? error : new Error(String(error))
            }
          }
          if ((flags & FLAG_END) === 0) return
        } else if (kind !== FrameKind.QueryEnd) {
          failure ??= unexpected(kind, "QueryResponse")
          if ((flags & FLAG_END) === 0) return
        }
        done = true
        clearTimeout(timer)
        if (failure !== undefined) reject(failure)
        else if (received) resolve(result)
        else reject(new Error(`No response for query "${outgoing.name}"`))
      }
      const fail = (error: Error): void => {
        if (done) return
        done = true
        clearTimeout(timer)
        reject(error)
      }

      try {
        const opened = busStream(
          link,
          bus,
          FrameKind.DispatchQuery,
          (w, busId) => {
            writeU16(w, busId)
            writeMessage(w, outgoing, queryInstructions, clientId, componentName)
          },
          true,
          sink,
          fail,
        )
        if (opened instanceof Promise) {
          opened.then((s) => {
            native = s
            if (done) s.cancel()
          }, fail)
        } else {
          native = opened
        }
      } catch (error) {
        fail(error instanceof Error ? error : new Error(String(error)))
      }
    })
  }

  const routing: SubscriptionCapableQueryBus<U> = {
    async query(unstamped: QueryMessage): Promise<unknown> {
      const activity = shutdownLatch.registerActivity()
      let admission: ReturnType<typeof outboundAdmission.enter> | undefined
      try {
        admission = outboundAdmission.enter(unstamped.identifier)
        const queryName = qualifiedNameToString(unstamped.name)

        // A transport is not a task: it has no unit of work, so it has no clock.
        // A message that reaches the wire with no instant yet gets one from system
        // time here — the envelope crosses a process boundary and must be fully
        // formed.
        return await request({
          messageId: unstamped.identifier,
          name: queryName,
          timestamp: unstamped.timestamp ?? Date.now(),
          payload: serializer.serialize(unstamped.payload, queryName, ""),
          metadata: unstamped.metadata,
        })
      } finally {
        admission?.end()
        activity.end()
      }
    },

    subscribe(queryName, handler) {
      localHandlers.add(queryName)
      next.subscribe(queryName, handler)

      const session = stream.session
      if (session === undefined) {
        // Not open yet, or coming back: opening announces every name.
        stream.start()
        return
      }
      subscribeOn(session, queryName)
      // The stream's window covers this name too.
      if (credit === undefined) grant(session)
    },

    subscriptionQuery(unstamped: QueryMessage, bufferSize?: number): SubscriptionQueryResult {
      if (shutdownLatch.shuttingDown) throw new Error("Messaging shutdown in progress")
      if (subscriptions.size >= MAX_SUBSCRIPTIONS) throw new Error(`Subscription capacity ${MAX_SUBSCRIPTIONS} exhausted`)
      const queryId = unstamped.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const queryName = qualifiedNameToString(unstamped.name)
      const subscriptionId = generateIdentifier()
      // Before anything is registered: a payload that cannot be serialized
      // leaves no subscription behind.
      const outgoing: OutgoingMessage = {
        messageId: queryId,
        name: queryName,
        timestamp: unstamped.timestamp ?? Date.now(),
        payload: serializer.serialize(unstamped.payload, queryName, ""),
        metadata: unstamped.metadata,
      }
      const handler = updateHandler(unstamped, bufferSize, () => subscriptions.delete(queryId))

      // CREDIT-BASED FLOW CONTROL, AND THE SUBSCRIBER OWES THE REFILL.
      //
      // The subscription opens with an initial window of credit. The server
      // decrements it per update and drops updates once it hits zero. Granting
      // a window and never refilling is therefore a subscription that works
      // for exactly `window` updates and then stops. We top the window back up
      // as updates are handed to the consumer, in batches of a quarter, so the
      // wire carries one small message per quarter-window instead of one per
      // update.
      //
      // THE SERVER CLAMPS THE INITIAL GRANT TO [1, 1024] and we mirror that
      // bound rather than trusting the number we asked for: a host passing
      // `bufferSize: 5000` is granted 1024, and refilling on a 5000-wide
      // window would let 1250 updates go by before topping up. The refill is
      // sized off what the server GAVE, not what we requested.
      const requested = bufferSize && bufferSize > 0 ? bufferSize : 256
      const window = Math.min(1024, Math.max(256, requested))
      const refillBatch = Math.max(1, Math.floor(window / 4))
      let consumedSinceRefill = 0
      let subscriptionClosed = false
      let initialSettled = false
      let explicitlyCompleted = false
      let native: NativeStream | undefined

      let resolveInitial!: (value: unknown) => void
      let rejectInitial!: (error: Error) => void
      const initialResult = new Promise<unknown>((resolve, reject) => {
        resolveInitial = resolve
        rejectInitial = reject
      })
      // Callers may consume updates without awaiting the initial result. Keep
      // the original promise rejectable without an unhandled rejection on close.
      void initialResult.catch(() => {})

      const initialTimer = setTimeout(() => closeSubscription(new Error("Subscription initial result timed out")), requestTimeoutMs)
      const removeShutdown = shutdownLatch.onShutdown(() => closeSubscription(new Error("Messaging shutdown in progress")))

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
        // Cancel ends the stream whether or not the server has; a stream that
        // already finished ignores it.
        native?.cancel()
        subscriptions.delete(queryId)
      }

      function endOfStream() {
        // EOF and completion frames must settle BOTH faces of a subscription.
        const missingInitial = !initialSettled
        if (!initialSettled) {
          rejectInitial(new Error("Subscription stream ended before initial result"))
          initialSettled = true
        }
        closeSubscription(!missingInitial && !explicitlyCompleted && !subscriptionClosed ? new Error("Subscription stream ended unexpectedly") : undefined)
      }

      const sink: FrameSink = (kind, flags, buf, start, end) => {
        if (subscriptionClosed) return
        let terminal = (flags & FLAG_END) !== 0
        try {
          if (kind !== FrameKind.SubscriptionQueryEvent) throw unexpected(kind, "SubscriptionQueryEvent")
          const r = reader(buf, start, end)
          readStr16(r)
          const event = decodeSubscriptionEvent(r)
          switch (event.kind) {
            case "initialResult": {
              const initial = event.value
              if (initialSettled) break
              if (initial.errorCode) throw mapErrorCode(initial.errorCode, initial.error?.message ?? "Unknown error")
              clearTimeout(initialTimer)
              resolveInitial(initial.payload === undefined ? undefined : deserializeResult(serializer, { type: initial.payload.typeName, revision: initial.payload.revision, data: initial.payload.data }))
              initialSettled = true
              break
            }
            case "update": {
              const update = event.value.payload
              const value = update === undefined ? undefined : deserializeResult(serializer, { type: update.typeName, revision: update.revision, data: update.data })
              if (!handler.offer(value)) throw new Error("Subscription query update buffer overflow")
              if (++consumedSinceRefill >= refillBatch) {
                const granted = consumedSinceRefill
                native?.send(FrameKind.Credit, (w) => encodeCredit(w, { credits: granted }))
                consumedSinceRefill = 0
              }
              break
            }
            case "complete":
              explicitlyCompleted = true
              terminal = true
              break
            case "completeExceptionally":
              throw new Error(event.value.error?.message ?? "Subscription query failed")
          }
        } catch (error) {
          closeSubscription(error instanceof Error ? error : new Error(String(error)))
          return
        }
        if (terminal) endOfStream()
      }
      const fail = (error: Error): void => {
        closeSubscription(error)
      }

      try {
        const opened = busStream(
          link,
          bus,
          FrameKind.OpenSubscriptionQuery,
          (w, busId) => {
            writeU16(w, busId)
            writeStr16(w, subscriptionId, "SubscriptionQuery.subscription_id")
            writeMessage(w, outgoing, queryInstructions, clientId, componentName)
            writeU64(w, BigInt(window))
          },
          false,
          sink,
          fail,
        )
        if (opened instanceof Promise) {
          opened.then((s) => {
            native = s
            if (subscriptionClosed) s.cancel()
          }, fail)
        } else {
          native = opened
        }
      } catch (error) {
        clearTimeout(initialTimer)
        removeShutdown()
        throw error
      }
      subscriptions.set(queryId, handler)

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

    subscribeToUpdates(unstamped: QueryMessage, bufferSize?: number): AsyncIterable<unknown> & { close(): void } {
      if (shutdownLatch.shuttingDown) throw new Error("Messaging shutdown in progress")
      if (subscriptions.size >= MAX_SUBSCRIPTIONS) throw new Error(`Subscription capacity ${MAX_SUBSCRIPTIONS} exhausted`)
      const queryId = unstamped.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      let removeShutdown: (() => void) | undefined
      const handler = updateHandler(unstamped, bufferSize, () => { subscriptions.delete(queryId); removeShutdown?.() })
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

    async emitUpdate(queryName: string, filter: SubscriptionFilter, update: unknown, uow?: UnitOfWork): Promise<void> {
      runAfterCommitOrImmediately(() => {
        let serialized: SerializedObject | undefined
        for (const [subscriptionId, sub] of provided) {
          if (sub.queryName !== queryName) continue
          if (!applySubscriptionFilter(filter, sub.payload)) continue
          serialized ??= serializer.serialize(update, queryName, "")
          offer(subscriptionId, sub, serialized)
        }
      }, uow)
    },

    async completeSubscription(queryName: string, filter?: SubscriptionFilter, uow?: UnitOfWork): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [subscriptionId, sub] of provided) {
          if (sub.queryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, sub.payload)) continue
          finish(subscriptionId, sub, { kind: "complete", value: { clientId, componentName } })
        }
      }, uow)
    },

    async completeSubscriptionExceptionally(queryName: string, error: Error, filter?: SubscriptionFilter, uow?: UnitOfWork): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [subscriptionId, sub] of provided) {
          if (sub.queryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, sub.payload)) continue
          finish(subscriptionId, sub, failureEvent(error.message))
        }
      }, uow)
    },
  }

  return routing
}

