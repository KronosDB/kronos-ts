import type {
  QueryBus,
  QueryMessage,
  SubscriptionFilter,
  SubscriptionQueryResult,
  UpdateHandler,
} from "@kronos-ts/messaging"
import {
  applySubscriptionFilter,
  createUpdateHandler,
  extractStructuredFilter,
  matchesPayloadEquals,
  runAfterCommitOrImmediately,
  runInNewUoW,
} from "@kronos-ts/messaging"
import { qualifiedNameToString } from "@kronos-ts/common"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"
import type {
  RabbitMqQueryUpdateEnvelope,
  RabbitMqQueryUpdatesTransport,
} from "./amqp-query-updates-transport.js"

export interface RabbitMqQueryEnvelope {
  readonly kind: "query"
  readonly requestId: string
  readonly message: QueryMessage
  readonly timeoutMs: number
}

export interface RabbitMqQueryReplyEnvelope {
  readonly requestId: string
  readonly ok: boolean
  readonly result?: unknown
  readonly error?: {
    readonly name?: string
    readonly message: string
    readonly stack?: string
  }
}

export interface RabbitMqQueryTransport {
  dispatch(envelope: RabbitMqQueryEnvelope): Promise<RabbitMqQueryReplyEnvelope>
  subscribe(
    queryName: string,
    handler: (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>,
  ): void | Promise<void>
}

export interface RabbitMqQueryBusOptions {
  readonly localSegment: QueryBus
  readonly transport: RabbitMqQueryTransport
  readonly updatesTransport?: RabbitMqQueryUpdatesTransport
  readonly config: RabbitMqResolvedConfig
}

/**
 * Distributed query bus decorator.
 *
 * Direct request/reply queries (`query` + `subscribe`) route over the
 * request/reply transport.
 *
 * Subscription queries combine two patterns:
 *
 *   1. **Initial result** travels over the existing request/reply transport
 *      via `bus.query`. Exactly one handler instance answers (competing
 *      consumer semantics).
 *   2. **Updates** are broadcast over a topic exchange via the updates
 *      transport. Every instance with active subscribers for the query name
 *      binds to the routing key and receives every emit, then routes to its
 *      local subscribers.
 *
 * Filter handling — function filters are local-only (cannot serialize JS
 * functions); structured `payloadEquals` filters serialize into the broadcast
 * envelope and apply on every receiver against each subscriber's stored
 * query payload. See {@link SubscriptionFilter}.
 *
 * Falls back to local-only behaviour when no `updatesTransport` is supplied.
 */
export function createRabbitMqQueryBus(options: RabbitMqQueryBusOptions): QueryBus {
  const localHandlers = new Set<string>()
  const { localSegment, transport, updatesTransport, config } = options

  // Subscriptions registered on THIS instance. Mirrors the simple-query-bus
  // local segment; the local segment is still used for the initial-result
  // dispatch, but we keep our own map so we can apply incoming broadcast
  // updates without re-entering the local bus.
  const subscriptions = new Map<string, UpdateHandler>()
  const queryNameRefcount = new Map<string, number>()

  function incrementBinding(queryName: string): boolean {
    const next = (queryNameRefcount.get(queryName) ?? 0) + 1
    queryNameRefcount.set(queryName, next)
    return next === 1
  }

  function decrementBinding(queryName: string): boolean {
    const current = queryNameRefcount.get(queryName) ?? 0
    if (current <= 1) {
      queryNameRefcount.delete(queryName)
      return true
    }
    queryNameRefcount.set(queryName, current - 1)
    return false
  }

  function deliverToLocalSubs(
    queryName: string,
    update: unknown,
    filterEquals: Record<string, unknown> | undefined,
  ): void {
    for (const [id, handler] of subscriptions) {
      if (!handler.active) {
        subscriptions.delete(id)
        continue
      }
      const handlerQueryName = qualifiedNameToString(handler.query.name)
      if (handlerQueryName !== queryName) continue
      if (filterEquals && !matchesPayloadEquals(handler.query.payload, filterEquals)) continue

      const accepted = handler.offer(update)
      if (!accepted) {
        handler.completeExceptionally(new Error("Subscription query update buffer overflow"))
        subscriptions.delete(id)
      }
    }
  }

  function completeLocalSubs(
    queryName: string,
    filterEquals: Record<string, unknown> | undefined,
    error?: Error,
  ): void {
    for (const [id, handler] of subscriptions) {
      const handlerQueryName = qualifiedNameToString(handler.query.name)
      if (handlerQueryName !== queryName) continue
      if (filterEquals && !matchesPayloadEquals(handler.query.payload, filterEquals)) continue

      if (error) handler.completeExceptionally(error)
      else handler.complete()
      subscriptions.delete(id)
    }
  }

  if (updatesTransport) {
    updatesTransport.setHandler((envelope) => {
      // Loopback dedup — local fan-out already happened on this instance.
      if (envelope.senderId === updatesTransport.senderId) return

      if (envelope.kind === "update") {
        deliverToLocalSubs(envelope.queryName, envelope.update, envelope.payloadEquals)
      } else if (envelope.kind === "complete") {
        completeLocalSubs(envelope.queryName, envelope.payloadEquals)
      } else if (envelope.kind === "completeExceptionally") {
        const remoteError = envelope.error
          ? Object.assign(new Error(envelope.error.message), {
              name: envelope.error.name ?? "RemoteSubscriptionError",
            })
          : new Error("Remote subscription failed")
        completeLocalSubs(envelope.queryName, envelope.payloadEquals, remoteError)
      }
    })
  }

  function registerSubscription(
    message: QueryMessage,
    bufferSize?: number,
  ): UpdateHandler & { iterable: AsyncIterable<unknown> } {
    const queryId = message.identifier
    if (subscriptions.has(queryId)) {
      throw new Error(`Subscription query already registered for identifier "${queryId}"`)
    }
    const handler = createUpdateHandler(message, bufferSize)
    subscriptions.set(queryId, handler)

    const queryName = qualifiedNameToString(message.name)
    if (incrementBinding(queryName) && updatesTransport) {
      void updatesTransport.bindQueryName(queryName).catch(() => {})
    }
    return handler
  }

  function unregisterSubscription(message: QueryMessage): void {
    const queryId = message.identifier
    const existing = subscriptions.get(queryId)
    if (!existing) return
    subscriptions.delete(queryId)
    existing.complete()
    const queryName = qualifiedNameToString(message.name)
    if (decrementBinding(queryName) && updatesTransport) {
      void updatesTransport.unbindQueryName(queryName).catch(() => {})
    }
  }

  const bus: QueryBus = {
    async query(message: QueryMessage): Promise<unknown> {
      const queryName = qualifiedNameToString(message.name)
      const preferLocal = config.queries.preferLocalHandlers && !config.queries.alwaysUseDistributedBus
      if (preferLocal && localHandlers.has(queryName)) {
        return localSegment.query(message)
      }

      const envelope: RabbitMqQueryEnvelope = {
        kind: "query",
        requestId: message.identifier,
        message,
        timeoutMs: config.queries.defaultTimeoutMs,
      }
      const reply = await transport.dispatch(envelope)
      if (!reply.ok) throw deserializeRemoteError(reply.error)
      return reply.result
    },

    subscribe(queryName: string, handler: (message: QueryMessage) => Promise<unknown>): void {
      localHandlers.add(queryName)
      localSegment.subscribe(queryName, handler)
      void transport.subscribe(queryName, async (envelope) => {
        try {
          // AF5 parity: an inbound distributed query is handled in its own
          // fresh UnitOfWork. Correlation/causation lineage rides on the query
          // message metadata, which crosses the wire intact.
          const result = await runInNewUoW(envelope.message.metadata, () =>
            handler(envelope.message),
          )
          return { requestId: envelope.requestId, ok: true, result }
        } catch (error) {
          return { requestId: envelope.requestId, ok: false, error: serializeError(error) }
        }
      })
    },

    subscriptionQuery(message: QueryMessage, bufferSize?: number): SubscriptionQueryResult {
      const updateHandler = registerSubscription(message, bufferSize)
      const initialResult = bus.query(message)
      return {
        initialResult,
        updates: updateHandler.iterable,
        close: () => unregisterSubscription(message),
      }
    },

    subscribeToUpdates(
      message: QueryMessage,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      const updateHandler = registerSubscription(message, bufferSize)
      return {
        [Symbol.asyncIterator]: () => updateHandler.iterable[Symbol.asyncIterator](),
        close: () => unregisterSubscription(message),
      }
    },

    async emitUpdate(
      queryName: string,
      filter: SubscriptionFilter,
      update: unknown,
    ): Promise<void> {
      const structured = extractStructuredFilter(filter)
      const filterEquals = structured?.payloadEquals as Record<string, unknown> | undefined

      runAfterCommitOrImmediately(() => {
        // Local fan-out — applies the full (possibly function) filter against
        // local subscribers.
        for (const [id, handler] of subscriptions) {
          if (!handler.active) {
            subscriptions.delete(id)
            continue
          }
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (!applySubscriptionFilter(filter, handler.query.payload)) continue

          const accepted = handler.offer(update)
          if (!accepted) {
            handler.completeExceptionally(
              new Error("Subscription query update buffer overflow"),
            )
            subscriptions.delete(id)
          }
        }

        // Distributed broadcast. We send the structured predicate when present
        // so remote receivers can re-apply it; function filters cannot cross
        // the wire and degrade to "deliver to all remote subscribers of this
        // query name" — discouraged for new code, see SubscriptionFilter docs.
        if (updatesTransport) {
          const envelope: RabbitMqQueryUpdateEnvelope = filterEquals
            ? {
                kind: "update",
                senderId: updatesTransport.senderId,
                queryName,
                update,
                payloadEquals: filterEquals,
              }
            : {
                kind: "update",
                senderId: updatesTransport.senderId,
                queryName,
                update,
              }
          void updatesTransport.publish(envelope).catch(() => {})
        }
      })
    },

    async completeSubscription(
      queryName: string,
      filter?: SubscriptionFilter,
    ): Promise<void> {
      const structured = filter ? extractStructuredFilter(filter) : undefined
      const filterEquals = structured?.payloadEquals as Record<string, unknown> | undefined

      runAfterCommitOrImmediately(() => {
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, handler.query.payload)) continue
          handler.complete()
          subscriptions.delete(id)
        }

        if (updatesTransport) {
          void updatesTransport
            .publish(
              filterEquals
                ? {
                    kind: "complete",
                    senderId: updatesTransport.senderId,
                    queryName,
                    payloadEquals: filterEquals,
                  }
                : {
                    kind: "complete",
                    senderId: updatesTransport.senderId,
                    queryName,
                  },
            )
            .catch(() => {})
        }
      })
    },

    async completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: SubscriptionFilter,
    ): Promise<void> {
      const structured = filter ? extractStructuredFilter(filter) : undefined
      const filterEquals = structured?.payloadEquals as Record<string, unknown> | undefined

      runAfterCommitOrImmediately(() => {
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, handler.query.payload)) continue
          handler.completeExceptionally(error)
          subscriptions.delete(id)
        }

        if (updatesTransport) {
          void updatesTransport
            .publish(
              filterEquals
                ? {
                    kind: "completeExceptionally",
                    senderId: updatesTransport.senderId,
                    queryName,
                    error: serializeError(error),
                    payloadEquals: filterEquals,
                  }
                : {
                    kind: "completeExceptionally",
                    senderId: updatesTransport.senderId,
                    queryName,
                    error: serializeError(error),
                  },
            )
            .catch(() => {})
        }
      })
    },
  }

  return bus
}

function serializeError(error: unknown): RabbitMqQueryReplyEnvelope["error"] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack }
  }
  return { message: String(error) }
}

function deserializeRemoteError(error: RabbitMqQueryReplyEnvelope["error"]): Error {
  const result = new Error(error?.message ?? "Remote query handling failed")
  result.name = error?.name ?? "RemoteQueryHandlingError"
  if (error?.stack) result.stack = error.stack
  return result
}
