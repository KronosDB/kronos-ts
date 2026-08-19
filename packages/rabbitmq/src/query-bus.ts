import {
  applySubscriptionFilter,
  qualifiedNameToString,
  runAfterCommitOrImmediately,
  stamped,
  updateHandler,
  type QueryBus,
  type QueryMessage,
  type SubscriptionFilter,
  type SubscriptionQueryResult,
  type UnitOfWork,
  type Unstamped,
  type UpdateHandler,
} from "@kronos-ts/core"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"
import type {
  DistributedSubscriberRegistry,
  SubscriptionDelivery,
} from "./distributed-subscriber-registry.js"
import type { RabbitMqBusOptions } from "./command-bus.js"

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

const DEFAULT_TIMEOUT_MS = 30_000

/**
 * What the bus borrows from the connection. `subscriberRegistry` is optional
 * because a connection without one leaves subscription queries local-only
 * rather than failing.
 */
export interface RabbitMqQueryBusSource {
  readonly config: RabbitMqResolvedConfig
  readonly queryTransport: RabbitMqQueryTransport
  readonly subscriberRegistry?: DistributedSubscriberRegistry
}

/**
 * A RabbitMQ-backed query bus over YOUR local segment.
 *
 * Direct request/reply queries fork exactly as {@link rabbitMqCommandBus} does:
 * a query this instance subscribed goes to `local`, anything else over the
 * broker — durable per-query queues with competing consumers, an identity-named
 * exclusive reply queue, correlation-id matched replies.
 *
 * ## Subscription queries
 *
 * Distributed-mirror model over the connection's
 * {@link DistributedSubscriberRegistry}. Every `subscriptionQuery` claims its
 * subscriber on the registry so every instance learns about it; every close
 * releases it.
 *
 * `emitUpdate` then walks the mirror LOCALLY — it holds every cluster-wide
 * subscriber's payload — applies the filter, and routes per-subscriber delivery
 * through the registry. Evaluating the filter colocated with the payload is
 * what lets a plain function predicate work across instances; the predicate
 * never crosses the wire. `completeSubscription` and
 * `completeSubscriptionExceptionally` follow the same path.
 *
 * Without a registry the bus degrades to local-only subscription queries rather
 * than failing.
 *
 * ## Where the interceptors go
 *
 * Outside, as on the command side —
 * `interceptingQueryBus(rabbitMqQueryBus(rabbit, local), lineage)`.
 *
 * KNOWN GAP, carried over unchanged: `interceptingQueryBus` forwards
 * `subscriptionQuery` / `subscribeToUpdates` to its delegate without running
 * the dispatch chain, so an outer wrapper's transforms never reach them. The
 * initial result below takes the same local-vs-remote fork as a plain `query`,
 * but it enters at `bus.query` — INSIDE any outer wrapper. Closing that needs a
 * seam in `interceptingQueryBus`, not here; the same hole exists on every
 * backend.
 */
export function rabbitMqQueryBus(
  rabbit: RabbitMqQueryBusSource,
  local: QueryBus,
  options: RabbitMqBusOptions = {},
): QueryBus {
  const transport = rabbit.queryTransport
  const registry = rabbit.subscriberRegistry
  const localHandlers = new Set<string>()
  const preferLocal = options.preferLocal ?? true
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS

  // UpdateHandlers for subs owned BY this instance, keyed by subId.
  const localOwnedHandlers = new Map<string, UpdateHandler>()

  function applyDelivery(delivery: SubscriptionDelivery): void {
    const handler = localOwnedHandlers.get(delivery.subId)
    if (!handler) return

    if (delivery.kind === "update") {
      if (!handler.active) {
        localOwnedHandlers.delete(delivery.subId)
        return
      }
      const accepted = handler.offer(delivery.update)
      if (!accepted) {
        handler.completeExceptionally(new Error("Subscription query update buffer overflow"))
        localOwnedHandlers.delete(delivery.subId)
      }
    } else if (delivery.kind === "complete") {
      handler.complete()
      localOwnedHandlers.delete(delivery.subId)
    } else if (delivery.kind === "completeExceptionally") {
      const error = Object.assign(new Error(delivery.error.message), {
        name: delivery.error.name ?? "RemoteSubscriptionError",
      })
      handler.completeExceptionally(error)
      localOwnedHandlers.delete(delivery.subId)
    }
  }

  if (registry) {
    registry.setDeliverHandler(applyDelivery)
  }

  function registerSubscription(
    message: QueryMessage,
    bufferSize?: number,
  ): UpdateHandler & { iterable: AsyncIterable<unknown> } {
    const subId = message.identifier
    if (localOwnedHandlers.has(subId)) {
      throw new Error(`Subscription query already registered for identifier "${subId}"`)
    }
    const handler = updateHandler(message, bufferSize)
    localOwnedHandlers.set(subId, handler)

    if (registry) {
      void registry
        .claim({
          subId,
          queryName: qualifiedNameToString(message.name),
          payload: message.payload,
        })
        .catch(() => {})
    }
    return handler
  }

  function unregisterSubscription(message: QueryMessage): void {
    const subId = message.identifier
    const existing = localOwnedHandlers.get(subId)
    if (!existing) return
    localOwnedHandlers.delete(subId)
    existing.complete()
    if (registry) {
      void registry.release(subId).catch(() => {})
    }
  }

  const bus: QueryBus = {
    async query(unstamped: Unstamped<QueryMessage>, uow?: UnitOfWork): Promise<unknown> {
      const queryName = qualifiedNameToString(unstamped.name)
      if (preferLocal && localHandlers.has(queryName)) {
        // Hand the unit of work through so a `ctx.query` that prefers a local
        // handler still nests in the caller's UoW, as the in-process bus does.
        return local.query(unstamped, uow)
      }

      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire still {@link Unstamped} is therefore
      // stamped from system time here — the envelope crosses a process boundary
      // and must be fully formed. A locally-shortcut message is handed to
      // `local` unstamped instead, so the task that handles it supplies the
      // instant.
      const message = stamped(unstamped, Date.now)

      const reply = await transport.dispatch({
        kind: "query",
        requestId: message.identifier,
        message,
        timeoutMs,
      })
      if (!reply.ok) throw deserializeRemoteError(reply.error)
      return reply.result
    },

    subscribe(queryName, handler): void {
      localHandlers.add(queryName)
      local.subscribe(queryName, handler)

      // As on the command side: a handling failure is an `ok: false` reply on
      // an ACKed message, not a nack. Inbound work runs through `local`, so it
      // inherits whatever unit-of-work policy the local segment was built with.
      void transport.subscribe(queryName, async (envelope) => {
        try {
          const result = await local.query(envelope.message)
          return { requestId: envelope.requestId, ok: true, result }
        } catch (error) {
          return { requestId: envelope.requestId, ok: false, error: serializeError(error) }
        }
      })
    },

    subscriptionQuery(
      unstamped: Unstamped<QueryMessage>,
      bufferSize?: number,
    ): SubscriptionQueryResult {
      const message = stamped(unstamped, Date.now)
      const handler = registerSubscription(message, bufferSize)
      // Through `bus.query`, so the initial result takes the same routing fork
      // a plain query does.
      const initialResult = bus.query(message)
      return {
        initialResult,
        updates: handler.iterable,
        close: () => unregisterSubscription(message),
      }
    },

    subscribeToUpdates(
      unstamped: Unstamped<QueryMessage>,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      const message = stamped(unstamped, Date.now)
      const handler = registerSubscription(message, bufferSize)
      return {
        [Symbol.asyncIterator]: () => handler.iterable[Symbol.asyncIterator](),
        close: () => unregisterSubscription(message),
      }
    },

    async emitUpdate(
      queryName: string,
      filter: SubscriptionFilter,
      update: unknown,
      uow?: UnitOfWork,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        if (registry) {
          for (const record of registry.records()) {
            if (record.queryName !== queryName) continue
            if (!applySubscriptionFilter(filter, record.payload)) continue
            void registry.deliver({ kind: "update", subId: record.subId, update }).catch(() => {})
          }
          return
        }

        // Local-only mode: filter and offer against the local subscriber set.
        for (const [id, handler] of localOwnedHandlers) {
          if (!handler.active) {
            localOwnedHandlers.delete(id)
            continue
          }
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (!applySubscriptionFilter(filter, handler.query.payload)) continue

          const accepted = handler.offer(update)
          if (!accepted) {
            handler.completeExceptionally(new Error("Subscription query update buffer overflow"))
            localOwnedHandlers.delete(id)
          }
        }
      }, uow)
    },

    async completeSubscription(
      queryName: string,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        if (registry) {
          for (const record of registry.records()) {
            if (record.queryName !== queryName) continue
            if (filter && !applySubscriptionFilter(filter, record.payload)) continue
            void registry.deliver({ kind: "complete", subId: record.subId }).catch(() => {})
          }
          return
        }

        for (const [id, handler] of localOwnedHandlers) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, handler.query.payload)) continue
          handler.complete()
          localOwnedHandlers.delete(id)
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
        if (registry) {
          const serialized = { name: error.name, message: error.message, stack: error.stack }
          for (const record of registry.records()) {
            if (record.queryName !== queryName) continue
            if (filter && !applySubscriptionFilter(filter, record.payload)) continue
            void registry
              .deliver({
                kind: "completeExceptionally",
                subId: record.subId,
                error: serialized,
              })
              .catch(() => {})
          }
          return
        }

        for (const [id, handler] of localOwnedHandlers) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, handler.query.payload)) continue
          handler.completeExceptionally(error)
          localOwnedHandlers.delete(id)
        }
      }, uow)
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
