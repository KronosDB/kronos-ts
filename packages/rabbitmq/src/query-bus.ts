import {
  applySubscriptionFilter,
  qualifiedNameToString,
  runAfterCommitOrImmediately,
  updateHandler,
  subscriptionInitialResult,
  type QueryBus,
  type SubscriptionCapableQueryBus,
  type QueryMessage,
  type SubscriptionFilter,
  type SubscriptionQueryResult,
  type UnitOfWork,
  type UpdateHandler,
} from "@kronos-ts/core"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"
import type {
  DistributedSubscriberRegistry,
  SubscriptionDelivery,
} from "./distributed-subscriber-registry.js"
import type { RabbitMqBusOptions } from "./command-bus.js"

export type RabbitMqQueryEnvelope = {
  readonly kind: "query"
  readonly requestId: string
  readonly message: QueryMessage
  readonly timeoutMs: number
}

export type RabbitMqQueryReplyEnvelope = {
  readonly requestId: string
  readonly ok: boolean
  readonly result?: unknown
  readonly error?: {
    readonly name?: string
    readonly message: string
    readonly stack?: string
  }
}

export type RabbitMqQueryTransport = {
  dispatch(envelope: RabbitMqQueryEnvelope): Promise<RabbitMqQueryReplyEnvelope>
  subscribe(
    queryName: string,
    handler: (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>,
  ): void | Promise<void>
}

const DEFAULT_TIMEOUT_MS = 30_000

/**
 * What the bus borrows from the connection. `subscriberRegistry` is optional
 * because a connection without one leaves subscription queries next-only
 * rather than failing.
 */
export type RabbitMqQueryBusSource = {
  readonly config: RabbitMqResolvedConfig
  readonly queryTransport: RabbitMqQueryTransport
  readonly subscriberRegistry?: DistributedSubscriberRegistry
}

/**
 * A RabbitMQ-backed query bus over YOUR next segment.
 *
 * Direct request/reply queries fork exactly as {@link rabbitMqCommandBus} does:
 * a query this instance subscribed goes to `next`, anything else over the
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
 * Without a registry the bus degrades to next-only subscription queries rather
 * than failing.
 *
 * ## Where the interceptors go
 *
 * Outside, as on the command side —
 * `interceptingQueryBus(rabbitMqQueryBus(next, rabbit), correlation)`.
 *
 * The outer interceptor transforms subscription queries and update-only
 * registrations too, before either reaches this bus.
 */
export function rabbitMqQueryBus<U extends UnitOfWork = UnitOfWork>(
  next: QueryBus<U>,
  rabbit: RabbitMqQueryBusSource,
  options: RabbitMqBusOptions = {},
): SubscriptionCapableQueryBus<U> {
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
        unregisterSubscription(handler.query)
        return
      }
      const accepted = handler.offer(delivery.update)
      if (!accepted) {
        handler.completeExceptionally(new Error("Subscription query update buffer overflow"))
        unregisterSubscription(handler.query)
      }
    } else if (delivery.kind === "complete") {
      handler.complete()
      unregisterSubscription(handler.query)
    } else if (delivery.kind === "completeExceptionally") {
      const error = Object.assign(new Error(delivery.error.message), {
        name: delivery.error.name ?? "RemoteSubscriptionError",
      })
      handler.completeExceptionally(error)
      unregisterSubscription(handler.query)
    }
  }

  if (registry) {
    registry.setDeliverHandler(applyDelivery)
  }

  function registerSubscription(
    message: QueryMessage,
    bufferSize?: number,
    onClose?: () => void,
  ): UpdateHandler & { iterable: AsyncIterable<unknown> } {
    const subId = message.identifier
    if (localOwnedHandlers.has(subId)) {
      throw new Error(`Subscription query already registered for identifier "${subId}"`)
    }
    if (localOwnedHandlers.size >= 1024) throw new Error("Subscription capacity 1024 exhausted")
    const handler = updateHandler(message, bufferSize, () => { unregisterSubscription(message); onClose?.() })
    localOwnedHandlers.set(subId, handler)

    if (registry) {
      void registry
        .claim({
          subId,
          queryName: qualifiedNameToString(message.name),
          payload: message.payload,
        })
        .catch((error) => handler.completeExceptionally(error instanceof Error ? error : new Error(String(error))))
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

  const bus: SubscriptionCapableQueryBus<U> = {
    async query(unstamped: QueryMessage): Promise<unknown> {
      const queryName = qualifiedNameToString(unstamped.name)
      if (preferLocal && localHandlers.has(queryName)) {
        // A co-located handler answers on a task of its own, exactly as a
        // remote one would — `next` mints it.
        return next.query(unstamped)
      }

      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire with no instant yet gets one from system
      // time here — the envelope crosses a process boundary and must be fully
      // formed. A locally-shortcut message is handed to `next` untouched
      // instead, so the task that handles it supplies the instant.
      const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }

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
      next.subscribe(queryName, handler)

      // As on the command side: a handling failure is an `ok: false` reply on
      // an ACKed message, not a nack. Inbound work runs through `next`, so it
      // inherits whatever unit-of-work policy the next segment was built with.
      void transport.subscribe(queryName, async (envelope) => {
        try {
          const result = await next.query(envelope.message)
          return { requestId: envelope.requestId, ok: true, result }
        } catch (error) {
          return { requestId: envelope.requestId, ok: false, error: serializeError(error) }
        }
      })
    },

    subscriptionQuery(
      unstamped: QueryMessage,
      bufferSize?: number,
    ): SubscriptionQueryResult {
      const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }
      let closeInitial = () => {}
      const handler = registerSubscription(message, bufferSize, () => closeInitial())
      // Through `bus.query`, so the initial result takes the same routing fork
      // a plain query does.
      const initial = subscriptionInitialResult(bus.query(message), (error) => handler.completeExceptionally(error))
      closeInitial = initial.close
      return {
        initialResult: initial.initialResult,
        updates: handler.iterable,
        close: () => unregisterSubscription(message),
      }
    },

    subscribeToUpdates(
      unstamped: QueryMessage,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }
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

        // Local-only mode: filter and offer against the next subscriber set.
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
