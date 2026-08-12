import type {
  QueryBus,
  QueryMessage,
  SubscriptionFilter,
  SubscriptionQueryResult,
  UpdateHandler,
} from "@kronos-ts/messaging"
import {
  applySubscriptionFilter,
  correlationDataDispatchInterceptor,
  interceptingQueryBus,
  updateHandler,
  runAfterCommitOrImmediately,
  runInNewUoW,
} from "@kronos-ts/messaging"
import { qualifiedNameToString } from "@kronos-ts/common"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"
import type {
  DeliverEnvelope,
  DistributedSubscriberRegistry,
} from "./distributed-subscriber-registry.js"

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
  readonly subscriberRegistry?: DistributedSubscriberRegistry
  readonly config: RabbitMqResolvedConfig
}

/**
 * Distributed query bus decorator.
 *
 * Direct request/reply queries (`query` + `subscribe`) route over the
 * request/reply transport.
 *
 * Subscription queries use a distributed-mirror model. Every subscribe
 * publishes a `claim` over the gossip fanout exchange so every instance
 * learns about it; every unsubscribe publishes a `release`. Each instance
 * keeps a cluster-wide `Map<subId, SubscriberRecord>` mirror.
 *
 * `emitUpdate` walks the mirror locally (it holds every cluster-wide
 * subscriber's payload), applies the filter — function predicates work
 * because evaluation happens colocated with the payload, not over the wire —
 * and routes per-subscriber delivery via the registry. Local subs are
 * dispatched in-process; remote subs receive a `DeliverEnvelope` on the
 * owner's direct queue.
 *
 * The same model handles `completeSubscription` and
 * `completeSubscriptionExceptionally`.
 *
 * Falls back to local-only behaviour when no `subscriberRegistry` is supplied.
 *
 * ## Correlation lineage
 *
 * Like the command bus, the returned object is wrapped in
 * {@link interceptingQueryBus} carrying {@link correlationDataDispatchInterceptor},
 * so lineage is stamped BEFORE the local-vs-remote decision in `query()` — see
 * the long note in `command-bus.ts` for the AxonFramework precedent. AF applies
 * the same treatment to queries: `AxonServerQueryBus` calls
 * `dispatchInterceptors.intercept(...)` at the top of `query`, `scatterGather`,
 * `streamingQuery` AND `subscriptionQuery`.
 *
 * `subscriptionQuery` here gets it for the INITIAL RESULT only, because that
 * goes back through `bus.query`. The subscription registration itself carries
 * `message.metadata` untouched: `interceptingQueryBus` (in `@kronos-ts/messaging`)
 * passes `subscriptionQuery` straight to the delegate without running the
 * dispatch chain, so there is no seam to hook from here. That is a messaging-side
 * gap, not a RabbitMQ one — the same hole exists on every backend.
 */
export function rabbitMqQueryBus(options: RabbitMqQueryBusOptions): QueryBus {
  const localHandlers = new Set<string>()
  const { localSegment, transport, subscriberRegistry, config } = options

  // UpdateHandlers for subs owned BY this instance, keyed by subId.
  const localOwnedHandlers = new Map<string, UpdateHandler>()

  function applyDelivery(envelope: DeliverEnvelope): void {
    const handler = localOwnedHandlers.get(envelope.subId)
    if (!handler) return

    if (envelope.kind === "update") {
      if (!handler.active) {
        localOwnedHandlers.delete(envelope.subId)
        return
      }
      const accepted = handler.offer(envelope.update)
      if (!accepted) {
        handler.completeExceptionally(new Error("Subscription query update buffer overflow"))
        localOwnedHandlers.delete(envelope.subId)
      }
    } else if (envelope.kind === "complete") {
      handler.complete()
      localOwnedHandlers.delete(envelope.subId)
    } else if (envelope.kind === "completeExceptionally") {
      const error = Object.assign(new Error(envelope.error.message), {
        name: envelope.error.name ?? "RemoteSubscriptionError",
      })
      handler.completeExceptionally(error)
      localOwnedHandlers.delete(envelope.subId)
    }
  }

  if (subscriberRegistry) {
    subscriberRegistry.setDeliverHandler(applyDelivery)
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

    if (subscriberRegistry) {
      void subscriberRegistry
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
    if (subscriberRegistry) {
      void subscriberRegistry.release(subId).catch(() => {})
    }
  }

  const routing: QueryBus = {
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
      const handler = registerSubscription(message, bufferSize)
      // Through the WRAPPED bus, so the initial result carries lineage.
      const initialResult = bus.query(message)
      return {
        initialResult,
        updates: handler.iterable,
        close: () => unregisterSubscription(message),
      }
    },

    subscribeToUpdates(
      message: QueryMessage,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
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
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        if (subscriberRegistry) {
          for (const record of subscriberRegistry.records()) {
            if (record.queryName !== queryName) continue
            if (!applySubscriptionFilter(filter, record.payload)) continue
            void subscriberRegistry
              .deliver({ kind: "update", subId: record.subId, update })
              .catch(() => {})
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
            handler.completeExceptionally(
              new Error("Subscription query update buffer overflow"),
            )
            localOwnedHandlers.delete(id)
          }
        }
      })
    },

    async completeSubscription(
      queryName: string,
      filter?: SubscriptionFilter,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        if (subscriberRegistry) {
          for (const record of subscriberRegistry.records()) {
            if (record.queryName !== queryName) continue
            if (filter && !applySubscriptionFilter(filter, record.payload)) continue
            void subscriberRegistry
              .deliver({ kind: "complete", subId: record.subId })
              .catch(() => {})
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
      })
    },

    async completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: SubscriptionFilter,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        if (subscriberRegistry) {
          const serialized = serializeError(error) ?? { message: String(error) }
          for (const record of subscriberRegistry.records()) {
            if (record.queryName !== queryName) continue
            if (filter && !applySubscriptionFilter(filter, record.payload)) continue
            void subscriberRegistry
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
      })
    },
  }

  const bus = interceptingQueryBus(routing)
  bus.registerDispatchInterceptor(correlationDataDispatchInterceptor())
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
