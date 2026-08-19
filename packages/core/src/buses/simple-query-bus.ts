import type { QueryBus } from "./query-bus.js"
import { stamped, type QueryMessage, type Unstamped } from "../messages/message.js"
import type { SubscriptionQueryResult, UpdateHandler } from "./subscription-query.js"
import { updateHandler, runAfterCommitOrImmediately } from "./subscription-query.js"
import type { SubscriptionFilter } from "./subscription-filter.js"
import { applySubscriptionFilter } from "./subscription-filter.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
import { qualifiedNameToString } from "../primitives/qualified-name.js"

/**
 * Simple in-process query bus with subscription query support.
 *
 * A direct query opens a fresh UnitOfWork — carrying a transaction when the bus
 * was minted from a transactional `unitOfWork` factory — unless the caller handed one in, in which
 * case it NESTS (see `query` below). Subscription queries receive an initial
 * result plus a stream of incremental updates emitted via `emitUpdate()`.
 *
 * The factory is captured here, mirroring `simpleCommandBus`, so
 * the `query(bus, …)` verb needs nothing but the bus.
 *
 * Interceptor support is provided by wrapping with
 * {@link interceptingQueryBus}.
 */
export function simpleQueryBus(unitOfWork: () => UnitOfWork): QueryBus {
  const handlers = new Map<string, (message: QueryMessage, uow: UnitOfWork) => Promise<unknown>>()

  // Active subscription query handlers, keyed by query identifier
  const subscriptions = new Map<string, UpdateHandler>()

  const bus: QueryBus = {
    async query(message: Unstamped<QueryMessage>, uow?: UnitOfWork): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for query "${key}"`)
      }

      // Mirrors simple-command-bus.dispatch, except that a query NESTS. The
      // decision is made on the HANDLE, not on any runner: `ctx.query` passes
      // the calling handler's unit of work straight through this parameter, and
      // a live one is reused so the consulting read shares the caller's
      // transaction. A primary dispatch passes none and we open a fresh one.
      //
      // Either way the unit of work that will handle the query is also the one
      // whose clock stamps it — a nested read is stamped by the task it joins.
      if (uow !== undefined && !uow.closed) {
        return handler(stamped(message, () => uow.now()), uow)
      }
      return unitOfWork().execute((u) => handler(stamped(message, () => u.now()), u))
    },

    subscribe(
      queryName: string,
      handler: (message: QueryMessage, uow: UnitOfWork) => Promise<unknown>,
    ) {
      const existing = handlers.get(queryName)
      if (existing && existing !== handler) {
        throw new Error(
          `A different handler is already registered for query "${queryName}". ` +
          `Duplicate query handler subscriptions are not allowed.`,
        )
      }
      handlers.set(queryName, handler)
    },

    subscriptionQuery(
      unstamped: Unstamped<QueryMessage>,
      bufferSize?: number,
    ): SubscriptionQueryResult {
      // A subscription is REGISTERED under the message, then answered by
      // `bus.query` below — so it has to be stamped before it is filed, not
      // when it is answered. There is no task here to borrow an instant from,
      // so one unit of work is minted for its clock alone; the query that
      // follows opens its own.
      const message = stamped(unstamped, () => unitOfWork().now())
      const queryId = message.identifier

      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const handler = updateHandler(message, bufferSize)
      subscriptions.set(queryId, handler)

      const initialResult = bus.query(message)

      return {
        initialResult,
        updates: handler.iterable,
        close: () => {
          subscriptions.delete(queryId)
          handler.complete()
        },
      }
    },

    subscribeToUpdates(
      unstamped: Unstamped<QueryMessage>,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      const message = stamped(unstamped, () => unitOfWork().now())
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
      uow?: UnitOfWork,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
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
      }, uow)
    },

    async completeSubscription(
      queryName: string,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, handler.query.payload)) continue

          handler.complete()
          subscriptions.delete(id)
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
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !applySubscriptionFilter(filter, handler.query.payload)) continue

          handler.completeExceptionally(error)
          subscriptions.delete(id)
        }
      }, uow)
    },
  }

  return bus
}
