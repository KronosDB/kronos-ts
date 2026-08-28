import type { QueryBus, SubscriptionCapableQueryBus } from "./bus.js"
import {
  withInstant,
  type QueryMessage,
  qualifiedNameToString,
} from "../messaging/messages.js"
import {
  type SubscriptionQueryResult,
  type UpdateHandler,
  updateHandler,
  runAfterCommitOrImmediately,
} from "./subscription-query.js"
import { type SubscriptionFilter, applySubscriptionFilter } from "./subscription-filter.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
/**
 * Simple in-process query bus with subscription query support.
 *
 * A direct query opens a fresh UnitOfWork — carrying a transaction when the bus
 * was minted from a transactional `unitOfWork` factory — unless the caller handed one in, in which
 * case it NESTS (see `query` below). Subscription queries receive an initial
 * result plus a stream of incremental updates emitted via `emitUpdate()`.
 *
 * The factory is captured here, mirroring `localCommandBus`, so
 * the `query(bus, …)` verb needs nothing but the bus.
 *
 * Interceptor support is provided by wrapping with
 * {@link interceptingQueryBus}.
 */
export function localQueryBus<U extends UnitOfWork = UnitOfWork>(
  unitOfWork: () => U,
): SubscriptionCapableQueryBus<U> {
  const handlers = new Map<string, (message: QueryMessage, uow: U) => Promise<unknown>>()

  // Active subscription query handlers, keyed by query identifier
  const subscriptions = new Map<string, UpdateHandler>()

  const bus: SubscriptionCapableQueryBus<U> = {
    async query(message: QueryMessage, uow?: UnitOfWork): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for query "${key}"`)
      }

      // Mirrors local-command-bus.dispatch, except that a query NESTS. The
      // decision is made on the HANDLE, not on any runner: `ctx.query` passes
      // the calling handler's unit of work straight through this parameter, and
      // a live one is reused so the consulting read shares the caller's
      // transaction. A primary dispatch passes none and we open a fresh one.
      //
      // Either way the unit of work that will handle the query is also the one
      // whose clock stamps it — a nested read is stamped by the task it joins.
      //
      // The nested handle arrives typed as the bare `UnitOfWork` — `query` is
      // the one seam a FOREIGN task can enter through, so its parameter cannot
      // promise `U`. It is one in practice: an entry names one command bus and
      // one query bus, and the entry types tie both to the same `U`, so the
      // task that nests here was minted by a factory of that same shape.
      if (uow !== undefined && !uow.closed) {
        return handler(withInstant(message, () => uow.now()), uow as U)
      }
      // A primary query mints its own, and the MINTED handle is what the
      // handler gets — see the note in `localCommandBus`.
      const opened = unitOfWork()
      return opened.execute(() => handler(withInstant(message, () => opened.now()), opened))
    },

    subscribe(
      queryName: string,
      handler: (message: QueryMessage, uow: U) => Promise<unknown>,
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
      unstamped: QueryMessage,
      bufferSize?: number,
    ): SubscriptionQueryResult {
      // A subscription is REGISTERED under the message, then answered by
      // `bus.query` below — so it has to be stamped before it is filed, not
      // when it is answered. There is no task here to borrow an instant from,
      // so one unit of work is minted for its clock alone; the query that
      // follows opens its own.
      const message = withInstant(unstamped, () => unitOfWork().now())
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
      unstamped: QueryMessage,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      const message = withInstant(unstamped, () => unitOfWork().now())
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
