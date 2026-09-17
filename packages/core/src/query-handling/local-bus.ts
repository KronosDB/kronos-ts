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
  subscriptionInitialResult,
  runAfterCommitOrImmediately,
} from "./subscription-query.js"
import { type SubscriptionFilter, applySubscriptionFilter } from "./subscription-filter.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
import { isTransactional, type RefusingTransactional } from "../unit-of-work/transactional.js"
/**
 * Simple in-process query bus with subscription query support.
 *
 * Every query opens a fresh UnitOfWork — see `QueryBus.query` for why a read
 * never nests into its caller's. Build this bus from the PLAIN `unitOfWork`
 * factory: a read needs no transaction, and a transactional factory here would
 * wrap every `SELECT` in `BEGIN`/`COMMIT` and hold a pooled connection per
 * in-flight query. Subscription queries receive an initial result plus a
 * stream of incremental updates emitted via `emitUpdate()`.
 *
 * The factory is captured here, mirroring `localCommandBus`, so
 * the `query(bus, …)` verb needs nothing but the bus.
 *
 * Interceptor support is provided by wrapping with
 * {@link interceptingQueryBus}.
 */
export function localQueryBus<F extends () => UnitOfWork = () => UnitOfWork>(
  unitOfWork: F & RefusingTransactional<F>,
): SubscriptionCapableQueryBus<ReturnType<F>> {
  type U = ReturnType<F>
  if (isTransactional(unitOfWork)) {
    throw new Error(
      "localQueryBus: a query bus must be built from the plain `unitOfWork` factory, not a " +
        "transaction family's. A read needs no transaction, and a query always runs in a task " +
        "of its own — a transactional query bus would wrap every read in BEGIN/COMMIT and hold " +
        "a pooled connection per in-flight query. Use `localQueryBus(unitOfWork)`.",
    )
  }
  const handlers = new Map<string, (message: QueryMessage, uow: U) => Promise<unknown>>()

  // Active subscription query handlers, keyed by query identifier
  const subscriptions = new Map<string, UpdateHandler>()

  const bus: SubscriptionCapableQueryBus<U> = {
    async query(message: QueryMessage): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for query "${key}"`)
      }

      // Mirrors local-command-bus.dispatch: every query mints its own unit of
      // work, and the MINTED handle is what the handler gets — see the note in
      // `localCommandBus`.
      const opened = unitOfWork() as U
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

      if (subscriptions.size >= 1024) throw new Error("Subscription capacity 1024 exhausted")
      let closeInitial = () => {}
      const handler = updateHandler(message, bufferSize, () => { subscriptions.delete(queryId); closeInitial() })
      subscriptions.set(queryId, handler)

      const initial = subscriptionInitialResult(bus.query(message), (error) => handler.completeExceptionally(error))
      closeInitial = initial.close

      return {
        initialResult: initial.initialResult,
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

      if (subscriptions.size >= 1024) throw new Error("Subscription capacity 1024 exhausted")
      const handler = updateHandler(message, bufferSize, () => subscriptions.delete(queryId))
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
