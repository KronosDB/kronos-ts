import type { QueryMessage, Unstamped } from "../messages/message.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { SubscriptionQueryResult } from "./subscription-query.js"
import type { SubscriptionFilter } from "./subscription-filter.js"

/**
 * The query bus — low-level infrastructure for dispatching query messages.
 *
 */
export interface QueryBus {
  /**
   * Dispatch a query message to its handler(s).
   *
   * Pass `uow` to NEST: the query runs inside that unit of work (and its
   * transaction) instead of opening its own. `ctx.query` passes the handler's
   * unit of work; a gateway dispatch passes nothing and the bus opens one.
   *
   * The message may arrive {@link Unstamped} — the `query` verb cannot know the
   * task's instant, so the bus stamps `timestamp` from the unit of work it
   * nests in or mints.
   */
  query(message: Unstamped<QueryMessage>, uow?: UnitOfWork): Promise<unknown>

  /**
   * Subscribe a handler for the given query name. The bus hands the unit of
   * work — nested or freshly opened — to the handler.
   */
  subscribe(
    queryName: string,
    handler: (message: QueryMessage, uow: UnitOfWork) => Promise<unknown>,
  ): void

  /**
   * Start a subscription query — returns the initial result plus a stream
   * of incremental updates.
   */
  subscriptionQuery(
    message: Unstamped<QueryMessage>,
    bufferSize?: number,
  ): SubscriptionQueryResult

  /**
   * Subscribe to updates only (no initial result).
   * Returns an async iterable of update payloads.
   *
   */
  subscribeToUpdates(
    message: Unstamped<QueryMessage>,
    bufferSize?: number,
  ): AsyncIterable<unknown> & { close(): void }

  /**
   * Emit an update to all active subscription queries matching the filter.
   * When a unit of work is passed, the update is deferred to its AFTER_COMMIT
   * phase; without one it is delivered immediately.
   *
   * The filter can be either a function (local-only when a distributed bus is
   * in use) or a structured `payloadEquals` predicate (crosses transports).
   * See {@link SubscriptionFilter}.
   */
  emitUpdate(
    queryName: string,
    filter: SubscriptionFilter,
    update: unknown,
    uow?: UnitOfWork,
  ): Promise<void>

  /**
   * Complete all subscription queries matching the filter. Deferred to
   * AFTER_COMMIT when a unit of work is passed.
   */
  completeSubscription(
    queryName: string,
    filter?: SubscriptionFilter,
    uow?: UnitOfWork,
  ): Promise<void>

  /**
   * Complete all subscription queries matching the filter with an error.
   * Deferred to AFTER_COMMIT when a unit of work is passed.
   */
  completeSubscriptionExceptionally(
    queryName: string,
    error: Error,
    filter?: SubscriptionFilter,
    uow?: UnitOfWork,
  ): Promise<void>
}
