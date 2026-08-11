import type { QueryMessage } from "./message.js"
import type { SubscribeOptions } from "./command-bus.js"
export type { SubscribeOptions }
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
   * The bus auto-nests via ALS (CTX-02): when called outside a UnitOfWork,
   * `runInUoW` creates one; when called inside an active UoW (handler-internal
   * re-dispatch), the active UoW is reused.
   */
  query(message: QueryMessage): Promise<unknown>

  /**
   * Subscribe a handler for the given query name.
   *
   * Plan 03-04 (CTX-04 / D-34): handler signature dropped its `ctx`
   * parameter. The ProcessingContext type is gone.
   */
  subscribe(
    queryName: string,
    handler: (message: QueryMessage) => Promise<unknown>,
    options?: SubscribeOptions,
  ): void

  /**
   * Start a subscription query — returns the initial result plus a stream
   * of incremental updates.
   */
  subscriptionQuery(message: QueryMessage, bufferSize?: number): SubscriptionQueryResult

  /**
   * Subscribe to updates only (no initial result).
   * Returns an async iterable of update payloads.
   *
   */
  subscribeToUpdates(message: QueryMessage, bufferSize?: number): AsyncIterable<unknown> & { close(): void }

  /**
   * Emit an update to all active subscription queries matching the filter.
   * When called within an active UnitOfWork (detected via ALS), the update is
   * deferred to AFTER_COMMIT.
   *
   * The filter can be either a function (local-only when a distributed bus is
   * in use) or a structured `payloadEquals` predicate (crosses transports).
   * See {@link SubscriptionFilter}.
   */
  emitUpdate(
    queryName: string,
    filter: SubscriptionFilter,
    update: unknown,
  ): Promise<void>

  /**
   * Complete all subscription queries matching the filter.
   */
  completeSubscription(
    queryName: string,
    filter?: SubscriptionFilter,
  ): Promise<void>

  /**
   * Complete all subscription queries matching the filter with an error.
   */
  completeSubscriptionExceptionally(
    queryName: string,
    error: Error,
    filter?: SubscriptionFilter,
  ): Promise<void>
}
