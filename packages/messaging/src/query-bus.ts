import type { QueryMessage } from "./message.js"
import type { ProcessingContext } from "./processing-context.js"
import type { SubscriptionQueryResult } from "./subscription-query.js"

/**
 * The query bus — low-level infrastructure for dispatching query messages.
 *
 * Aligned with AF5's {@code QueryBus} interface.
 */
export interface QueryBus {
  /**
   * Dispatch a query message to its handler(s).
   * Creates a UnitOfWork internally and drives the full lifecycle.
   *
   * Aligned with AF5's {@code QueryBus.query()}.
   */
  query(message: QueryMessage, context?: ProcessingContext): Promise<unknown>

  /**
   * Subscribe a handler for the given query name.
   */
  subscribe(
    queryName: string,
    handler: (message: QueryMessage, ctx: ProcessingContext) => Promise<unknown>,
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
   * Aligned with AF5's {@code QueryBus.subscribeToUpdates()}.
   */
  subscribeToUpdates(message: QueryMessage, bufferSize?: number): AsyncIterable<unknown> & { close(): void }

  /**
   * Emit an update to all active subscription queries matching the filter.
   * When called within a ProcessingContext, the update is deferred to AFTER_COMMIT.
   */
  emitUpdate(
    queryName: string,
    filter: (queryPayload: unknown) => boolean,
    update: unknown,
    context?: ProcessingContext,
  ): Promise<void>

  /**
   * Complete all subscription queries matching the filter.
   */
  completeSubscription(
    queryName: string,
    filter?: (queryPayload: unknown) => boolean,
    context?: ProcessingContext,
  ): Promise<void>

  /**
   * Complete all subscription queries matching the filter with an error.
   */
  completeSubscriptionExceptionally(
    queryName: string,
    error: Error,
    filter?: (queryPayload: unknown) => boolean,
    context?: ProcessingContext,
  ): Promise<void>
}
