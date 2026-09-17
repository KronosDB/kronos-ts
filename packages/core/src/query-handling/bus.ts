import type { QueryMessage } from "../messaging/messages.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { SubscriptionQueryResult } from "./subscription-query.js"
import type { SubscriptionFilter } from "./subscription-filter.js"

/**
 * The query bus — low-level infrastructure for dispatching query messages.
 *
 * `U` is the unit of work this bus MINTS for a primary query, and the type its
 * subscribed handlers receive. Defaults to the bare {@link UnitOfWork}; see
 * {@link import("../command-handling/bus.js").CommandBus} for why it is threaded.
 */
export type QueryBus<U extends UnitOfWork = UnitOfWork> = {
  /**
   * Dispatch a query message to its handler(s).
   *
   * A QUERY IS ALWAYS ITS OWN TASK. The bus mints a fresh unit of work for
   * every query, whether it arrived at the edge or from `ctx.query` inside a
   * handler — a read never shares the caller's transaction, clock or
   * correlation, and behaves the same whether the handler is co-located or
   * reached over a transport. (It used to take an optional unit of work to
   * NEST into; that made a co-located read run inside the command's
   * transaction while a remote one did not, and let a nested read overwrite
   * the caller's correlation. AF5's `SimpleQueryBus` mints per query too.)
   *
   * The message may arrive with NO `timestamp` — the bus fills it from the
   * unit of work it mints.
   */
  query(message: QueryMessage): Promise<unknown>

  /**
   * Subscribe a handler for the given query name. The bus hands the unit of
   * work it minted for the query to the handler.
   */
  subscribe(
    queryName: string,
    handler: (message: QueryMessage, uow: U) => Promise<unknown>,
  ): void
}

/**
 * THE SUBSCRIPTION TIER — the third capability tier, and the first on a BUS.
 *
 * The base {@link QueryBus} is COMPLETE without it: a bus you can query and
 * subscribe handlers on is everything request/response needs, and an
 * implementer writes TWO functions, not seven. Live updates are a capability a
 * bus HAS or does not — offered natively or added by wrapping — exactly as
 * snapshotting and scheduling are tiers on the log. `localQueryBus` offers it
 * natively (the machinery is in-process anyway); the transports offer it
 * server- or broker-mediated. A bus that cannot serve live updates simply does
 * not claim the type, and everything demanding it refuses at COMPILE TIME
 * instead of throwing on the first subscriber somebody armed in production.
 */
export type SubscriptionBusCapability = {
  /**
   * Start a subscription query — returns the initial result plus a stream
   * of incremental updates.
   */
  subscriptionQuery(
    message: QueryMessage,
    bufferSize?: number,
  ): SubscriptionQueryResult

  /**
   * Subscribe to updates only (no initial result).
   * Returns an async iterable of update payloads.
   *
   */
  subscribeToUpdates(
    message: QueryMessage,
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

/** A query bus that can also serve live subscription queries. */
export type SubscriptionCapableQueryBus<U extends UnitOfWork = UnitOfWork> = QueryBus<U> &
  SubscriptionBusCapability

/**
 * THE anchor for the subscription demand — the mirror of `IfScheduleCapable`,
 * branched on the BUS instead of the log. Anything later anchors HERE; add a
 * capability, not a predicate.
 */
export type IfSubscriptionCapable<Q extends QueryBus<any>, Capable, Bare> =
  Q extends SubscriptionBusCapability ? Capable : Bare
