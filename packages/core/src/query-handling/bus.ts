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
   * Pass `uow` to NEST: the query runs inside that unit of work (and its
   * transaction) instead of opening its own. `ctx.query` passes the handler's
   * unit of work; a gateway dispatch passes nothing and the bus opens one.
   *
   * The message may arrive with NO `timestamp` — the `query` verb cannot know
   * the task's instant, so the bus fills it from the unit of work it nests in
   * or mints.
   */
  query(message: QueryMessage, uow?: UnitOfWork): Promise<unknown>

  /**
   * Subscribe a handler for the given query name. The bus hands the unit of
   * work — nested or freshly opened — to the handler.
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
