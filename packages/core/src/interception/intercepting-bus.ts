import type { CommandBus } from "../command-handling/bus.js"
import type { QueryBus } from "../query-handling/bus.js"
import type { CommandMessage, Message, QueryMessage } from "../messaging/messages.js"
import type { SubscriptionQueryResult } from "../query-handling/subscription-query.js"
import type { SubscriptionFilter } from "../query-handling/subscription-filter.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * A message transform. This is the whole interceptor concept, minus the
 * registry: a bus wrapper applies it to every message crossing the seam, and
 * the layering you get is the layering you wrote.
 *
 * ONE function, not a list. Plurality composes in function space —
 * `(m) => tenancy(correlation(m))` — where the order is written down and readable,
 * instead of in a variadic parameter where two far-apart call sites can fight
 * over it.
 *
 * It takes the MESSAGE, not its metadata: correlation is not a function of
 * metadata alone (`causationId` is the message's identifier, which does not
 * appear in its metadata), and a transform that cannot see the identifier
 * cannot express the one rule everybody needs.
 *
 * ```ts
 * interceptingCommandBus(localCommandBus(unitOfWork), correlation)
 * ```
 */
export type Intercept<M extends Message = Message> = (message: M) => M

// The one intercept everybody writes — `correlation` — lives in
// `../correlation/`, with the rest of the concept. This file owns the SEAM; the
// correlation folder owns the RULE.

/**
 * Apply `intercept` to everything dispatched through `next`.
 *
 * ## Why this wraps OUTSIDE a transport
 *
 * `interceptingCommandBus(rabbitMqCommandBus(local, rabbit), correlation)` — the
 * wrap goes on the OUTSIDE, so it covers both branches of the transport's
 * local-vs-remote fork. Wrapping on the inside is the classic correlation defect:
 * commands routed over the wire leave with no `correlationId` / `causationId`
 * at all, because only the local branch reaches the intercepting bus.
 */
export function interceptingCommandBus<B extends CommandBus<any>>(
  next: B,
  intercept: Intercept<CommandMessage>,
): B {
  return {
    ...next,

    async dispatch(message: CommandMessage) {
      // No cast, and nothing to cast to: an intercept takes and returns the
      // same type the bus takes, because there is no separate "not through a
      // task yet" type any more. A transform is a spread over the message, so
      // one that arrived without an instant leaves without one, and the bus
      // behind this fills it in when it mints the unit of work.
      return next.dispatch(intercept(message))
    },
    subscribe(commandName: string, handler: unknown) {
      next.subscribe(commandName, handler as never)
    },
    // CAPABILITY-PRESERVING: `B` in, `B` out. Interception is the OUTERMOST
    // wrap in most chains, so a signature that collapsed to `CommandBus<U>`
    // would throw away whatever the transports and recorders under it had
    // added — and a demand written against those would then fail on a chain
    // that satisfies it perfectly.
  } as B
}

/**
 * Query-side counterpart of {@link interceptingCommandBus}. Only `query` and
 * `subscriptionQuery` carry an outgoing message to transform; the rest of the
 * surface is pass-through.
 *
 * Two functions rather than one generic because `CommandBus.dispatch` and
 * `QueryBus.query` are different method names on unrelated interfaces — a
 * single generic would have to erase both to an index signature to type it,
 * which costs more than the duplication saves.
 */
export function interceptingQueryBus<B extends QueryBus<any>>(
  next: B,
  intercept: Intercept<QueryMessage>,
): B {
  return {
    ...next,

    async query(message: QueryMessage, uow?: UnitOfWork): Promise<unknown> {
      return next.query(intercept(message), uow)
    },
    subscribe(queryName: string, handler: unknown) {
      next.subscribe(queryName, handler as never)
    },
    subscriptionQuery(
      message: QueryMessage,
      bufferSize?: number,
    ): SubscriptionQueryResult {
      return next.subscriptionQuery(intercept(message), bufferSize)
    },
    subscribeToUpdates(
      message: QueryMessage,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      return next.subscribeToUpdates(intercept(message), bufferSize)
    },
    emitUpdate(
      queryName: string,
      filter: SubscriptionFilter,
      update: unknown,
      uow?: UnitOfWork,
    ): Promise<void> {
      return next.emitUpdate(queryName, filter, update, uow)
    },
    completeSubscription(
      queryName: string,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
    ): Promise<void> {
      return next.completeSubscription(queryName, filter, uow)
    },
    completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
    ): Promise<void> {
      return next.completeSubscriptionExceptionally(queryName, error, filter, uow)
    },
    // CAPABILITY-PRESERVING — see `interceptingCommandBus`.
  } as B
}
