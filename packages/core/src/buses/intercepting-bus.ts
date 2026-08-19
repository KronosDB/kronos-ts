import type { CommandBus } from "./command-bus.js"
import type { QueryBus } from "./query-bus.js"
import type { CommandMessage, Message, QueryMessage, Unstamped } from "../messages/message.js"
import type { SubscriptionQueryResult } from "./subscription-query.js"
import type { SubscriptionFilter } from "./subscription-filter.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * A message transform. This is the whole interceptor concept, minus the
 * registry: a bus wrapper applies it to every message crossing the seam, and
 * the layering you get is the layering you wrote.
 *
 * ONE function, not a list. Plurality composes in function space —
 * `(m) => tenancy(lineage(m))` — where the order is written down and readable,
 * instead of in a variadic parameter where two far-apart call sites can fight
 * over it.
 *
 * It takes the MESSAGE, not its metadata: lineage is not a function of
 * metadata alone (`causationId` is the message's identifier, which does not
 * appear in its metadata), and a transform that cannot see the identifier
 * cannot express the one rule everybody needs.
 *
 * ```ts
 * interceptingCommandBus(simpleCommandBus(unitOfWork), lineage)
 * ```
 */
export type Intercept<M extends Message = Message> = (message: M) => M

/**
 * The lineage rule, written out. Both fields SEED and neither clobbers:
 *
 * - `correlationId` is preserved when the message already has one, and
 *   otherwise starts a new chain at this message.
 * - `causationId` is preserved when the message already has one, and otherwise
 *   starts at this message's own identifier.
 *
 * The `??` on `causationId` is the whole point. A message that arrives with a
 * cause already on it was CAUSED by something — `ctx.send` / `ctx.query` /
 * `ctx.append` stamp the handled message's identifier onto everything a handler
 * emits, which is where a real causationId comes from. An unconditional
 * `causationId: message.identifier` at the bus edge overwrote that with the
 * emitted message's own identifier, so every message in a multi-hop chain
 * claimed to have caused itself and the causal graph collapsed to a set of
 * self-loops. `lineage` seeds ROOTS — messages born at an edge with no cause —
 * and ctx re-stamps per hop.
 *
 * Applying it twice is a no-op on both fields, which is what lets a transport
 * bus wrap a local segment that is itself intercepting.
 */
export const lineage = <M extends Message>(message: M): M => ({
  ...message,
  metadata: {
    ...message.metadata,
    correlationId: String(message.metadata.correlationId ?? message.identifier),
    causationId: String(message.metadata.causationId ?? message.identifier),
  },
})

/**
 * Apply `intercept` to everything dispatched through `delegate`.
 *
 * ## Why this wraps OUTSIDE a transport
 *
 * `interceptingCommandBus(rabbitMqCommandBus(rabbit, local), lineage)` — the
 * wrap goes on the OUTSIDE, so it covers both branches of the transport's
 * local-vs-remote fork. Wrapping on the inside is the classic lineage defect:
 * commands routed over the wire leave with no `correlationId` / `causationId`
 * at all, because only the local branch reaches the intercepting bus.
 */
export function interceptingCommandBus(
  delegate: CommandBus,
  intercept: Intercept<CommandMessage>,
): CommandBus {
  return {
    async dispatch(message) {
      // An intercept sees the message as it was BORN, which may be before the
      // bus behind this one stamped its instant — the cast says exactly that.
      // Every transform is a spread over the message, so a message with no
      // `timestamp` yet comes out of one still having none, and the delegate
      // stamps it when it mints the unit of work.
      return delegate.dispatch(
        intercept(message as CommandMessage) as Unstamped<CommandMessage>,
      )
    },
    subscribe(commandName, handler) {
      delegate.subscribe(commandName, handler)
    },
  }
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
export function interceptingQueryBus(
  delegate: QueryBus,
  intercept: Intercept<QueryMessage>,
): QueryBus {
  return {
    async query(message: Unstamped<QueryMessage>, uow?: UnitOfWork): Promise<unknown> {
      return delegate.query(intercept(message as QueryMessage) as Unstamped<QueryMessage>, uow)
    },
    subscribe(
      queryName: string,
      handler: (message: QueryMessage, uow: UnitOfWork) => Promise<unknown>,
    ) {
      delegate.subscribe(queryName, handler)
    },
    subscriptionQuery(
      message: Unstamped<QueryMessage>,
      bufferSize?: number,
    ): SubscriptionQueryResult {
      return delegate.subscriptionQuery(
        intercept(message as QueryMessage) as Unstamped<QueryMessage>,
        bufferSize,
      )
    },
    subscribeToUpdates(
      message: Unstamped<QueryMessage>,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      return delegate.subscribeToUpdates(
        intercept(message as QueryMessage) as Unstamped<QueryMessage>,
        bufferSize,
      )
    },
    emitUpdate(
      queryName: string,
      filter: SubscriptionFilter,
      update: unknown,
      uow?: UnitOfWork,
    ): Promise<void> {
      return delegate.emitUpdate(queryName, filter, update, uow)
    },
    completeSubscription(
      queryName: string,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
    ): Promise<void> {
      return delegate.completeSubscription(queryName, filter, uow)
    },
    completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
    ): Promise<void> {
      return delegate.completeSubscriptionExceptionally(queryName, error, filter, uow)
    },
  }
}
