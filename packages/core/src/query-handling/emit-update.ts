import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import { qualifiedNameToString, type QueryDescriptor } from "../messaging/messages.js"
import type { IfSubscriptionCapable, QueryBus, SubscriptionBusCapability } from "./bus.js"
import type { SubscriptionFilter } from "./subscription-filter.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * WHAT THE SUBSCRIPTION TIER PUTS ON A CONTEXT — one member, declared once.
 *
 * THIS IS THE SPELLING A HANDLER WRITES. A handler says what it USES by
 * INTERSECTING the face, never by restating the context's type parameters:
 *
 * ```ts
 * eventHandler(Enrolled, async (m, ctx: EventHandlerContext & SubscriptionCapability) => {
 *   ctx.emitUpdate(Watch, (q) => q.id === m.payload.id, view)
 * })
 * ```
 *
 * `EventHandlerContext<EventStore, SubscriptionCapableQueryBus>` says the same
 * thing and is worse: type parameters are POSITIONAL, so naming the bus means
 * restating the log — a default the handler had no opinion about — and the
 * annotation then mentions two things to demand one. The parameters are the
 * SUPPLY side (the entry threads its bus and its log in); the intersection is
 * the DEMAND side, and a demand names only what it needs. The persistence
 * packages' faces (`DrizzleCapability`, `PostgresCapability`) are written and
 * demanded exactly this way.
 *
 * The refusal is unchanged either way: against an entry whose `queryBus` never
 * claimed {@link SubscriptionBusCapability} the supplied context has no
 * `emitUpdate`, so the handler does not fit the entry.
 */
export type SubscriptionCapability = {
  readonly emitUpdate: EmitUpdateFunction
}

/**
 * The face as the CONTEXT assembles it — present only when the entry's bus
 * claimed the tier. Against a bare bus `ctx.emitUpdate` is structurally
 * ABSENT rather than present-and-throwing ("Property 'emitUpdate' does not
 * exist"), the same construction as `SuppliedScheduleCapability<E>` one tier
 * over. Hosts write {@link SubscriptionCapability}; this is what supplies it.
 */
export type SuppliedSubscriptionCapability<Q extends QueryBus<any>> = IfSubscriptionCapable<
  Q,
  SubscriptionCapability,
  unknown
>

/** Emit a subscription-query update from within the current invocation. */
export type EmitUpdateFunction = <Q extends StandardSchemaV1>(
  query: QueryDescriptor<Q>,
  filter: SubscriptionFilter<InferOutput<Q>>,
  update: unknown,
) => void

/**
 * Build the `emitUpdate` capability for ONE invocation, closed over that
 * invocation's unit of work and query bus.
 *
 * Internal — not exported from the package barrel. Handlers reach the result
 * as `ctx.emitUpdate`.
 *
 * Throws {@link NoActiveUnitOfWork} once the unit of work has closed;
 * {@link WrongUoWPhase} outside the INVOCATION phase. The unit of work is
 * handed to the bus so the update is deferred to AFTER_COMMIT — subscribers
 * only see what actually committed.
 */
export function emitUpdateFunction(deps: {
  uow: UnitOfWork
  queryBus?: QueryBus
}): EmitUpdateFunction {
  return (queryDescriptor, filter, update) => {
    const uow = requireInvocation(deps.uow)
    const bus = deps.queryBus as (QueryBus & Partial<SubscriptionBusCapability>) | undefined
    if (!bus) throw new Error("No query bus configured")
    // The demand is a type and types are erased; this is the one defensive
    // trace, for JavaScript callers. Core names the CAPABILITY, not a bus.
    if (typeof bus.emitUpdate !== "function")
      throw new Error(
        "this entry's queryBus cannot emit subscription updates — the subscription tier is a capability of the bus (SubscriptionCapableQueryBus); wire one that has it",
      )
    const queryName = qualifiedNameToString(queryDescriptor.name)
    void bus.emitUpdate(queryName, filter as SubscriptionFilter, update, uow)
  }
}
