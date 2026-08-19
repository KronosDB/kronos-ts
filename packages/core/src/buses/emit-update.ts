import type { z } from "zod"
import { qualifiedNameToString } from "../primitives/qualified-name.js"
import type { QueryBus } from "./query-bus.js"
import type { QueryDescriptor } from "../messages/descriptor.js"
import type { SubscriptionFilter } from "./subscription-filter.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"

/** Emit a subscription-query update from within the current invocation. */
export interface EmitUpdateFunction {
  <Q extends z.ZodType>(
    query: QueryDescriptor<Q>,
    filter: SubscriptionFilter<z.infer<Q>>,
    update: unknown,
  ): void
}

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
    const bus = deps.queryBus
    if (!bus) throw new Error("No query bus configured")
    const queryName = qualifiedNameToString(queryDescriptor.name)
    void bus.emitUpdate(queryName, filter as SubscriptionFilter, update, uow)
  }
}
