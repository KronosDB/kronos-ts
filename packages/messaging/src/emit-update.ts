import type { z } from "zod"
import { resourceKey, qualifiedNameToString, type ResourceKey } from "@kronos-ts/common"
import { requireInvocationPhase } from "./processing-state.js"
import type { QueryBus } from "./query-bus.js"
import type { QueryDescriptor } from "./descriptor.js"

/** Emit a subscription-query update from within the current processing context. */
export interface EmitUpdateFunction {
  <Q extends z.ZodType>(
    query: QueryDescriptor<Q>,
    filter: (query: z.infer<Q>) => boolean,
    update: unknown,
  ): void
}

/**
 * Resource key for the query bus component.
 * Written by handling modules + processors at handler-invocation entry (D-44).
 */
export const QUERY_BUS_KEY: ResourceKey<QueryBus> = resourceKey("queryBus")

/**
 * Plan 04-01 (HDL-02 / D-42): module-level emitUpdate.
 *
 * Throws NoActiveUnitOfWork outside a UoW; throws WrongUoWPhase outside
 * INVOCATION phase (D-43 mutator guard). Emits a subscription query update
 * through the active query bus.
 */
export const emitUpdate: EmitUpdateFunction = (queryDescriptor, filter, update) => {
  const state = requireInvocationPhase() // D-43 mutator guard
  const bus = state.resources.get(QUERY_BUS_KEY.symbol) as QueryBus | undefined
  if (!bus) throw new Error("No query bus configured")
  const queryName = qualifiedNameToString(queryDescriptor.name)
  bus.emitUpdate(queryName, filter as (q: unknown) => boolean, update)
}
