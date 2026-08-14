import { generateIdentifier } from "@kronos-ts/common"
import type { z } from "zod"
import type { QueryDescriptor } from "./descriptor.js"
import { QUERY_BUS_KEY } from "./emit-update.js"
import type { InferResult } from "./gateway.js"
import { requireInvocationPhase } from "./processing-state.js"
import type { QueryBus } from "./query-bus.js"

type QueryDispatchFunction = <P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
  descriptor: QueryDescriptor<P, R>,
  payload: z.infer<P>,
) => Promise<InferResult<R>>

/**
 * Query from inside a handler.
 *
 * Internal — not exported from the package barrel. Handlers reach this as
 * `ctx.query` on the handler contexts; the ALS resolution below is the
 * implementation detail that makes the frozen context instance work.
 *
 * AF5-aligned: in Axon you inject the QueryGateway into any handler and use
 * it; this is the kronos-shaped equivalent. The caller's `metadata` IS
 * carried onto the outgoing query, so correlation/causation lineage
 * propagates the same way `ctx.send` propagates it — through message
 * metadata — across any transport, local or distributed. `bus.query`
 * auto-nests into the active UnitOfWork (see `simple-query-bus`), so a
 * consulting read shares the handler's UoW rather than opening one.
 *
 * Prefer a projection or a capability command; reach for this when a
 * decision genuinely needs another module's answer, fresh.
 */
async function dispatchQuery(descriptor: QueryDescriptor<any, any>, payload: unknown): Promise<unknown> {
  const state = requireInvocationPhase() // D-43 phase guard
  const bus = state.resources.get(QUERY_BUS_KEY.symbol) as QueryBus | undefined
  if (!bus) throw new Error("No query bus configured")
  return bus.query({
    kind: "query",
    identifier: generateIdentifier(),
    name: descriptor.name,
    payload,
    metadata: state.metadata,
    timestamp: Date.now(),
  })
}

export const query = dispatchQuery as QueryDispatchFunction
