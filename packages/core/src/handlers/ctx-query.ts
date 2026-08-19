import { emptyMetadata, mergeMetadata } from "../primitives/metadata.js"
import { generateIdentifier } from "../primitives/identifier.js"
import type { z } from "zod"
import type { QueryDescriptor } from "../messages/descriptor.js"
import type { InferResult } from "../messages/descriptor.js"
import type { Message } from "../messages/message.js"
import type { QueryBus } from "../buses/query-bus.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"

/** `ctx.query` — consult a query handler from inside a handler. */
export type QueryDispatchFunction = <P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
  descriptor: QueryDescriptor<P, R>,
  payload: z.infer<P>,
) => Promise<InferResult<R>>

/**
 * Build the `query` capability for ONE invocation, closed over that
 * invocation's unit of work and query bus.
 *
 * Internal — not exported from the package barrel. Handlers reach the result
 * as `ctx.query`.
 *
 * AF5-aligned: in Axon you inject the query gateway into any handler and use
 * it; this is the kronos-shaped equivalent. Lineage is stamped onto the
 * outgoing query exactly as `ctx.send` stamps it — the causing message's own
 * metadata as the base, the unit of work's correlation data merged over. The
 * unit of work itself is handed to `bus.query`, so a consulting read NESTS —
 * it shares the handler's UoW (and its transaction) rather than opening one.
 *
 * Prefer a projection or a capability command; reach for this when a decision
 * genuinely needs another module's answer, fresh.
 */
export function queryFunction(deps: {
  uow: UnitOfWork
  message?: Message
  queryBus?: QueryBus
}): QueryDispatchFunction {
  const dispatch = async (
    descriptor: QueryDescriptor<any, any>,
    payload: unknown,
  ): Promise<unknown> => {
    const uow = requireInvocation(deps.uow)
    const bus = deps.queryBus
    if (!bus) throw new Error("No query bus configured")
    return bus.query(
      {
        kind: "query",
        identifier: generateIdentifier(),
        name: descriptor.name,
        payload,
        metadata: mergeMetadata(deps.message?.metadata ?? emptyMetadata(), uow.correlationData()),
        timestamp: uow.now(),
      },
      uow,
    )
  }
  return dispatch as QueryDispatchFunction
}
