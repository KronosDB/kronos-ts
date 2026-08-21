import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import {
  emptyMetadata,
  type Metadata,
  type QueryDescriptor,
  type InferResult,
  queryDescriptor as declareQuery,
  type QualifiedName,
} from "../messaging/messages.js"
import { generateIdentifier } from "../messaging/identifier.js"
import type { QueryBus } from "./bus.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"

// ---------------------------------------------------------------------------
// THE TWO BIRTHS OF A QUERY, in one file.
//
// A query is born either at the EDGE — `query(bus, D, p)`, a read arriving from
// outside — or INSIDE A HANDLING, as `ctx.query`, where a task is already open
// and the read NESTS into it. Same message, same bus call, two lifetimes; the
// only difference between the two functions below is which one of them has a
// `uow` to stamp from and to join.
// ---------------------------------------------------------------------------

/** `ctx.query` — consult a query handler from inside a handler. */
export type QueryDispatchFunction = <P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined = undefined>(
  descriptor: QueryDescriptor<P, R>,
  payload: InferOutput<P>,
  metadata?: Metadata,
) => Promise<InferResult<R>>

/**
 * Build the `query` capability for ONE invocation, closed over that
 * invocation's unit of work and query bus.
 *
 * Internal — not exported from the package barrel. Handlers reach the result
 * as `ctx.query`.
 *
 * AF5-aligned: in Axon you inject the query gateway into any handler and use
 * it; this is the kronos-shaped equivalent. The unit of work itself is handed
 * to `bus.query`, so a consulting read NESTS — it shares the handler's UoW (and
 * its transaction) rather than opening one.
 *
 * The metadata is exactly what the caller passed, for the same reason
 * `ctx.send`'s is: carrying is a host policy, and
 * `correlatingHandler(next, from)` is what implements it — by wrapping this
 * verb and overlaying through this parameter.
 *
 * Prefer a projection or a capability command; reach for this when a decision
 * genuinely needs another module's answer, fresh.
 */
export function queryFunction(deps: {
  uow: UnitOfWork
  queryBus?: QueryBus
}): QueryDispatchFunction {
  const dispatch = async (
    descriptor: QueryDescriptor<any, any>,
    payload: unknown,
    metadata?: Metadata,
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
        metadata: metadata ?? emptyMetadata(),
        timestamp: uow.now(),
      },
      uow,
    )
  }
  return dispatch as QueryDispatchFunction
}

// ---------------------------------------------------------------------------
// The EDGE verb — build the message, hand it to the bus. Nothing named
// "gateway". The bus is the first argument, so a host that wants it fixed
// partially applies the function itself.
// ---------------------------------------------------------------------------

/**
 * Two operations, one name — because the surface names both `query({ … })`
 * (declare a query type) and `query(bus, …)` (dispatch one), and a barrel
 * exports one binding per name. They are told apart by arity: the declaration
 * form takes a single definition object, the dispatch form takes a bus first.
 *
 * Declare:
 * ```ts
 * const GetCourse = query({ name: qn("university", "GetCourse"), payload: … })
 * ```
 *
 * Dispatch:
 * ```ts
 * const course = await query(queryBus, GetCourse, { courseId: "cs-101" })
 * ```
 */
export function query<P extends StandardSchemaV1>(def: {
  name: QualifiedName
  version?: string
  payload: P
}): QueryDescriptor<P, undefined>

export function query<P extends StandardSchemaV1, R extends StandardSchemaV1>(def: {
  name: QualifiedName
  version?: string
  payload: P
  result: R
}): QueryDescriptor<P, R>

export function query<P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined = undefined>(
  bus: QueryBus,
  descriptor: QueryDescriptor<P, R>,
  payload: InferOutput<P>,
  metadata?: Metadata,
): Promise<InferResult<R>>

export function query(a: any, b?: any, c?: any, d?: Metadata): any {
  if (b === undefined) return declareQuery(a)
  const bus = a as QueryBus
  const descriptor = b as QueryDescriptor
  return bus.query({
    kind: "query",
    identifier: generateIdentifier(),
    name: descriptor.name,
    payload: c,
    metadata: d ?? emptyMetadata(),
  })
}
