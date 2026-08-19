import type { z } from "zod"
import { emptyMetadata, type Metadata } from "../primitives/metadata.js"
import { generateIdentifier } from "../primitives/identifier.js"
import type { QualifiedName } from "../primitives/qualified-name.js"
import {
  queryDescriptor,
  type CommandDescriptor,
  type InferResult,
  type QueryDescriptor,
} from "../messages/descriptor.js"
import type { CommandBus } from "./command-bus.js"
import type { QueryBus } from "./query-bus.js"
import type { SubscriptionQueryResult } from "./subscription-query.js"

// ---------------------------------------------------------------------------
// Edge verbs — build the message, hand it to the bus. Nothing named "gateway".
//
// A gateway was an object with one method that closed over a bus. These are the
// same operation with the bus as its first argument: a library function of ALL
// its real arguments, which a host can partially apply itself if it wants the
// bus fixed. There is nothing to construct and nothing to hold.
// ---------------------------------------------------------------------------

/**
 * Build a command message from `descriptor` + `payload` and dispatch it.
 *
 * ```ts
 * const result = await send(commandBus, CreateCourse, { courseId: "cs-101" })
 * //    ^ inferred from the descriptor's `result` schema
 * ```
 *
 * `metadata` is where PER-REQUEST data enters — the actor, the tenant, the
 * trace header. It enters HERE because this is where the message is born, and
 * a message's metadata cannot be reconstructed anywhere downstream.
 *
 * The `timestamp` does NOT enter here, and that is the one asymmetry worth
 * knowing: the BUS owns unit-of-work entry (`simpleCommandBus` opens a fresh one
 * per dispatch) and a unit of work carries the clock, so the bus stamps the
 * instant from `uow.now()`. This verb establishes nothing and reads no clock.
 */
export async function send<P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
  bus: CommandBus,
  descriptor: CommandDescriptor<P, R>,
  payload: z.infer<P>,
  metadata?: Metadata,
): Promise<InferResult<R>> {
  return bus.dispatch({
    kind: "command",
    identifier: generateIdentifier(),
    name: descriptor.name,
    payload,
    metadata: metadata ?? emptyMetadata(),
  }) as Promise<InferResult<R>>
}

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
export function query<P extends z.ZodType>(def: {
  name: QualifiedName
  version?: string
  payload: P
}): QueryDescriptor<P, undefined>

export function query<P extends z.ZodType, R extends z.ZodType>(def: {
  name: QualifiedName
  version?: string
  payload: P
  result: R
}): QueryDescriptor<P, R>

export function query<P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
  bus: QueryBus,
  descriptor: QueryDescriptor<P, R>,
  payload: z.infer<P>,
  metadata?: Metadata,
): Promise<InferResult<R>>

export function query(a: any, b?: any, c?: any, d?: Metadata): any {
  if (b === undefined) return queryDescriptor(a)
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

/**
 * Start a subscription query: the initial result plus the stream of updates
 * handlers emit for it via `ctx.emitUpdate`.
 */
export function subscriptionQuery<
  P extends z.ZodType,
  R extends z.ZodType | undefined = undefined,
>(
  bus: QueryBus,
  descriptor: QueryDescriptor<P, R>,
  payload: z.infer<P>,
  metadata?: Metadata,
): SubscriptionQueryResult {
  return bus.subscriptionQuery({
    kind: "query",
    identifier: generateIdentifier(),
    name: descriptor.name,
    payload,
    metadata: metadata ?? emptyMetadata(),
  })
}
