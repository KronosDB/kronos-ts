import {
  generateIdentifier,
  emptyMetadata,
  type Metadata,
} from "@kronos-ts/common"
import type { CommandBus } from "./command-bus.js"
import type { QueryBus } from "./query-bus.js"
import type { CommandDescriptor, QueryDescriptor } from "./descriptor.js"
import type { SubscriptionQueryResult } from "./subscription-query.js"
import { runInNewUoW, type UoWRunner } from "./unit-of-work.js"
import type { z } from "zod"

/**
 * Infers the result type from a descriptor's `result` schema.
 * If no result schema, returns `unknown`.
 */
type InferResult<R extends z.ZodType | undefined> =
  R extends z.ZodType ? z.infer<R> : unknown

/**
 * User-facing command gateway. Wraps payloads into proper command messages
 * and delegates to the command bus.
 *
 * When the command descriptor has a `result` schema, the return type is inferred:
 * ```typescript
 * const CreateCourse = command({
 *   name: qn("university", "CreateCourse"),
 *   payload: z.object({ courseId: z.string() }),
 *   result: z.object({ id: z.string() }),
 * })
 *
 * const result = await gateway.send(CreateCourse, { courseId: "cs-101" })
 * //    ^ { id: string } — inferred from descriptor
 * ```
 */
export interface CommandGateway {
  send<P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
    descriptor: CommandDescriptor<P, R>,
    payload: z.infer<P>,
    metadata?: Metadata,
  ): Promise<InferResult<R>>
}

/**
 * User-facing query gateway. Wraps payloads into proper query messages
 * and delegates to the query bus.
 *
 * When the query descriptor has a `result` schema, the return type is inferred:
 * ```typescript
 * const GetCourse = query({
 *   name: qn("university", "GetCourseView"),
 *   payload: z.object({ courseId: z.string() }),
 *   result: z.object({ courseId: z.string(), name: z.string() }),
 * })
 *
 * const course = await gateway.query(GetCourse, { courseId: "cs-101" })
 * //    ^ { courseId: string, name: string } — inferred from descriptor
 * ```
 */
export interface QueryGateway {
  query<P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
    descriptor: QueryDescriptor<P, R>,
    payload: z.infer<P>,
    metadata?: Metadata,
  ): Promise<InferResult<R>>

  subscriptionQuery<P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
    descriptor: QueryDescriptor<P, R>,
    payload: z.infer<P>,
    metadata?: Metadata,
  ): SubscriptionQueryResult
}

/**
 * Creates a command gateway backed by a command bus.
 *
 * AF5-aligned (CLAUDE.md command model): the gateway is a thin message-builder
 * and does NOT establish a UnitOfWork. The command bus owns the single
 * per-command UoW (and, via the configured `unitOfWorkFactory`, its
 * transaction) — see `simpleCommandBus`. Dispatch interceptors run on
 * the message before it crosses into that UoW; the dispatch-side hook channel
 * is ALS, not a threaded ProcessingContext.
 */
export function commandGateway(bus: CommandBus): CommandGateway {
  return {
    async send(descriptor, payload, metadata) {
      const resolvedMetadata = metadata ?? emptyMetadata()
      return bus.dispatch({
        kind: "command",
        identifier: generateIdentifier(),
        name: descriptor.name,
        payload,
        metadata: resolvedMetadata,
        timestamp: Date.now(),
      }) as any
    },
  }
}

/**
 * Creates a query gateway backed by a query bus.
 *
 * See `commandGateway` for the `unitOfWorkRunner` injection contract.
 */
export function queryGateway(
  bus: QueryBus,
  unitOfWorkRunner: UoWRunner = runInNewUoW,
): QueryGateway {
  return {
    async query(descriptor, payload, metadata) {
      const resolvedMetadata = metadata ?? emptyMetadata()
      // Plan 03-01 (D-32) / Plan 03-03 (CTX-01): gateway always starts a new
      // UoW; bus.query auto-nests via runInUoW in simple-query-bus.
      // subscriptionQuery below stays as-is — its initialResult goes through
      // bus.query, which handles its own UoW.
      return unitOfWorkRunner(resolvedMetadata, () =>
        bus.query({
          kind: "query",
          identifier: generateIdentifier(),
          name: descriptor.name,
          payload,
          metadata: resolvedMetadata,
          timestamp: Date.now(),
        }) as Promise<any>,
      ) as any
    },

    subscriptionQuery(descriptor, payload, metadata) {
      return bus.subscriptionQuery({
        kind: "query",
        identifier: generateIdentifier(),
        name: descriptor.name,
        payload,
        metadata: metadata ?? emptyMetadata(),
        timestamp: Date.now(),
      })
    },
  }
}
