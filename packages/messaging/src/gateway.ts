import {
  generateIdentifier,
  emptyMetadata,
  type Metadata,
} from "@kronos-ts/common"
import type { CommandBus } from "./command-bus.js"
import type { QueryBus } from "./query-bus.js"
import type { CommandDescriptor, QueryDescriptor } from "./descriptor.js"
import type { ProcessingContext } from "./processing-context.js"
import type { SubscriptionQueryResult } from "./subscription-query.js"
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
    context?: ProcessingContext,
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
    context?: ProcessingContext,
  ): Promise<InferResult<R>>

  subscriptionQuery<P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
    descriptor: QueryDescriptor<P, R>,
    payload: z.infer<P>,
    metadata?: Metadata,
  ): SubscriptionQueryResult
}

/**
 * Creates a command gateway backed by a command bus.
 */
export function createCommandGateway(bus: CommandBus): CommandGateway {
  return {
    async send(descriptor, payload, metadata, context) {
      return bus.dispatch({
        identifier: generateIdentifier(),
        name: descriptor.name,
        payload,
        metadata: metadata ?? emptyMetadata(),
        timestamp: Date.now(),
      }, context) as any
    },
  }
}

/**
 * Creates a query gateway backed by a query bus.
 */
export function createQueryGateway(bus: QueryBus): QueryGateway {
  return {
    async query(descriptor, payload, metadata, context) {
      return bus.query({
        identifier: generateIdentifier(),
        name: descriptor.name,
        payload,
        metadata: metadata ?? emptyMetadata(),
        timestamp: Date.now(),
      }, context) as any
    },

    subscriptionQuery(descriptor, payload, metadata) {
      return bus.subscriptionQuery({
        identifier: generateIdentifier(),
        name: descriptor.name,
        payload,
        metadata: metadata ?? emptyMetadata(),
        timestamp: Date.now(),
      })
    },
  }
}
