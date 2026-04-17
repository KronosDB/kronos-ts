import type { z } from "zod"
import type { QualifiedName, Tag } from "@kronos-ts/common"
import { tagsFromRecord } from "@kronos-ts/common"

/**
 * Describes a command message type — its name, payload schema,
 * and optional result schema for typed gateway returns.
 */
export interface CommandDescriptor<
  P extends z.ZodType = z.ZodType,
  R extends z.ZodType | undefined = undefined,
> {
  readonly kind: "command"
  readonly name: QualifiedName
  /** Version of the command. Default: "1.0". Aligned with `@Command(version = "...")`. */
  readonly version: string
  readonly payload: P
  /** Optional result schema — enables typed return from `commandGateway.send()`. */
  readonly result?: R
  /**
   * The payload field that contains the routing key for distributed command routing.
   * Aligned with Java's `@Command(routingKey = "fieldName")`.
   *
   * Used by the command gateway to extract the routing key before dispatch.
   * Commands with the same routing key are routed to the same handler instance.
   */
  readonly routingKey?: string
}

/**
 * Describes an event message type — its name, payload schema, and tag derivation.
 * Tags define how events are indexed for criteria-based sourcing.
 */
export interface EventDescriptor<P extends z.ZodType = z.ZodType> {
  readonly kind: "event"
  readonly name: QualifiedName
  readonly version: string
  readonly payload: P
  readonly tags?: (payload: z.infer<P>) => Tag[]
}

/**
 * Describes a query message type — its name, payload schema,
 * and optional result schema for typed gateway returns.
 */
export interface QueryDescriptor<
  P extends z.ZodType = z.ZodType,
  R extends z.ZodType | undefined = undefined,
> {
  readonly kind: "query"
  readonly name: QualifiedName
  /** Version of the query. Default: "1.0". Aligned with `@Query(version = "...")`. */
  readonly version: string
  readonly payload: P
  /** Optional result schema — enables typed return from `queryGateway.query()`. */
  readonly result?: R
}

/** Any message descriptor. */
export type MessageDescriptor =
  | CommandDescriptor
  | EventDescriptor
  | QueryDescriptor

/**
 * Creates a command descriptor.
 *
 * Without result schema (void command):
 * ```
 * const CreateCourse = command({
 *   name: qn("university", "CreateCourse"),
 *   payload: z.object({ courseId: z.string(), name: z.string() }),
 *   routingKey: "courseId",
 * })
 * ```
 *
 * With result schema (typed return):
 * ```
 * const CreateCourse = command({
 *   name: qn("university", "CreateCourse"),
 *   payload: z.object({ courseId: z.string() }),
 *   result: z.object({ courseId: z.string() }),
 *   routingKey: "courseId",
 * })
 * // commandGateway.send(CreateCourse, { courseId: "cs-101" }) → Promise<{ courseId: string }>
 * ```
 */
export function command<P extends z.ZodType>(def: {
  name: QualifiedName
  version?: string
  payload: P
  routingKey?: string
}): CommandDescriptor<P, undefined>

export function command<P extends z.ZodType, R extends z.ZodType>(def: {
  name: QualifiedName
  version?: string
  payload: P
  result: R
  routingKey?: string
}): CommandDescriptor<P, R>

export function command(def: any): CommandDescriptor {
  return { kind: "command" as const, version: def.version ?? "1.0", ...def }
}

/**
 * Creates an event descriptor.
 *
 * Tags can return `Tag[]` or a `Record<string, string>`:
 * ```typescript
 * event({
 *   name: qn("university", "CourseCreated"),
 *   payload: z.object({ courseId: z.string(), name: z.string() }),
 *   tags: (p) => ({ courseId: p.courseId }),
 * })
 * ```
 */
export function event<P extends z.ZodType>(def: {
  name: QualifiedName
  version?: string
  payload: P
  tags?: (payload: z.infer<P>) => Tag[] | Record<string, string>
}): EventDescriptor<P> {
  const normalized = def.tags
    ? {
        ...def,
        tags: (payload: z.infer<P>): Tag[] => {
          const result = def.tags!(payload)
          if (Array.isArray(result)) return result
          return tagsFromRecord(result)
        },
      }
    : def
  return { kind: "event" as const, version: normalized.version ?? "1.0", ...normalized }
}

/**
 * Creates a query descriptor.
 *
 * Without result schema:
 * ```
 * const GetCourse = query({
 *   name: qn("university", "GetCourseView"),
 *   payload: z.object({ courseId: z.string() }),
 * })
 * ```
 *
 * With result schema (typed return):
 * ```
 * const GetCourse = query({
 *   name: qn("university", "GetCourseView"),
 *   payload: z.object({ courseId: z.string() }),
 *   result: z.object({ courseId: z.string(), name: z.string() }),
 * })
 * // queryGateway.query(GetCourse, { courseId: "cs-101" }) → Promise<{ courseId: string, name: string }>
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

export function query(def: any): QueryDescriptor {
  return { kind: "query" as const, version: def.version ?? "1.0", ...def }
}
