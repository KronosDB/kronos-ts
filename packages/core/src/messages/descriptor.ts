import type { z } from "zod"
import type { QualifiedName } from "../primitives/qualified-name.js"
import type { Tag } from "../primitives/tag.js"
import { qualifiedNameToString } from "../primitives/qualified-name.js"
import { tag, tagsFromRecord } from "../primitives/tag.js"

/**
 * The result type a descriptor's `result` schema promises. `unknown` when the
 * descriptor declares none — the honest answer, not `void`.
 */
export type InferResult<R extends z.ZodType | undefined> =
  R extends z.ZodType ? z.infer<R> : unknown

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
  /** Version of the command. Default: "1.0". */
  readonly version: string
  readonly payload: P
  /** Optional result schema — enables typed return from `send(bus, …)`. */
  readonly result?: R
  /**
   * The payload field that contains the routing key for distributed command routing.
   *
   * Used by the command gateway to extract the routing key before dispatch.
   * Commands with the same routing key are routed to the same handler instance.
   */
  readonly routingKey?: string
}

/**
 * How an event derives its tags, as a record of EXTRACTORS: the record's own
 * keys ARE the tag keys, and each value pulls that tag's value off the payload.
 *
 * ```typescript
 * tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId }
 * ```
 *
 * This shape exists so the tag KEYS are statically evident to the framework
 * without running anything — see {@link EventDescriptor.tagKeys}.
 */
export type TagExtractors<P extends z.ZodType = z.ZodType> = Record<
  string,
  (payload: z.infer<P>) => string
>

/**
 * Describes an event message type — its name, payload schema, and tag derivation.
 * Tags define how events are indexed for query-based sourcing.
 */
export interface EventDescriptor<P extends z.ZodType = z.ZodType> {
  readonly kind: "event"
  readonly name: QualifiedName
  readonly version: string
  readonly payload: P
  readonly tags?: (payload: z.infer<P>) => Tag[]
  /**
   * The tag KEYS every instance of this event carries — the event's half of the
   * DCB query, known WITHOUT a payload in hand.
   *
   * State query derivation intersects this with the state's own tag record to
   * scope each event type to the tags it can actually be matched on (see
   * `packages/modelling/src/state.ts`). That intersection is only sound if this
   * list is exhaustive and payload-independent.
   *
   * `[]` means "carries no tags". `undefined` means NOT KNOWN — the descriptor
   * was given an opaque `tags` FUNCTION and no explicit `tagKeys`. Undefined is
   * never guessed at: a state that folds such an event fails loudly at boot
   * rather than deriving a query from an assumed key set.
   */
  readonly tagKeys?: readonly string[]
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
  /** Version of the query. Default: "1.0". */
  readonly version: string
  readonly payload: P
  /** Optional result schema — enables typed return from `query(bus, …)`. */
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
 * // send(commandBus, CreateCourse, { courseId: "cs-101" }) → Promise<{ courseId: string }>
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
 * PREFER the record-of-extractors form — the record's keys are the tag keys, so
 * the framework knows them without running your code, and state queries can be
 * scoped per event type (see {@link EventDescriptor.tagKeys}):
 *
 * ```typescript
 * event({
 *   name: qn("university", "StudentSubscribedToCourse"),
 *   payload: z.object({ courseId: z.string(), studentId: z.string() }),
 *   tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
 * })
 * ```
 *
 * A FUNCTION returning `Tag[]` or `Record<string, string>` is still accepted for
 * tag sets a per-key extractor cannot express — a key that varies with the
 * payload, or a variable number of tags. The keys of a function are not
 * knowable, so declare them alongside it whenever a state folds this event:
 *
 * ```typescript
 * event({
 *   name: qn("catalog", "ItemsRelabelled"),
 *   payload: z.object({ items: z.array(z.string()) }),
 *   tags: (p) => p.items.map((id) => tag("itemId", id)),
 *   tagKeys: ["itemId"],   // not derivable from the function — say it
 * })
 * ```
 */
export function event<P extends z.ZodType>(def: {
  name: QualifiedName
  version?: string
  payload: P
  tags?: TagExtractors<P> | ((payload: z.infer<P>) => Tag[] | Record<string, string>)
  /**
   * The tag keys this event carries. REQUIRED ONLY for the function form, and
   * only when a state folds this event — the record form derives them.
   */
  tagKeys?: readonly string[]
}): EventDescriptor<P> {
  const rawTags = def.tags

  if (rawTags !== undefined && typeof rawTags !== "function") {
    // Record-of-extractors: the keys are right there, no inference needed.
    const extractors = Object.entries(rawTags as TagExtractors<P>)
    if (def.tagKeys !== undefined) {
      throw new Error(
        `event(${qualifiedNameToString(def.name)}): \`tagKeys\` was given alongside a \`tags\` record. ` +
        "The record's own keys ARE the tag keys — they cannot disagree, so remove `tagKeys`. " +
        "`tagKeys` is only for the `tags` FUNCTION form, whose keys cannot be derived.",
      )
    }
    return {
      kind: "event" as const,
      name: def.name,
      version: def.version ?? "1.0",
      payload: def.payload,
      tags: (payload: z.infer<P>): Tag[] =>
        extractors.map(([key, extract]) => tag(key, extract(payload))),
      tagKeys: extractors.map(([key]) => key),
    }
  }

  const tags: ((payload: z.infer<P>) => Tag[]) | undefined = rawTags
    ? (payload: z.infer<P>): Tag[] => {
        const result = (rawTags as (p: z.infer<P>) => Tag[] | Record<string, string>)(payload)
        return Array.isArray(result) ? result : tagsFromRecord(result)
      }
    : undefined

  // No `tags` at all means this event carries none — that IS a known key set
  // (the empty one), not an unknown one. A `tags` function without `tagKeys` is
  // genuinely unknown, and stays `undefined` so it can fail loudly at the point
  // a state actually needs it.
  const tagKeys: readonly string[] | undefined = def.tagKeys ?? (tags ? undefined : [])

  return {
    kind: "event" as const,
    name: def.name,
    version: def.version ?? "1.0",
    payload: def.payload,
    ...(tags ? { tags } : {}),
    ...(tagKeys ? { tagKeys } : {}),
  }
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
 * // query(queryBus, GetCourse, { courseId: "cs-101" }) → Promise<{ courseId: string, name: string }>
 * ```
 */
export function queryDescriptor<P extends z.ZodType>(def: {
  name: QualifiedName
  version?: string
  payload: P
}): QueryDescriptor<P, undefined>

export function queryDescriptor<P extends z.ZodType, R extends z.ZodType>(def: {
  name: QualifiedName
  version?: string
  payload: P
  result: R
}): QueryDescriptor<P, R>

export function queryDescriptor(def: any): QueryDescriptor {
  return { kind: "query" as const, version: def.version ?? "1.0", ...def }
}
