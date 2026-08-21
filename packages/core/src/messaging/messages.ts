// THE DECLARATION VOCABULARY — one file, because it is one subject.
//
// A qualified name, a metadata map, a message and the descriptor that declares
// one are not four topics that happen to live near each other: they are the
// single answer to "what is a message, before any kind picks it up". Splitting
// them across five files made five imports of one idea, and every reader had to
// reassemble it. The pieces that genuinely stand alone stayed alone — `tag.ts`,
// `identifier.ts`, `serialized-error.ts` — because each is one small thing with
// no view of the rest.
//
// This file imports from NO activity folder. `messaging/` is the bottom of the
// dependency order, which is the rule that settles every "where does this live"
// argument: anything the declaration vocabulary needs is itself vocabulary, and
// moves down here.

import type { Tag } from "./tag.js"
import { tag, tagsFromRecord } from "./tag.js"
import type { InferOutput, StandardSchemaV1 } from "./standard-schema.js"

// ═══════════════════════════════════════════════════════════════════════════
// QUALIFIED NAMES — how a message is addressed
// ═══════════════════════════════════════════════════════════════════════════

/**
 * A structured message name consisting of a namespace and a local name.
 * Aligns with Axon Server's wire format where messages are routed by name.
 *
 * The namespace typically represents the bounded context or module,
 * while the name identifies the specific message type.
 */
export type QualifiedName = {
  readonly namespace: string
  readonly name: string
}

/**
 * Creates a {@link QualifiedName} from a namespace and name.
 */
export function qn(namespace: string, name: string): QualifiedName {
  return { namespace, name }
}

/**
 * Serializes a {@link QualifiedName} to its wire format: `"namespace.name"`.
 */
export function qualifiedNameToString(qn: QualifiedName): string {
  return `${qn.namespace}.${qn.name}`
}

/**
 * Parses a dot-separated string into a {@link QualifiedName}.
 * Splits on the last dot, so `"a.b.c"` becomes `{ namespace: "a.b", name: "c" }`.
 */
export function qualifiedNameFromString(fqn: string): QualifiedName {
  const lastDot = fqn.lastIndexOf(".")
  if (lastDot === -1) {
    return qn("", fqn)
  }
  return qn(fqn.slice(0, lastDot), fqn.slice(lastDot + 1))
}

/**
 * Returns true if two {@link QualifiedName}s are equal.
 */
export function qualifiedNamesEqual(a: QualifiedName, b: QualifiedName): boolean {
  return a.namespace === b.namespace && a.name === b.name
}

// ═══════════════════════════════════════════════════════════════════════════
// METADATA — what a message carries besides its payload
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Message metadata — a read-only string-keyed map of arbitrary values.
 * Carries cross-cutting information like correlation IDs, trace context,
 * user identity, etc.
 *
 * Metadata is immutable — all transformation methods return new instances.
 */
export type Metadata = Readonly<Record<string, unknown>>

/**
 * Well-known metadata keys used by the framework.
 */
export const MetadataKeys = {
  CORRELATION_ID: "correlationId",
  CAUSATION_ID: "causationId",
  TRACE_ID: "traceId",
} as const

/**
 * Creates an empty metadata object.
 */
export function emptyMetadata(): Metadata {
  return {}
}

/**
 * Creates metadata with a single entry.
 */
export function metadataWith(key: string, value: unknown): Metadata {
  return { [key]: value }
}

/**
 * Merges two metadata objects. Values from `override` take precedence.
 */
export function mergeMetadata(base: Metadata, override: Metadata): Metadata {
  return { ...base, ...override }
}

/**
 * Returns new metadata with the given entry added (or replaced).
 */
export function metadataAnd(metadata: Metadata, key: string, value: unknown): Metadata {
  return { ...metadata, [key]: value }
}

/**
 * Returns new metadata with the given entry added only if not already present.
 */
export function metadataAndIfNotPresent(metadata: Metadata, key: string, supplier: () => unknown): Metadata {
  if (key in metadata) return metadata
  return { ...metadata, [key]: supplier() }
}

/**
 * Returns new metadata with the specified keys removed.
 */
export function metadataWithoutKeys(metadata: Metadata, ...keys: string[]): Metadata {
  const result: Record<string, unknown> = { ...metadata }
  for (const key of keys) {
    delete result[key]
  }
  return result
}

/**
 * Returns new metadata containing only the specified keys.
 */
export function metadataSubset(metadata: Metadata, ...keys: string[]): Metadata {
  const result: Record<string, unknown> = {}
  for (const key of keys) {
    if (key in metadata) {
      result[key] = metadata[key]
    }
  }
  return result
}

/**
 * Check if metadata contains a specific key.
 */
export function metadataContains(metadata: Metadata, key: string): boolean {
  return key in metadata
}

// ═══════════════════════════════════════════════════════════════════════════
// MESSAGES — the three kinds, and what they have in common
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Discriminates a message by its dispatch category.
 *
 * TypeScript erases interfaces at runtime, so `CommandMessage`, `EventMessage`,
 * and `QueryMessage` — which are otherwise shape-identical — cannot be told
 * apart with `instanceof` the way Axon Framework's nominal interfaces can.
 * This field is the structural-typing equivalent of that `instanceof` check:
 * it lets a reusable handler interceptor branch on message category without
 * being pinned to a single bus.
 */
export type MessageKind = "command" | "event" | "query"

/**
 * A message carrying a payload and metadata, identified by a unique ID
 * and routed by its qualified name.
 *
 * `kind` is derived at construction/reconstruction time, never persisted —
 * each bus and event-store reconstruction site sets it from context.
 *
 * `timestamp` is OPTIONAL, and its absence means one specific thing: this
 * message has not been through a task yet. The instant belongs to the TASK
 * that handles the message, and at the moment an edge verb builds one — `send`,
 * `query`, `subscriptionQuery` — the task does not exist: the bus mints the
 * unit of work, and a unit of work is what carries the clock. So the verb
 * builds everything it can know and the bus fills in the one field only the
 * task can supply, from `uow.now()`. A transport, having no task, fills it from
 * system time at the wire, because an envelope crossing a process boundary must
 * be fully formed.
 *
 * Which is why {@link EventMessage} narrows it back to REQUIRED. An event is a
 * FACT — something that already happened — and a fact you can read has an
 * instant, always. Only a command or a query can be briefly in flight without
 * one.
 */
export type Message<P = unknown> = {
  readonly kind: MessageKind
  readonly identifier: string
  readonly name: QualifiedName
  readonly payload: P
  readonly metadata: Metadata
  readonly timestamp?: number
}

/**
 * A command message — dispatched to exactly one handler, may return a result.
 */
export type CommandMessage<P = unknown> = Message<P> & {
  readonly kind: "command"
}

/**
 * A command result message — the response from handling a command.
 */
export type CommandResultMessage<R = unknown> = {
  readonly identifier: string
  readonly payload: R | undefined
  readonly metadata: Metadata
  readonly error?: Error
}

/**
 * An event message — published to all interested handlers.
 *
 * `timestamp` is REQUIRED here, unlike on the base {@link Message}. An event is
 * a fact: `ctx.append` stamps it from the task's instant at birth, the store
 * writes that instant, and every read hands it back. There is no window in
 * which an event exists without one.
 */
export type EventMessage<P = unknown> = Message<P> & {
  readonly kind: "event"
  readonly version: string
  readonly timestamp: number
  readonly tags: ReadonlyArray<{ readonly key: string; readonly value: string }>
}

/**
 * The message a descriptor describes, with its payload narrowed to what the
 * descriptor's schema infers. INTERNAL — it is the return of {@link is} and
 * nothing else, and a name a host never writes is a name a host should not have
 * to know.
 *
 * The three arms are the three kinds, discriminated on the descriptor's own
 * `kind` field: the same discriminant the runtime check reads, so the type and
 * the check can never drift apart.
 */
type MessageFor<D extends MessageDescriptor> =
  D extends { kind: "command"; payload: infer P extends StandardSchemaV1 }
    ? CommandMessage<InferOutput<P>>
    : D extends { kind: "query"; payload: infer P extends StandardSchemaV1 }
      ? QueryMessage<InferOutput<P>>
      : D extends { kind: "event"; payload: infer P extends StandardSchemaV1 }
        ? EventMessage<InferOutput<P>>
        : never

/**
 * Pattern-match a message against a descriptor — the `case` of any function
 * that branches on what a message is.
 *
 * ONE GUARD FOR ALL THREE KINDS, because "is this message that message type" is
 * one question. It matches the descriptor's KIND and its qualified NAME, and —
 * for an event, the only kind that carries a version on the message — its
 * VERSION too. A command or a query is a request in flight rather than a stored
 * fact, so its descriptor's version is declaration-side only and there is
 * nothing on the message to compare it against.
 *
 * It NARROWS to the descriptor's own message type with the payload inferred
 * from its schema, so the branch reads the matched shape fully typed:
 *
 * ```ts
 * if (is(message, CreateCourse)) {
 *   message.payload.courseId       // CommandMessage<{ courseId: string, … }>
 * }
 * ```
 *
 * Declare an outdated event version as its own descriptor and the compiler
 * knows what `payload` looked like back then — which is what makes an upcaster
 * a plain typed switch:
 *
 * ```ts
 * const upcast: Upcast = (e) => {
 *   if (is(e, CourseCreatedV1)) {
 *     return { ...e, version: CourseCreated.version, payload: { ...e.payload, capacity: 30 } }
 *   }
 *   return e
 * }
 * ```
 */
export function is<D extends MessageDescriptor>(
  message: Message,
  descriptor: D,
): message is MessageFor<D> {
  if (message.kind !== descriptor.kind) return false
  if (!qualifiedNamesEqual(message.name, descriptor.name)) return false
  // Only an event carries a version on the message itself.
  return message.kind !== "event" || (message as EventMessage).version === descriptor.version
}

/**
 * An event as a processor delivers it — the fact, plus where it sat in the
 * stream when the source has a position. `timestamp` is required, inherited
 * from {@link EventMessage}: this is a value read back out of a log.
 */
export type SequencedEventMessage<P = unknown> = EventMessage<P> & {
  /** Stream position when the source has one; absent for push-only delivery. */
  readonly sequence?: bigint
}

/**
 * A query message — dispatched to handler(s) that can answer it.
 */
export type QueryMessage<P = unknown> = Message<P> & {
  readonly kind: "query"
}

/**
 * Fill in a message's instant if whoever built it could not.
 *
 * INTERNAL. It is not exported from the barrel and there is no type for "a
 * message without an instant" any more, because neither was ever a concept a
 * host had to hold: a bus takes a message, and if that message has no instant
 * the bus supplies one from the task it just minted. Nothing about WHEN the
 * instant is settled changed — only that the vocabulary for it is gone.
 *
 * Idempotent, which is what lets a wrapping bus and the bus it wraps both call
 * it without either having to know whether the other already did.
 */
export function withInstant<M extends Message>(message: M, instant: () => number): M {
  return message.timestamp === undefined
    ? { ...message, timestamp: instant() }
    : message
}

// ═══════════════════════════════════════════════════════════════════════════
// DESCRIPTORS — declaring a message type
//
// The payload, result and id constraints are STANDARD SCHEMA, not any one
// library's type: whatever puts a `~standard` property on its schemas works
// here, which today means zod, valibot, arktype and a growing list. Core
// depends on none of them. See `./standard-schema.ts`.
// ═══════════════════════════════════════════════════════════════════════════

/**
 * The result type a descriptor's `result` schema promises. `unknown` when the
 * descriptor declares none — the honest answer, not `void`.
 */
export type InferResult<R extends StandardSchemaV1 | undefined> =
  R extends StandardSchemaV1 ? InferOutput<R> : unknown

/**
 * Describes a command message type — its name, payload schema,
 * and optional result schema for typed gateway returns.
 */
export type CommandDescriptor<
  P extends StandardSchemaV1 = StandardSchemaV1,
  R extends StandardSchemaV1 | undefined = undefined,
> = {
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
export type TagExtractors<P extends StandardSchemaV1 = StandardSchemaV1> = Record<
  string,
  (payload: InferOutput<P>) => string
>

/**
 * Describes an event message type — its name, payload schema, and tag derivation.
 * Tags define how events are indexed for query-based sourcing.
 */
export type EventDescriptor<P extends StandardSchemaV1 = StandardSchemaV1> = {
  readonly kind: "event"
  readonly name: QualifiedName
  readonly version: string
  readonly payload: P
  readonly tags?: (payload: InferOutput<P>) => Tag[]
  /**
   * The tag KEYS every instance of this event carries — the event's half of the
   * DCB query, known WITHOUT a payload in hand.
   *
   * State query derivation intersects this with the state's own tag record to
   * scope each event type to the tags it can actually be matched on (see
   * `event-sourcing/state.ts`). That intersection is only sound if this list is
   * exhaustive and payload-independent.
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
export type QueryDescriptor<
  P extends StandardSchemaV1 = StandardSchemaV1,
  R extends StandardSchemaV1 | undefined = undefined,
> = {
  readonly kind: "query"
  readonly name: QualifiedName
  /** Version of the query. Default: "1.0". */
  readonly version: string
  readonly payload: P
  /** Optional result schema — enables typed return from `query(bus, …)`. */
  readonly result?: R
}

/**
 * Any message descriptor.
 *
 * Widened to `<any>` on purpose, and for the same reason the `kronos` entry
 * types are: a descriptor CARRIES A FUNCTION — an event's `tags` extractor takes
 * the payload — so its parameter is checked contravariantly and a CONCRETELY
 * typed descriptor is not assignable to the defaulted one. `EventDescriptor<{
 * courseId: string }>`'s extractor cannot stand in for `(payload: unknown) =>
 * Tag[]`, and a `result` schema cannot stand in for the `undefined` default,
 * which between them ruled out every descriptor anybody actually declares.
 *
 * This is the CONSTRAINT, never the value: every function taking a descriptor
 * takes it as `D extends MessageDescriptor` and reads the real payload type back
 * off `D`, so `is(message, CourseCreated)` narrows to the exact payload and
 * `validate(CreateCourse, body)` returns the exact parsed object.
 */
export type MessageDescriptor =
  | CommandDescriptor<any, any>
  | EventDescriptor<any>
  | QueryDescriptor<any, any>

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
export function command<P extends StandardSchemaV1>(def: {
  name: QualifiedName
  version?: string
  payload: P
  routingKey?: string
}): CommandDescriptor<P, undefined>

export function command<P extends StandardSchemaV1, R extends StandardSchemaV1>(def: {
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
export function event<P extends StandardSchemaV1>(def: {
  name: QualifiedName
  version?: string
  payload: P
  tags?: TagExtractors<P> | ((payload: InferOutput<P>) => Tag[] | Record<string, string>)
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
      tags: (payload: InferOutput<P>): Tag[] =>
        extractors.map(([key, extract]) => tag(key, extract(payload))),
      tagKeys: extractors.map(([key]) => key),
    }
  }

  const tags: ((payload: InferOutput<P>) => Tag[]) | undefined = rawTags
    ? (payload: InferOutput<P>): Tag[] => {
        const result = (rawTags as (p: InferOutput<P>) => Tag[] | Record<string, string>)(payload)
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
export function queryDescriptor<P extends StandardSchemaV1>(def: {
  name: QualifiedName
  version?: string
  payload: P
}): QueryDescriptor<P, undefined>

export function queryDescriptor<P extends StandardSchemaV1, R extends StandardSchemaV1>(def: {
  name: QualifiedName
  version?: string
  payload: P
  result: R
}): QueryDescriptor<P, R>

export function queryDescriptor(def: any): QueryDescriptor {
  return { kind: "query" as const, version: def.version ?? "1.0", ...def }
}

// ═══════════════════════════════════════════════════════════════════════════
// NAMESPACES — the same three constructors, with the namespace already said
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Creates a namespace-scoped factory for message descriptors.
 * Reduces repetition when defining many messages in the same bounded context.
 *
 * What it returns is DERIVED, never hand-declared: the three functions are
 * exactly the three constructors above with their first argument closed over,
 * so their inference is the same inference and there is no second shape to keep
 * in step.
 *
 * ```typescript
 * const ns = withNamespace("university.courses")
 *
 * const CreateCourse = ns.command("CreateCourse", {
 *   payload: z.object({ courseId: z.string(), name: z.string() }),
 * })
 *
 * const CourseCreated = ns.event("CourseCreated", {
 *   payload: z.object({ courseId: z.string(), name: z.string() }),
 *   tags: (p) => [tag("courseId", p.courseId)],
 * })
 *
 * const GetCourse = ns.query("GetCourse", {
 *   payload: z.object({ courseId: z.string() }),
 * })
 * ```
 */
export function withNamespace(namespace: string) {
  function scopedCommand<P extends StandardSchemaV1>(name: string, def: {
    payload: P
    version?: string
    routingKey?: string
  }): ReturnType<typeof command<P>>
  function scopedCommand<P extends StandardSchemaV1, R extends StandardSchemaV1>(name: string, def: {
    payload: P
    result: R
    version?: string
    routingKey?: string
  }): ReturnType<typeof command<P, R>>
  function scopedCommand(name: string, def: { payload: StandardSchemaV1 }) {
    return command({ name: qn(namespace, name), ...def } as never)
  }

  function scopedQuery<P extends StandardSchemaV1>(name: string, def: {
    payload: P
    version?: string
  }): ReturnType<typeof queryDescriptor<P>>
  function scopedQuery<P extends StandardSchemaV1, R extends StandardSchemaV1>(name: string, def: {
    payload: P
    result: R
    version?: string
  }): ReturnType<typeof queryDescriptor<P, R>>
  function scopedQuery(name: string, def: { payload: StandardSchemaV1 }) {
    return queryDescriptor({ name: qn(namespace, name), ...def } as never)
  }

  return {
    /** Create a command descriptor in this namespace. */
    command: scopedCommand,

    /** Create an event descriptor in this namespace. */
    event<P extends StandardSchemaV1>(name: string, def: {
      payload: P
      version?: string
      tags?: TagExtractors<P> | ((payload: InferOutput<P>) => Tag[] | Record<string, string>)
      tagKeys?: readonly string[]
    }) {
      return event({ name: qn(namespace, name), ...def })
    },

    /** Create a query descriptor in this namespace. */
    query: scopedQuery,
  }
}
