import type {
  CommandDescriptor,
  EventDescriptor,
  InferOutput,
  Metadata,
  QueryDescriptor,
  StandardSchemaV1,
} from "@kronos-ts/core"

// ---------------------------------------------------------------------------
// The vocabulary a scenario is written in.
//
// Everything here is a PURE value constructor: it allocates a record and
// returns it. Nothing touches a store, a bus or a clock — which is what makes a
// scenario a value you can write down, name, share between fixtures and
// generate, rather than a script that only means something while it runs.
//
// `event`, `command` and `query` name a MESSAGE. The same three name an
// EXPECTATION, because "the event StudentSubscribed with this payload" is one
// idea whether you are stating it as history or claiming it as a consequence,
// and giving it two spellings would only ask the reader which one they were
// looking at.
// ---------------------------------------------------------------------------

/** Milliseconds. The unit a clock reads and `timestamp` carries. */
export type Duration = number

// ── the payload hole ───────────────────────────────────────────────────────

/**
 * A payload, or part of one, the assertion declines to pin.
 *
 * It exists because some fields are not the test's business — a generated id, a
 * timestamp inside a payload, a nested blob some other module owns — and the
 * alternative is either restating them (which pins what you did not mean to
 * pin) or dropping down to `expect()` on the raw record (which loses the diff).
 * Renders as `*` in a failure.
 */
export type Any = {
  readonly kind: "any"
  /** When given, the value must also validate against this schema. */
  readonly schema?: StandardSchemaV1
}

/**
 * A hole in an expected payload — positionally, exactly where the payload goes.
 *
 * ```ts
 * then(event(OrderPlaced, any()))                       // some OrderPlaced
 * then(event(OrderPlaced, { orderId: "o-1", at: any() }))   // this one, whenever
 * then(event(OrderPlaced, { orderId: any(z.uuid()) }))   // any Standard Schema
 * ```
 *
 * It is an ASSERTION value. A hole in a `given` fact would put a sentinel into
 * the event store, so the fixture rejects one there with an error saying so.
 */
export function any(schema?: StandardSchemaV1): Any {
  return schema === undefined ? { kind: "any" } : { kind: "any", schema }
}

/**
 * True for a value produced by {@link any}.
 *
 * Shape-checked down to its key set, so a DOMAIN payload that happens to have a
 * `kind: "any"` field of its own is compared as the payload it is rather than
 * silently swallowing everything it was compared against.
 */
export function isAny(value: unknown): value is Any {
  if (typeof value !== "object" || value === null) return false
  if ((value as { kind?: unknown }).kind !== "any") return false
  return Object.keys(value).every((key) => key === "kind" || key === "schema")
}

/**
 * A payload with holes allowed anywhere: the descriptor's own payload type, with
 * {@link Any} admitted at every position.
 *
 * A payload that does not fit its descriptor is still a compile error — the hole
 * widens what may appear, it does not switch checking off.
 */
export type Expected<T> =
  | Any
  | (T extends ReadonlyArray<infer E>
      ? ReadonlyArray<Expected<E>>
      : T extends object
        ? { readonly [K in keyof T]: Expected<T[K]> }
        : T)

// ── message values ─────────────────────────────────────────────────────────

/** A named event with a payload: a past fact, or an expected consequence. */
export type EventValue<P extends StandardSchemaV1 = StandardSchemaV1> = {
  readonly kind: "event"
  readonly descriptor: EventDescriptor<P>
  readonly payload: unknown
  /** Present only when the scenario said something about metadata. */
  readonly metadata?: Metadata
}

/** A named command with a payload: an act, or an expected dispatch. */
export type CommandValue<
  P extends StandardSchemaV1 = StandardSchemaV1,
  R extends StandardSchemaV1 | undefined = any,
> = {
  readonly kind: "command"
  readonly descriptor: CommandDescriptor<P, R>
  readonly payload: unknown
  readonly metadata?: Metadata
}

/** A named query with a payload. Only ever an act — a read is not a consequence. */
export type QueryValue<
  P extends StandardSchemaV1 = StandardSchemaV1,
  R extends StandardSchemaV1 | undefined = any,
> = {
  readonly kind: "query"
  readonly descriptor: QueryDescriptor<P, R>
  readonly payload: unknown
  readonly metadata?: Metadata
}

/**
 * An event: a fact for `given`, an arrival for `when`, an expectation for `then`.
 *
 * `metadata` is what the edge stamps when this is an act, and a SUBSET claim
 * when it is an expectation — the keys given must match, the ones not mentioned
 * are not looked at, because nobody wants a test to fail over a `causationId`
 * they never asked about.
 */
export function event<P extends StandardSchemaV1>(
  descriptor: EventDescriptor<P>,
  payload: Expected<InferOutput<P>>,
  metadata?: Metadata,
): EventValue<P> {
  return metadata === undefined
    ? { kind: "event", descriptor, payload }
    : { kind: "event", descriptor, payload, metadata }
}

/** A command: the act for `when`, or a dispatch expected in `then`. */
export function command<P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined = undefined>(
  descriptor: CommandDescriptor<P, R>,
  payload: Expected<InferOutput<P>>,
  metadata?: Metadata,
): CommandValue<P, R> {
  return metadata === undefined
    ? { kind: "command", descriptor, payload }
    : { kind: "command", descriptor, payload, metadata }
}

/** A query: the act for `when`. */
export function query<P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined = undefined>(
  descriptor: QueryDescriptor<P, R>,
  payload: Expected<InferOutput<P>>,
  metadata?: Metadata,
): QueryValue<P, R> {
  return metadata === undefined
    ? { kind: "query", descriptor, payload }
    : { kind: "query", descriptor, payload, metadata }
}

/** The one thing a scenario does. Exactly one of the three. */
export type Action = CommandValue<any, any> | QueryValue<any, any> | EventValue<any>

// ── assertion values ───────────────────────────────────────────────────────

/** The act's answer: a command handler's return, or a query's result. */
export type ResultAssertion = {
  readonly kind: "result"
  readonly value: unknown
}

/**
 * What the act answered — deep, with {@link any} holes.
 *
 * Meaningless for an event act (an event that arrives answers nobody), and the
 * fixture says so rather than comparing against `undefined`.
 */
export function result(value: unknown): ResultAssertion {
  return { kind: "result", value }
}

/**
 * How `error` claims the throw.
 *
 * - `string` — the message CONTAINS it. A substring, not an equality: messages
 *   carry ids and counts and a test should not pin those.
 * - `RegExp` — the message matches it.
 * - a predicate — anything else, `(e) => e instanceof CourseFull` included.
 *   One escape hatch instead of a list of special cases.
 */
export type ErrorMatcher = string | RegExp | ((error: unknown) => boolean)

export type ErrorAssertion = {
  readonly kind: "error"
  readonly matcher: ErrorMatcher
}

/**
 * The act threw, and this is how.
 *
 * Asserting an error also DECLINES the throw: without it, a throw the scenario
 * did not claim surfaces as itself rather than as an events diff, because the
 * real error is what the reader needs.
 */
export function error(matcher: ErrorMatcher): ErrorAssertion {
  return { kind: "error", matcher }
}

export type NoEventsAssertion = {
  readonly kind: "no-events"
}

/** Nothing was appended. The empty case of the event list, said out loud. */
export function noEvents(): NoEventsAssertion {
  return { kind: "no-events" }
}

export type NoCommandsAssertion = {
  readonly kind: "no-commands"
}

/** Nothing was dispatched — the assertion an automation test is usually about. */
export function noCommands(): NoCommandsAssertion {
  return { kind: "no-commands" }
}

export type ScheduledAssertion = {
  readonly kind: "scheduled"
  readonly event: EventValue<any>
  readonly after: Duration
}

/**
 * An event was ARMED to fire `after` from the instant the handler armed it.
 *
 * The delay is relative on purpose: a test knows it asked for "thirty seconds
 * from now", and it does not know — and should not have to compute — which
 * absolute millisecond the handler ran at.
 */
export function scheduled(event: EventValue<any>, after: Duration): ScheduledAssertion {
  return { kind: "scheduled", event, after }
}

export type CancelledAssertion = {
  readonly kind: "cancelled"
  readonly event: EventValue<any>
}

/** A schedule for this event was cancelled before it fired. */
export function cancelled(event: EventValue<any>): CancelledAssertion {
  return { kind: "cancelled", event }
}

/**
 * Anything `then` accepts.
 *
 * Each CATEGORY is judged only if the scenario mentions it: events, commands,
 * the result, the throw, the schedules. Say nothing about a category and it is
 * not looked at — which is what lets one scenario be about an automation and
 * another about a return value without either restating the other's world.
 */
export type Assertion =
  | EventValue<any>
  | CommandValue<any, any>
  | ResultAssertion
  | ErrorAssertion
  | NoEventsAssertion
  | NoCommandsAssertion
  | ScheduledAssertion
  | CancelledAssertion
