import { qualifiedNameToString } from "@kronos-ts/core"
import type {
  App,
  CommandDescriptor,
  CommandMessage,
  EventDescriptor,
  EventMessage,
  InferOutput,
  Metadata,
  QueryDescriptor,
  StandardSchemaV1,
} from "@kronos-ts/core"
import {
  compare,
  indent,
  judgeCancelled,
  judgeError,
  judgeMessages,
  judgeScheduled,
  preview,
  ScenarioAssertionError,
} from "./diff.js"
import type { Duration } from "./clock.js"
import type { ScheduleRecord } from "./recording.js"

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

export type { Duration } from "./clock.js"

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

/** The act a scenario performs: a command, a query, or an event arriving. */
export type Action = CommandValue<any, any> | QueryValue<any, any> | EventValue<any>

// ── the outcome ────────────────────────────────────────────────────────────

/**
 * What a scenario's act did, and the app it did it in — the one argument every
 * assertion takes.
 *
 * `events` and `commands` cover THE ACT ONLY: `given` is history and is never in
 * them, and the act's own command is not among `commands`.
 */
export type Outcome = {
  /** What the act was. */
  readonly act: "command" | "query" | "event"
  /** A command handler's return, or a query's answer. `undefined` for an event act or a throw. */
  readonly result: unknown
  /** Whether the act threw. */
  readonly threw: boolean
  /** What it threw, when it did. */
  readonly error: unknown
  /** Events appended during the act, automations included. */
  readonly events: ReadonlyArray<EventMessage>
  /** Commands dispatched during the act, the act's own command excluded. */
  readonly commands: ReadonlyArray<CommandMessage>
  /** The running app the fixture was handed. What a check reaches for when the built-ins are not enough. */
  readonly app: App
}

/**
 * The outcome in an app whose log holds its deadlines against a clock
 * (`recordingEventStore(store, { clock })`): what the act did to them as well.
 */
export type TimedOutcome = Outcome & {
  /** Deadlines armed, fired or cancelled during the act. */
  readonly schedules: ReadonlyArray<ScheduleRecord>
}

// ── assertions ─────────────────────────────────────────────────────────────

/**
 * Anything `then` accepts: a function of the {@link Outcome}. It returns when the
 * claim holds and throws when it does not.
 *
 * The built-ins below are exactly this, and so is anything you write — an
 * assertion is not a special kind of value, which is the whole extension story:
 *
 * ```ts
 * const rejected = (code: string) => ({ result }: Outcome) =>
 *   expect(result).toMatchObject({ ok: false, rejection: code })
 *
 * .then(rejected("CART_FULL"), noEvents())
 * .then(async ({ app }) => expect(await app.db.select().from(carts)).toHaveLength(1))
 * ```
 *
 * Two optional properties a function may carry: `label`, the words it adds to
 * the scenario's sentence, and `claimsError: true`, which says it judges a
 * throw — without one, a throw nobody claimed surfaces as itself, because the
 * real error is what the reader needs.
 */
export type Assertion<O extends Outcome = Outcome> = ((outcome: O) => void | Promise<void>) & {
  readonly label?: string
  readonly claimsError?: boolean
}

function assertion<O extends Outcome = Outcome>(
  label: string,
  check: (outcome: O) => void,
  claimsError = false,
): Assertion<O> {
  return Object.assign(check, claimsError ? { label, claimsError } : { label })
}

function fail(text: string | undefined): void {
  if (text !== undefined) throw new ScenarioAssertionError(text)
}

const nameOf = (descriptor: { name: { namespace: string; name: string } }) => qualifiedNameToString(descriptor.name)
const localName = (descriptor: { name: { name: string } }) => descriptor.name.name

/**
 * Exactly these events were appended, in this order — nothing missing, nothing
 * extra. Payloads are deep-strict with {@link any} holes; metadata, when an
 * expectation names some, is a subset claim.
 */
export function events(...expected: EventValue<any>[]): Assertion {
  const label = expected.length === 0 ? "no events" : expected.map((e) => localName(e.descriptor)).join(", ")
  return assertion(label, ({ events }) =>
    fail(
      judgeMessages(
        "events",
        "appended",
        expected.map((e) => ({ value: e, name: nameOf(e.descriptor) })),
        events.map((e) => ({ name: qualifiedNameToString(e.name), payload: e.payload, metadata: e.metadata })),
      ),
    ),
  )
}

/** Nothing was appended. The empty case of {@link events}, said out loud. */
export function noEvents(): Assertion {
  return events()
}

/** Exactly these commands were dispatched by the act's handlers, in this order. */
export function commands(...expected: CommandValue<any, any>[]): Assertion {
  const label = expected.length === 0 ? "no commands" : expected.map((c) => localName(c.descriptor)).join(", ")
  return assertion(label, ({ commands }) =>
    fail(
      judgeMessages(
        "commands",
        "dispatched",
        expected.map((c) => ({ value: c, name: nameOf(c.descriptor) })),
        commands.map((c) => ({ name: qualifiedNameToString(c.name), payload: c.payload, metadata: c.metadata })),
      ),
    ),
  )
}

/** Nothing was dispatched — what an automation test is often about. */
export function noCommands(): Assertion {
  return commands()
}

/**
 * What the act answered: a command handler's return, or a query's answer —
 * deep-strict, with {@link any} holes.
 *
 * Meaningless for an event act (an event that arrives answers nobody), and it
 * says so rather than comparing against `undefined`.
 */
export function result(value: unknown): Assertion {
  return assertion("a result", ({ act, result: actual }) => {
    if (act === "event") {
      throw new Error(
        "@kronos-ts/test: `result(…)` claims the act's answer, but the act is an EVENT arriving — " +
          "an event answers nobody. Assert what it CAUSED instead: the events it led to, or the " +
          "commands its automations dispatched.",
      )
    }
    const differences = compare(value, actual, "")
    if (differences.length === 0) return
    fail(
      `the ${act === "query" ? "query's answer" : "command's result"} is not what \`result()\` claimed.\n` +
        `  expected: ${preview(value)}\n` +
        `  actual:   ${preview(actual)}\n` +
        `  differences:\n${indent(differences, 4)}`,
    )
  })
}

/**
 * How {@link error} claims the throw.
 *
 * - `string` — the message CONTAINS it. A substring, not an equality: messages
 *   carry ids and counts and a test should not pin those.
 * - `RegExp` — the message matches it.
 * - a predicate — anything else, `(e) => e instanceof CourseFull` included.
 */
export type ErrorMatcher = string | RegExp | ((error: unknown) => boolean)

/** The act threw, and this is how. Claiming the throw is what declines it. */
export function error(matcher: ErrorMatcher): Assertion {
  return assertion("an error", ({ threw, error: thrown }) => fail(judgeError(matcher, { threw, thrown })), true)
}

/**
 * An event was ARMED to fire `after` from the instant the handler armed it.
 *
 * The delay is relative on purpose: a test knows it asked for "thirty seconds
 * from now", and it does not know — and should not have to compute — which
 * absolute millisecond the handler ran at.
 *
 * A claim about deadlines, so only a fixture over a clocked log takes it: its
 * outcome is a {@link TimedOutcome}.
 */
export function scheduled(event: EventValue<any>, after: Duration): Assertion<TimedOutcome> {
  return assertion<TimedOutcome>(`${localName(event.descriptor)} scheduled after ${after}ms`, ({ schedules }) =>
    fail(judgeScheduled(event, after, heldDeadlines(schedules, "scheduled"))),
  )
}

/** A schedule for this event was cancelled before it fired. Like {@link scheduled}, for a clocked log only. */
export function cancelled(event: EventValue<any>): Assertion<TimedOutcome> {
  return assertion<TimedOutcome>(`${localName(event.descriptor)} cancelled`, ({ schedules }) =>
    fail(judgeCancelled(event, heldDeadlines(schedules, "cancelled"))),
  )
}

/** The deadlines the outcome carries — refused, for JavaScript and casts, when the log holds none. */
function heldDeadlines(schedules: ReadonlyArray<ScheduleRecord> | undefined, claim: string): ReadonlyArray<ScheduleRecord> {
  if (schedules !== undefined) return schedules
  throw new Error(
    `@kronos-ts/test: \`${claim}(…)\` claims a deadline, but the app's log holds none it can see. ` +
      "Give the recorder a clock: `recordingEventStore(store, { clock })`.",
  )
}
