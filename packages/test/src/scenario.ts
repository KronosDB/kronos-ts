import type { CommandMessage, EventMessage } from "@kronos-ts/core"
import type { Action, Assertion, Duration, EventValue } from "./values.js"

// ---------------------------------------------------------------------------
// A scenario is a VALUE, built by a chain that never mutates.
//
// The chain exists for ONE reason: order is real here. `given` is before,
// `when` is the act, `wait` is time passing at a named point, `then` is after —
// and a record of four fields cannot say whether the wait came before or after
// the act. So the joints are written as a pipe, and every step returns a NEW
// value, which is what lets a half-built scenario be named and finished several
// different ways.
//
// The TYPES carry the grammar. There is exactly one `when` because `when`
// returns a shape that has no `when`; `then` is terminal because it returns a
// Scenario, which is data and has no methods at all.
// ---------------------------------------------------------------------------

/**
 * What a step asks of the fixture's clock.
 *
 * `advance` is the only thing that needs one it can MOVE, and moving a clock is
 * a capability the fixture has or does not — so it rides in the scenario's TYPE
 * and a fixture that cannot move time refuses the scenario rather than throwing
 * when it reaches the step.
 */
export type Advances = boolean

/** One thing that happens, in order. */
export type Step =
  | { readonly kind: "given"; readonly events: ReadonlyArray<EventValue<any>> }
  | { readonly kind: "advance"; readonly duration: Duration }
  | { readonly kind: "await"; readonly until?: Settled; readonly within?: Duration }
  | { readonly kind: "when"; readonly action: Action }

/**
 * What `await` waits FOR — a claim about the world, re-judged until it holds.
 *
 * Absent, `await` waits for the processors to catch up and nothing more, which
 * is all an in-memory scope ever needs. Against real infrastructure there is no
 * "caught up" anybody can observe from outside, so you say what you are waiting
 * for and the fixture keeps looking until it is true or the deadline passes.
 */
export type Settled = (observed: {
  readonly events: ReadonlyArray<EventMessage>
  readonly commands: ReadonlyArray<CommandMessage>
}) => boolean | Promise<boolean>

/**
 * A finished scenario: the steps, the claims, and a sentence describing itself.
 *
 * Data — no methods, nothing captured, nothing bound to a fixture. Which is the
 * point: the same scenario runs against an in-memory scope and against real
 * infrastructure, and it is the same test both times.
 */
export type Scenario<A extends Advances = boolean> = {
  /** Phantom: whether any step moves the clock. Never read at runtime. */
  readonly advances?: A
  readonly steps: ReadonlyArray<Step>
  readonly then: ReadonlyArray<Assertion>
  /** "given CourseCreated, when SubscribeStudent, then StudentSubscribed" */
  readonly description: string
}

/** Before the act: time may pass, then exactly one thing happens. */
export type ScenarioStart<A extends Advances = false> = {
  /**
   * Move the fixture's clock forward by `duration` — deadlines armed before it
   * fire here. Repeatable, and it makes the scenario one only a time-advancing
   * fixture will run.
   */
  advance(duration: Duration): ScenarioStart<true>
  /**
   * Wait for the world to catch up: the processors reach the head of the log,
   * and — when `until` is given — keep looking until it holds. Moves no clock,
   * so any fixture runs it.
   */
  await(until?: Settled, within?: Duration): ScenarioStart<A>
  /** The act — exactly one command, query or event. */
  when(action: Action): ScenarioActed<A>
}

/** After the act: time may pass, then the claims close the scenario. */
export type ScenarioActed<A extends Advances = false> = {
  /** Move the clock — deadlines fire here. Repeatable. */
  advance(duration: Duration): ScenarioActed<true>
  /** Wait for the world to catch up. Moves no clock. */
  await(until?: Settled, within?: Duration): ScenarioActed<A>
  /** What the act should have done. Terminal. */
  then(...assertions: Assertion[]): Scenario<A>
}

/**
 * A scenario with no history: the world starts empty.
 *
 * ```ts
 * scenario().when(command(CreateCourse, { courseId: "cs-101" }))
 *           .then(event(CourseCreated, { courseId: "cs-101" }))
 * ```
 */
export function scenario(): ScenarioStart<false> {
  return start([])
}

/**
 * A scenario whose world already contains these facts.
 *
 * Given events are HISTORY: they go straight into the log and the automations do
 * NOT re-fire for them, because they are the state of the world when the test
 * begins, not things that are happening now.
 *
 * ```ts
 * given(event(CourseCreated, { courseId: "cs-101", capacity: 1 }))
 *   .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "s-1" }))
 *   .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "s-1" }),
 *         command(CloseCourse, { courseId: "cs-101" }))
 * ```
 *
 * `given()` with no facts is the same thing as `scenario()` — spelling the empty
 * world either way is allowed, because a suite that builds its givens from an
 * array should not have to branch when the array is empty.
 */
export function given(...events: EventValue<any>[]): ScenarioStart<false> {
  return start(events.length === 0 ? [] : [{ kind: "given", events }])
}

function start(steps: ReadonlyArray<Step>): ScenarioStart<any> {
  return {
    advance(duration) {
      return start([...steps, { kind: "advance", duration }])
    },
    await(until, within) {
      return start([...steps, { kind: "await", until, within }])
    },
    when(action) {
      return acted([...steps, { kind: "when", action }])
    },
  }
}

function acted(steps: ReadonlyArray<Step>): ScenarioActed<any> {
  return {
    advance(duration) {
      return acted([...steps, { kind: "advance", duration }])
    },
    await(until, within) {
      return acted([...steps, { kind: "await", until, within }])
    },
    then(...assertions) {
      return { steps, then: assertions, description: describe(steps, assertions) }
    },
  }
}

// ── the sentence ───────────────────────────────────────────────────────────

/** The local name of a descriptor — what a reader calls the message. */
function localName(descriptor: { name: { name: string } }): string {
  return descriptor.name.name
}

function describeAssertion(assertion: Assertion): string {
  switch (assertion.kind) {
    case "event":
    case "command":
      return localName(assertion.descriptor)
    case "result":
      return "a result"
    case "error":
      return "an error"
    case "no-events":
      return "no events"
    case "no-commands":
      return "no commands"
    case "scheduled":
      return `${localName(assertion.event.descriptor)} scheduled after ${assertion.after}ms`
    case "cancelled":
      return `${localName(assertion.event.descriptor)} cancelled`
  }
}

/**
 * The scenario, as a sentence.
 *
 * Derived rather than written, so it cannot drift from what the scenario does —
 * which is what makes it usable as a test's NAME, and why `run` puts it at the
 * top of a failure.
 */
function describe(steps: ReadonlyArray<Step>, assertions: ReadonlyArray<Assertion>): string {
  const clauses: string[] = []
  for (const step of steps) {
    if (step.kind === "given") {
      clauses.push(`given ${step.events.map((e) => localName(e.descriptor)).join(", ")}`)
    } else if (step.kind === "advance") {
      clauses.push(`advance ${step.duration}ms`)
    } else if (step.kind === "await") {
      clauses.push(step.until === undefined ? "await" : "await until")
    } else {
      clauses.push(`when ${localName(step.action.descriptor)}`)
    }
  }
  clauses.push(
    assertions.length === 0
      ? "then nothing is claimed"
      : `then ${assertions.map(describeAssertion).join(", ")}`,
  )
  return clauses.join(", ")
}
