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

/** One thing that happens, in order. */
export type Step =
  | { readonly kind: "given"; readonly events: ReadonlyArray<EventValue<any>> }
  | { readonly kind: "wait"; readonly duration: Duration }
  | { readonly kind: "when"; readonly action: Action }

/**
 * A finished scenario: the steps, the claims, and a sentence describing itself.
 *
 * Data — no methods, nothing captured, nothing bound to a fixture. Which is the
 * point: the same scenario runs against an in-memory scope and against real
 * infrastructure, and it is the same test both times.
 */
export type Scenario = {
  readonly steps: ReadonlyArray<Step>
  readonly then: ReadonlyArray<Assertion>
  /** "given CourseCreated, when SubscribeStudent, then StudentSubscribed" */
  readonly description: string
}

/** Before the act: time may pass, then exactly one thing happens. */
export type ScenarioStart = {
  /** Let `duration` of the fixture's time pass. Repeatable. */
  wait(duration: Duration): ScenarioStart
  /** The act — exactly one command, query or event. */
  when(action: Action): ScenarioActed
}

/** After the act: time may pass, then the claims close the scenario. */
export type ScenarioActed = {
  /** Let `duration` pass — deadlines fire here. Repeatable. */
  wait(duration: Duration): ScenarioActed
  /** What the act should have done. Terminal. */
  then(...assertions: Assertion[]): Scenario
}

/**
 * A scenario with no history: the world starts empty.
 *
 * ```ts
 * scenario().when(command(CreateCourse, { courseId: "cs-101" }))
 *           .then(event(CourseCreated, { courseId: "cs-101" }))
 * ```
 */
export function scenario(): ScenarioStart {
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
export function given(...events: EventValue<any>[]): ScenarioStart {
  return start(events.length === 0 ? [] : [{ kind: "given", events }])
}

function start(steps: ReadonlyArray<Step>): ScenarioStart {
  return {
    wait(duration) {
      return start([...steps, { kind: "wait", duration }])
    },
    when(action) {
      return acted([...steps, { kind: "when", action }])
    },
  }
}

function acted(steps: ReadonlyArray<Step>): ScenarioActed {
  return {
    wait(duration) {
      return acted([...steps, { kind: "wait", duration }])
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
    } else if (step.kind === "wait") {
      clauses.push(`wait ${step.duration}ms`)
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
