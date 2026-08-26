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
  | { readonly kind: "when"; readonly action: Action }

/**
 * HOW THE CLAIMS ARE JUDGED — once, or until they hold.
 *
 * `then` judges the world as it stands the moment the act settles, which is
 * everything a deterministic scope needs: if a claim does not hold now it will
 * not hold later either, and waiting would be theatre.
 *
 * `await` judges the same claims REPEATEDLY until they hold or the deadline
 * passes. That is the real-infrastructure shape — a projection behind a
 * database, a processor on another node — where the act returning does not
 * mean the world has finished reacting. There is nothing extra to write: what
 * you are waiting for is what you were going to assert anyway.
 */
export type Judgement = "once" | "until"

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
  /** Whether the claims are judged once, or until they hold. */
  readonly judgement: Judgement
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
  /** The act — exactly one command, query or event. */
  when(action: Action): ScenarioActed<A>
}

/** After the act: time may pass, then the claims close the scenario. */
export type ScenarioActed<A extends Advances = false> = {
  /** Move the clock — deadlines fire here. Repeatable. */
  advance(duration: Duration): ScenarioActed<true>
  /** What the act should have done, judged once. Terminal. */
  then(...assertions: Assertion[]): Scenario<A>
  /**
   * What the act should EVENTUALLY have done — the same claims, re-judged
   * until they hold or `run`'s `within` passes. Terminal.
   *
   * For a world that keeps working after the act returns: a projection behind
   * a database, a processor on another node. Moves no clock, so any fixture
   * runs it.
   */
  await(...assertions: Assertion[]): Scenario<A>
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
    then(...assertions) {
      return {
        steps,
        then: assertions,
        judgement: "once",
        description: describe(steps, assertions),
      }
    },
    await(...assertions) {
      return {
        steps,
        then: assertions,
        judgement: "until",
        description: `${describe(steps, assertions)} (eventually)`,
      }
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
