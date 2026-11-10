import type { App } from "@kronos-ts/core"
import type { Duration } from "./clock.js"
import { ScenarioAssertionError } from "./diff.js"
import { openWorld, type RecordedApp, type World } from "./fixture.js"
import type { ScheduleRecording } from "./recording.js"
import {
  type Action,
  type Assertion,
  type EventValue,
  type Outcome,
  type QueryValue,
  result as resultOf,
  type TimedOutcome,
} from "./values.js"

// ---------------------------------------------------------------------------
// The grammar, in the shapes event models are written in.
//
//   command:    given(events) → when(command) → then(events | the answer)
//   view:       given(events) → when(query)   → then(the read model)
//   automation: given(events) → when(event)   → then(commands)
//   storyline:  a read model, shown → after(events) → shown → …
//
// `given` is history: projections fold it, automations do not act on it. The
// builders are VALUES — every step returns a new one — and `then` is where one
// runs, in the world its fixture was handed.
// ---------------------------------------------------------------------------

type Step =
  | { readonly kind: "given"; readonly events: ReadonlyArray<EventValue<any>> }
  | { readonly kind: "advance"; readonly duration: Duration }
  | { readonly kind: "when"; readonly action: Action }

/** True when the outcome is timed: the app's log holds its deadlines against a clock. */
type Timed<O extends Outcome> = [O] extends [TimedOutcome] ? true : false

/** Before the act: history, time passing, then the act. */
export type ScenarioStart<O extends Outcome = Outcome> = Timed<O> extends true
  ? BeforeAct<O> & {
      /** Move the clock forward — deadlines armed before it fire here. Repeatable. */
      advance(duration: Duration): ScenarioStart<O>
    }
  : BeforeAct<O>

/** What every scenario can do before its act, timed or not. */
export type BeforeAct<O extends Outcome = Outcome> = {
  /** Facts that already happened. Repeatable; the facts accumulate in order. */
  given(...events: EventValue<any>[]): ScenarioStart<O>
  /** The act: a command, a query, or an event arriving. */
  when(action: Action): ScenarioActed<O>
}

/** After the act: time may pass, then `then` runs the scenario and judges it. */
export type ScenarioActed<O extends Outcome = Outcome> = Timed<O> extends true
  ? AfterAct<O> & {
      /** Move the clock after the act — deadlines fire here. Repeatable. */
      advance(duration: Duration): ScenarioActed<O>
    }
  : AfterAct<O>

/** What every scenario can do after its act, timed or not. */
export type AfterAct<O extends Outcome = Outcome> = {
  /**
   * Run the scenario and judge these claims — each a function of the
   * {@link Outcome}. Resolves with the outcome; rejects with the failure.
   *
   * Awaiting the builder WITHOUT `then` runs it with no claims, which is the
   * thenable a `then` method makes inevitable — it is harmless rather than a
   * hang: the scenario runs, an unclaimed throw rejects, and the await
   * resolves with the outcome.
   */
  then(...assertions: Assertion<O>[]): Judged<O>
}

/**
 * A running scenario: a promise of its outcome, and `and` — the same world,
 * continued. Written in one chain, so every step after the first happens in
 * what the previous ones left behind:
 *
 * ```ts
 * await fixture(app)
 *   .when(command(SubscribeStudent, first)).then(events(event(StudentSubscribed, first)))
 *   .and.when(command(SubscribeStudent, second)).then(error("full"))
 * ```
 */
export type Judged<O extends Outcome = Outcome> = Promise<O> & {
  /** Continue in the world this scenario left behind. */
  readonly and: ScenarioStart<O>
}

/**
 * One read model walked through its states: what it shows, the events that
 * happen, what it shows then — each state judged once the events before it are
 * handled. A storyline is awaited as it is; there is no `then` to write.
 *
 * `shows` takes the expected answer (deep-strict, `any()` holes) or any
 * {@link Assertion}, judged with the query as the act.
 */
export type Storyline<O extends Outcome = Outcome> = PromiseLike<void> & {
  shows(expected: unknown): Storyline<O>
  after(...events: EventValue<any>[]): Storyline<O>
}

/** A fixture: the scenario start, plus storylines. */
export type Fixture<O extends Outcome = Outcome> = ScenarioStart<O> & {
  /** A storyline of the read model this query asks for. */
  storyline(read: QueryValue<any, any>): Storyline<O>
}

/** The store an app reports, as a type. */
type StoreOf<A> = A extends App<infer S, any, any> ? S : never

/**
 * The outcome a fixture of this app judges: timed when its log holds deadlines
 * against a clock. An app with no log at all is not timed.
 */
export type OutcomeOf<A> = [StoreOf<A>] extends [never]
  ? Outcome
  : [StoreOf<A>] extends [ScheduleRecording]
    ? TimedOutcome
    : Outcome

/**
 * Scenarios against a running app, built the way `main.ts` builds it with the
 * infrastructure wrapped once at the top — the log and the command bus in
 * recorders, and the log given the clock the app's tasks read:
 *
 * ```ts
 * const clock = advanceableClock()
 * const uow = () => unitOfWork(clock)
 * const eventStore = recordingEventStore(inMemoryEventStore(), { clock })
 * const commandBus = recordingCommandBus(interceptingCommandBus(localCommandBus(uow), correlation))
 * const queryBus = interceptingQueryBus(localQueryBus(uow), correlation)
 * const app = kronos({ commandHandlers: […], queryHandlers: […], eventHandlers: […] })
 *
 * await fixture(app)
 *   .given(event(ItemAdded, a))
 *   .when(command(AddItem, b))
 *   .then(events(event(ItemAdded, b)))
 * ```
 *
 * The fixture reads the log and the buses off the app (`app.eventStores`,
 * `app.commandBuses`, `app.queryBuses`). Its TYPE refuses an app whose log or
 * command bus is not recorded, and offers `advance` — and takes `scheduled` and
 * `cancelled` — only when the log was given a clock. An app with more than one
 * store or bus of a kind is refused when the scenario runs: build the fixture's
 * app from the configuration of the module under test.
 *
 * ONE FIXTURE, MANY SCENARIOS. Build the app once — per spec file or suite, of
 * the whole app or of one slice — call `fixture(app)` once, and start every
 * scenario from it. The first takes the app over; each after it runs in what
 * the ones before it left, one at a time:
 * - each scenario sees only what ITS act did (its own observation window);
 * - nothing is reset: the log, the processors' positions and the read models
 *   carry on, so scenarios stay apart by minting their own ids;
 * - the clock is the test's own and carries on too;
 * - each scenario starts with an empty deadline book — a deadline lives through
 *   its scenario and that scenario's `.and` steps, never into the next one.
 *
 * The fixture never stops the app: the test does, with whatever else it opened
 * (a database to drop), in its runner's hooks. The app may be handed as a
 * promise, for one whose resources are opened asynchronously.
 */
export function fixture<A extends RecordedApp>(app: A | Promise<A>): Fixture<OutcomeOf<A>> {
  const home: Home = { app, world: undefined, turn: Promise.resolve() }
  const lineage: Lineage = { home, prior: undefined }
  return {
    ...start(lineage, []),
    storyline: (read: QueryValue<any, any>) => beats(home, read, []),
  } as unknown as Fixture<OutcomeOf<A>>
}

/** The world a fixture stands for, opened by its first scenario and shared by all of them. */
type Home = {
  readonly app: App | Promise<App>
  world: Promise<World> | undefined
  /** Settles when the scenario that holds the world lets go of it. */
  turn: Promise<void>
}

/** Where a builder's run happens: a new scenario on its fixture, or the step before it. */
type Lineage = {
  readonly home: Home
  readonly prior: Run | undefined
}

/** A world held by one scenario, and how that scenario lets go of it. */
type Session = {
  readonly world: World
  /** Hand the world to the next scenario. Absent once let go. */
  release: (() => void) | undefined
}

/** A step that has run (or is running), with the session it ran in. */
type Run = {
  readonly done: Promise<Session>
  continued: boolean
}

/**
 * Wait for the scenarios before this one to finish, then take the world — opened
 * on first use. Scenarios on one fixture run one at a time even when the runner
 * starts them together, because they share the recorders their windows read.
 */
async function take(home: Home): Promise<Session> {
  const previous = home.turn
  let release!: () => void
  home.turn = new Promise<void>((resolve) => {
    release = resolve
  })
  await previous
  try {
    home.world ??= Promise.resolve(home.app).then(openWorld)
    return { world: await home.world, release }
  } catch (error) {
    release()
    throw error
  }
}

/** Let go of the world, if this session still holds it. */
function letGo(session: Session): void {
  session.release?.()
  session.release = undefined
}

// The builders below are written once, for every outcome: the TYPES above decide
// which of their steps a fixture offers.
type AnyStart = ScenarioStart<TimedOutcome>
type AnyActed = ScenarioActed<TimedOutcome>

function start(lineage: Lineage, steps: ReadonlyArray<Step>): AnyStart {
  return {
    given: (...events) => start(lineage, [...steps, { kind: "given", events }]),
    advance: (duration) => start(lineage, [...steps, { kind: "advance", duration }]),
    when: (action) => acted(lineage, [...steps, { kind: "when", action }]),
  }
}

function acted(lineage: Lineage, steps: ReadonlyArray<Step>): AnyActed {
  return {
    advance: (duration) => acted(lineage, [...steps, { kind: "advance", duration }]),
    then(...assertions: Assertion<TimedOutcome>[]): Judged<TimedOutcome> {
      // The promise machinery calls `then(resolve, reject)` when a builder is
      // awaited without its claims. Those are engine callbacks, not claims:
      // run with none and settle through them.
      if (isPromiseResolution(assertions)) {
        const [resolve, reject] = assertions as unknown as [(v: unknown) => void, (e: unknown) => void]
        const judged = runJudged(lineage, steps, [])
        judged.then(resolve, reject)
        return judged
      }
      return runJudged(lineage, steps, assertions)
    },
  }
}

function runJudged(
  lineage: Lineage,
  steps: ReadonlyArray<Step>,
  assertions: ReadonlyArray<Assertion<TimedOutcome>>,
): Judged<TimedOutcome> {
  let resolveSession!: (session: Session) => void
  let rejectSession!: (error: unknown) => void
  const run: Run = {
    done: new Promise<Session>((resolve, reject) => {
      resolveSession = resolve
      rejectSession = reject
    }),
    continued: false,
  }
  // A step after a failed one never runs; nobody has to handle this rejection.
  run.done.catch(() => {})

  const outcome = (async (): Promise<TimedOutcome> => {
    let session: Session | undefined
    try {
      session = await sessionFor(lineage)
      const judged = await perform(session.world, steps, assertions)
      // Hand the world on to a step chained onto this one, or let the next
      // scenario have it. The chain is built synchronously, so by now
      // `continued` is settled; a step chained later takes its turn again.
      if (!run.continued) letGo(session)
      resolveSession(session)
      return judged
    } catch (error) {
      if (session !== undefined) letGo(session)
      rejectSession(error)
      throw error
    }
  })()

  let continuation: AnyStart | undefined
  return Object.defineProperty(outcome, "and", {
    get(): AnyStart {
      run.continued = true
      // The chain is awaited at its END: a failure here reaches the caller as
      // the last step's rejection, so this step's own promise is not left
      // unhandled.
      outcome.catch(() => {})
      continuation ??= start({ ...lineage, prior: run }, [])
      return continuation
    },
  }) as Judged<TimedOutcome>
}

/**
 * The session a step runs in: a new scenario takes its turn and starts with an
 * empty deadline book; a chained step continues the one before it — taking a
 * turn of its own when that one already let go.
 */
async function sessionFor(lineage: Lineage): Promise<Session> {
  if (lineage.prior === undefined) {
    const session = await take(lineage.home)
    session.world.begin()
    return session
  }
  const prior = await lineage.prior.done
  if (prior.release !== undefined) return prior
  const again = await take(lineage.home)
  return again
}

/** Drive the world through the steps, then judge the claims. */
async function perform(
  world: World,
  steps: ReadonlyArray<Step>,
  assertions: ReadonlyArray<Assertion<TimedOutcome>>,
): Promise<TimedOutcome> {
  world.window()
  let acted: Awaited<ReturnType<World["act"]>> | undefined
  let act: Action | undefined
  for (const step of steps) {
    if (step.kind === "given") {
      if (act !== undefined) {
        throw new Error("@kronos-ts/test: `given` after `when` — history comes before the act.")
      }
      await world.history(step.events)
    } else if (step.kind === "advance") {
      await world.advance(step.duration)
    } else {
      act = step.action
      acted = await world.act(step.action)
    }
  }
  if (act === undefined || acted === undefined) {
    throw new Error("@kronos-ts/test: this scenario has no `when` — there is nothing to run.")
  }

  const claimsError = assertions.some((a) => a.claimsError === true)
  if (acted.threw && !claimsError) throw acted.thrown

  const outcome = outcomeOf(world, act.kind, acted)
  const failures = await judge(assertions, outcome)
  if (failures.length === 0) return outcome
  throw new ScenarioAssertionError([`${describe(steps, assertions)}\n`, ...failures].join("\n"))
}

/**
 * The outcome as the world recorded it. `schedules` is present only when the
 * log holds deadlines; the type a fixture hands its claims says the same.
 */
function outcomeOf(world: World, act: Action["kind"], acted: Awaited<ReturnType<World["act"]>>): TimedOutcome {
  const { schedules, ...observed } = world.observe(acted.actIdentifier)
  return {
    act,
    result: acted.result,
    threw: acted.threw,
    error: acted.thrown,
    ...observed,
    ...(schedules !== undefined ? { schedules } : {}),
    app: world.app,
  } as TimedOutcome
}

/** Every claim, judged; the failures as text. A usage error is thrown, not reported. */
async function judge(assertions: ReadonlyArray<Assertion<TimedOutcome>>, outcome: TimedOutcome): Promise<string[]> {
  const failures: string[] = []
  for (const assertion of assertions) {
    try {
      await assertion(outcome)
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("@kronos-ts/test:")) throw error
      failures.push(error instanceof Error ? error.message : String(error))
    }
  }
  return failures
}

// ── storyline ──────────────────────────────────────────────────────────────

type Beat =
  | { readonly kind: "shows"; readonly expected: unknown }
  | { readonly kind: "after"; readonly events: ReadonlyArray<EventValue<any>> }

function beats(home: Home, read: QueryValue<any, any>, so_far: ReadonlyArray<Beat>): Storyline {
  return {
    shows: (expected) => beats(home, read, [...so_far, { kind: "shows", expected }]),
    after: (...events) => beats(home, read, [...so_far, { kind: "after", events }]),
    then(onFulfilled, onRejected) {
      return tell(home, read, so_far).then(onFulfilled, onRejected)
    },
  }
}

async function tell(home: Home, read: QueryValue<any, any>, story: ReadonlyArray<Beat>): Promise<void> {
  const session = await take(home)
  const { world } = session
  world.begin()
  try {
    const told: string[] = []
    for (const [index, beat] of story.entries()) {
      if (beat.kind === "after") {
        for (const e of beat.events) {
          world.window()
          const arrived = await world.act(e)
          if (arrived.threw) throw arrived.thrown
        }
        told.push(`after ${beat.events.map((e) => e.descriptor.name.name).join(", ")}`)
        continue
      }
      const check: Assertion<TimedOutcome> =
        typeof beat.expected === "function" ? (beat.expected as Assertion<TimedOutcome>) : resultOf(beat.expected)
      world.window()
      const asked = await world.act(read)
      if (asked.threw) throw asked.thrown
      const failures = await judge([check], outcomeOf(world, "query", asked))
      if (failures.length > 0) {
        const where = told.length === 0 ? "at the start" : told.join(", ")
        throw new ScenarioAssertionError(
          [`storyline of ${read.descriptor.name.name}, beat ${index + 1} (${where})\n`, ...failures].join("\n"),
        )
      }
      told.push("shows")
    }
  } finally {
    letGo(session)
  }
}

// ── the sentence ───────────────────────────────────────────────────────────

/** The scenario as a sentence, at the top of every failure. Derived, so it cannot drift. */
function describe(steps: ReadonlyArray<Step>, assertions: ReadonlyArray<Assertion<any>>): string {
  const clauses: string[] = []
  for (const step of steps) {
    if (step.kind === "given") clauses.push(`given ${step.events.map((e) => e.descriptor.name.name).join(", ")}`)
    else if (step.kind === "advance") clauses.push(`advance ${step.duration}ms`)
    else clauses.push(`when ${step.action.descriptor.name.name}`)
  }
  clauses.push(
    assertions.length === 0
      ? "then nothing is claimed"
      : `then ${assertions.map((a) => a.label ?? (a.name || "a check")).join(", ")}`,
  )
  return clauses.join(", ")
}

/** `then(resolve, reject)` from the promise machinery, not two claims. */
function isPromiseResolution(args: ReadonlyArray<unknown>): boolean {
  return (
    args.length === 2 &&
    args.every((f) => typeof f === "function" && Function.prototype.toString.call(f).includes("[native code]"))
  )
}
