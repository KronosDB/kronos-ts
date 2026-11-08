import { ScenarioAssertionError } from "./diff.js"
import { openSite, resolveOptions, type ScenarioOptions, type Scope, type Site, type Subject } from "./site.js"
import {
  type Action,
  type Assertion,
  type Duration,
  type EventValue,
  type Outcome,
  type QueryValue,
  result as resultOf,
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
// builders are VALUES — every step returns a new one, so a half-built scenario
// can be named and finished several ways — and `then` is where one runs.
// ---------------------------------------------------------------------------

type Step =
  | { readonly kind: "given"; readonly events: ReadonlyArray<EventValue<any>> }
  | { readonly kind: "advance"; readonly duration: Duration }
  | { readonly kind: "when"; readonly action: Action }

/** Before the act: history, time passing, then the act. */
export type ScenarioStart = {
  /** Facts that already happened. Repeatable; the facts accumulate in order. */
  given(...events: EventValue<any>[]): ScenarioStart
  /** Move the clock forward — deadlines armed before it fire here. Repeatable. */
  advance(duration: Duration): ScenarioStart
  /** The act: a command, a query, or an event arriving. */
  when(action: Action): ScenarioActed
}

/** After the act: time may pass, then `then` runs the scenario and judges it. */
export type ScenarioActed = {
  /** Move the clock after the act — deadlines fire here. Repeatable. */
  advance(duration: Duration): ScenarioActed
  /**
   * Run the scenario and judge these claims — each a function of the
   * {@link Outcome}. Resolves with the outcome; rejects with the failure.
   *
   * Awaiting the builder WITHOUT `then` runs it with no claims, which is the
   * thenable a `then` method makes inevitable — it is harmless rather than a
   * hang: the scenario runs, an unclaimed throw rejects, and the await
   * resolves with the outcome.
   */
  then(...assertions: Assertion[]): Judged
}

/**
 * A running scenario: a promise of its outcome, and `and` — the same world,
 * continued. Written in one chain, so every step after the first happens in
 * what the previous ones left behind:
 *
 * ```ts
 * await scenario(courses)
 *   .when(command(SubscribeStudent, first)).then(events(event(StudentSubscribed, first)))
 *   .and.when(command(SubscribeStudent, second)).then(error("full"))
 * ```
 */
export type Judged = Promise<Outcome> & {
  /** Continue in the world this scenario left behind. */
  readonly and: ScenarioStart
}

/**
 * A scenario about `subject` — a slice's three lists, or a composition root
 * that is a function of the resources.
 *
 * ```ts
 * await scenario(addItem)
 *   .given(event(ItemAdded, a))
 *   .when(command(AddItem, b))
 *   .then(events(event(ItemAdded, b)))
 * ```
 *
 * `options` overrides what `configure` set for this scenario only.
 */
export function scenario(subject: Subject | Scope, options?: ScenarioOptions): ScenarioStart {
  return start({ subject, options, prior: undefined }, [])
}

/** Where a builder's run happens: a fresh site, or the one a previous step left behind. */
type Lineage = {
  readonly subject: Subject | Scope
  readonly options: ScenarioOptions | undefined
  readonly prior: Run | undefined
}

/** A step that has run (or is running), with the site it ran on. */
type Run = {
  readonly done: Promise<Site>
  continued: boolean
}

function start(lineage: Lineage, steps: ReadonlyArray<Step>): ScenarioStart {
  return {
    given: (...events) => start(lineage, [...steps, { kind: "given", events }]),
    advance: (duration) => start(lineage, [...steps, { kind: "advance", duration }]),
    when: (action) => acted(lineage, [...steps, { kind: "when", action }]),
  }
}

function acted(lineage: Lineage, steps: ReadonlyArray<Step>): ScenarioActed {
  return {
    advance: (duration) => acted(lineage, [...steps, { kind: "advance", duration }]),
    then(...assertions: Assertion[]): Judged {
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

function runJudged(lineage: Lineage, steps: ReadonlyArray<Step>, assertions: ReadonlyArray<Assertion>): Judged {
  const run: Run = {
    done: undefined as unknown as Promise<Site>,
    continued: false,
  }
  let resolveSite!: (site: Site) => void
  let rejectSite!: (error: unknown) => void
  ;(run as { done: Promise<Site> }).done = new Promise<Site>((resolve, reject) => {
    resolveSite = resolve
    rejectSite = reject
  })
  // A step after a failed one never runs; nobody has to handle this rejection.
  run.done.catch(() => {})

  const outcome = (async (): Promise<Outcome> => {
    let site: Site | undefined
    try {
      site = lineage.prior ? await lineage.prior.done : await openSite(lineage.subject, resolveOptions(lineage.options))
      const judged = await perform(site, steps, assertions)
      // Hand the world on if a step was chained onto this one, close it if not.
      // The chain is built synchronously, so by now `continued` is settled.
      if (run.continued) resolveSite(site)
      else await site.close()
      return judged
    } catch (error) {
      rejectSite(error)
      if (site !== undefined) await site.close().catch(() => {})
      throw error
    }
  })()

  let continuation: ScenarioStart | undefined
  return Object.defineProperty(outcome, "and", {
    get(): ScenarioStart {
      run.continued = true
      // The chain is awaited at its END: a failure here reaches the caller as
      // the last step's rejection, so this step's own promise is not left
      // unhandled.
      outcome.catch(() => {})
      continuation ??= start({ ...lineage, prior: run }, [])
      return continuation
    },
  }) as Judged
}

/** Drive the site through the steps, then judge the claims. */
async function perform(site: Site, steps: ReadonlyArray<Step>, assertions: ReadonlyArray<Assertion>): Promise<Outcome> {
  site.window()
  let acted:
    | {
        result: unknown
        threw: boolean
        thrown: unknown
        actIdentifier: string | undefined
      }
    | undefined
  let act: Action | undefined
  for (const step of steps) {
    if (step.kind === "given") {
      if (act !== undefined) {
        throw new Error("@kronos-ts/test: `given` after `when` — history comes before the act.")
      }
      await site.history(step.events)
    } else if (step.kind === "advance") {
      await site.advance(step.duration)
    } else {
      act = step.action
      acted = await site.act(step.action)
    }
  }
  if (act === undefined || acted === undefined) {
    throw new Error("@kronos-ts/test: this scenario has no `when` — there is nothing to run.")
  }

  const claimsError = assertions.some((a) => a.claimsError === true)
  if (acted.threw && !claimsError) throw acted.thrown

  const description = describe(steps, assertions)
  const deadline = Date.now() + site.within
  for (;;) {
    const outcome: Outcome = {
      act: act.kind,
      result: acted.result,
      threw: acted.threw,
      error: acted.thrown,
      ...site.observe(acted.actIdentifier),
      app: site.app,
    }
    const failures = await judge(assertions, outcome)
    if (failures.length === 0) return outcome
    // In memory the world is settled: what is not true now will not become true.
    // A foreign site keeps working where it cannot be watched, so it gets `within`.
    if (!site.foreign || Date.now() > deadline) {
      throw new ScenarioAssertionError([`${description}\n`, ...failures].join("\n"))
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 20))
    await site.quiesce()
  }
}

/** Every claim, judged; the failures as text. A usage error is thrown, not reported. */
async function judge(assertions: ReadonlyArray<Assertion>, outcome: Outcome): Promise<string[]> {
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

/**
 * One read model walked through its states: what it shows, the events that
 * happen, what it shows then — each state judged once the events before it are
 * handled. A storyline is awaited as it is; there is no `then` to write.
 *
 * ```ts
 * await storyline(cartItems, query(GetCartItems, { aggregateId }))
 *   .shows({ ok: true, aggregateId, items: [], totalPrice: 0 })
 *   .after(event(ItemAdded, a)).shows({ ok: true, aggregateId, items: [a], totalPrice: 10 })
 * ```
 *
 * `shows` takes the expected answer (deep-strict, `any()` holes) or any
 * {@link Assertion}, judged with the query as the act.
 */
export type Storyline = PromiseLike<void> & {
  shows(expected: unknown): Storyline
  after(...events: EventValue<any>[]): Storyline
}

export function storyline(subject: Subject | Scope, read: QueryValue<any, any>, options?: ScenarioOptions): Storyline {
  return beats(subject, read, options, [])
}

function beats(
  subject: Subject | Scope,
  read: QueryValue<any, any>,
  options: ScenarioOptions | undefined,
  so_far: ReadonlyArray<Beat>,
): Storyline {
  return {
    shows: (expected) => beats(subject, read, options, [...so_far, { kind: "shows", expected }]),
    after: (...events) => beats(subject, read, options, [...so_far, { kind: "after", events }]),
    then(onFulfilled, onRejected) {
      return tell(subject, read, options, so_far).then(onFulfilled, onRejected)
    },
  }
}

async function tell(
  subject: Subject | Scope,
  read: QueryValue<any, any>,
  options: ScenarioOptions | undefined,
  story: ReadonlyArray<Beat>,
): Promise<void> {
  const site = await openSite(subject, resolveOptions(options))
  try {
    const told: string[] = []
    for (const [index, beat] of story.entries()) {
      if (beat.kind === "after") {
        for (const e of beat.events) {
          site.window()
          const arrived = await site.act(e)
          if (arrived.threw) throw arrived.thrown
        }
        told.push(`after ${beat.events.map((e) => e.descriptor.name.name).join(", ")}`)
        continue
      }
      const check: Assertion =
        typeof beat.expected === "function" ? (beat.expected as Assertion) : resultOf(beat.expected)
      const deadline = Date.now() + site.within
      for (;;) {
        site.window()
        const asked = await site.act(read)
        if (asked.threw) throw asked.thrown
        const outcome: Outcome = {
          act: "query",
          result: asked.result,
          threw: false,
          error: undefined,
          ...site.observe(asked.actIdentifier),
          app: site.app,
        }
        const failures = await judge([check], outcome)
        if (failures.length === 0) break
        if (!site.foreign || Date.now() > deadline) {
          const where = told.length === 0 ? "at the start" : told.join(", ")
          throw new ScenarioAssertionError(
            [`storyline of ${read.descriptor.name.name}, beat ${index + 1} (${where})\n`, ...failures].join("\n"),
          )
        }
        await new Promise<void>((resolve) => setTimeout(resolve, 20))
      }
      told.push(`shows`)
    }
  } finally {
    await site.close()
  }
}

// ── the sentence ───────────────────────────────────────────────────────────

/** The scenario as a sentence, at the top of every failure. Derived, so it cannot drift. */
function describe(steps: ReadonlyArray<Step>, assertions: ReadonlyArray<Assertion>): string {
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
