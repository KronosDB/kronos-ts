import {
  emptyMetadata,
  eventProcessor,
  generateIdentifier,
  inMemoryDeadLetterQueue,
  inMemoryEventStore,
  inMemorySnapshotStore,
  inMemoryTokenStore,
  kronos,
  qualifiedNameToString,
  simpleCommandBus,
  simpleQueryBus,
  unitOfWork,
} from "@kronos-ts/core"
import type {
  Clock,
  CommandHandlerDefinition,
  CommandHandlerEntry,
  CommandMessage,
  EventHandlerDefinition,
  EventHandlerEntry,
  EventMessage,
  EventProcessor,
  EventScheduler,
  EventStore,
  QueryHandlerDefinition,
  QueryHandlerEntry,
  QueryMessage,
  RunningProcessor,
  SequencedDeadLetterQueue,
  Sited,
  SnapshotStore,
  StateEntry,
  StateModule,
  StateOptions,
  TokenStore,
  UnitOfWork,
  Unstamped,
} from "@kronos-ts/core"
import { evaluate, ScenarioAssertionError, type Observed } from "./diff.js"
import {
  controllableScheduler,
  recordingCommandBus,
  recordingEventStore,
  recordingQueryBus,
} from "./recording.js"
import type { Scenario } from "./scenario.js"
import { isAny, type Action, type Duration, type EventValue } from "./values.js"

// ---------------------------------------------------------------------------
// The fixture: the SITE a scenario runs at.
//
// A scenario says what should happen. The fixture says where. It creates the
// resources — one log, one snapshot cache, one cursor table, one dead-letter
// queue, two buses, one clock, one scheduler — and HANDS THEM to the scope,
// which is a FUNCTION of them. That is the whole inversion: production's
// composition root is also a function of its resources, so a scope written for
// the fixture is a scope you can deploy, and a scope written for production is
// one you can test. Nothing is replaced behind anybody's back.
//
// Everything in here is deterministic. The clock does not tick unless a scenario
// says `wait`; the scheduler has no timer; the processors are driven to the head
// of the log and asked whether they are finished. There are no sleeps, so a suite
// of a thousand scenarios runs in the time a thousand function calls take.
// ---------------------------------------------------------------------------

/**
 * The instant a fixture's clock starts at when nobody says otherwise.
 *
 * A FIXED instant rather than the wall clock, because a timestamp a test can
 * predict is worth more than one that is technically current: an assertion can
 * name `FIXTURE_EPOCH + 30_000` and mean it.
 */
export const FIXTURE_EPOCH = Date.UTC(2024, 0, 1)

/**
 * A processor the scope has DESCRIBED but not built, because the resources it
 * reads from belong to the site.
 *
 * This is the slice idiom, typed: the slice closes out the semantics that are
 * its own — its durable name, its lane, whether it parks poison pills — and
 * leaves the resources as its parameter list. A shorter parameter list is fine;
 * a projection that wants global order and no queue writes
 * `(eventStore, tokenStore, unitOfWork) => eventProcessor({ ... })` and declines
 * the fourth argument by assignability.
 */
export type PartialProcessor = (
  eventStore: EventStore,
  tokenStore: TokenStore,
  unitOfWork: () => UnitOfWork,
  deadLetterQueue: SequencedDeadLetterQueue,
) => EventProcessor

/** An event handler entry as a scope writes one: the processor may still be partial. */
export type FixtureEventHandler = Sited<EventHandlerDefinition<any, any>> & {
  readonly processor?: EventProcessor | PartialProcessor
}

/**
 * What a scope returns: the four lists `kronos` takes.
 *
 * The same four, with the same meanings. A scope is not a special test shape —
 * it is a composition root whose resources arrive as arguments.
 */
export interface FixtureLists {
  readonly commandHandlers?: ReadonlyArray<Sited<CommandHandlerDefinition<any, any>>>
  readonly queryHandlers?: ReadonlyArray<Sited<QueryHandlerDefinition<any, any>>>
  readonly eventHandlers?: ReadonlyArray<FixtureEventHandler>
  readonly states?: ReadonlyArray<StateEntry>
}

/**
 * A composition root as a function of the resources the fixture owns.
 *
 * ```ts
 * const fixture = testFixture((eventStore, snapshotStore) => courses(eventStore, snapshotStore))
 * const fixture = testFixture((eventStore) => ({ states: [{ ...Course, eventStore }], … }))
 * ```
 */
export type FixtureScope = (eventStore: EventStore, snapshotStore: SnapshotStore) => FixtureLists

export interface FixtureOptions {
  /**
   * How long to keep re-judging the claims before calling them failed. Only ever
   * used against a scope that brought resources the fixture does not own — an
   * all-in-memory scope is deterministic, so a claim that does not hold on the
   * first look will not hold on the second either, and waiting would be theatre.
   * Default: 5000ms.
   */
  readonly within?: Duration
  /**
   * Where the fixture's time starts. Absent means {@link FIXTURE_EPOCH} — or
   * system time under `realTime`, where the clock has to be real for a real wait
   * to mean anything.
   */
  readonly clock?: Clock
  /**
   * Make `wait` genuinely elapse instead of jumping the clock. For a scope whose
   * own infrastructure has its own timers — a database scheduler with a polling
   * worker — where there is nothing for the fixture to jump.
   */
  readonly realTime?: boolean
}

/** What one act did. `events` and `commands` cover THIS act only. */
export interface RunOutcome {
  /** A command handler's return, or a query's answer. `undefined` for an event act. */
  readonly result: unknown
  /** Events appended during the act — automations included, `given` excluded. */
  readonly events: ReadonlyArray<EventMessage>
  /** Commands dispatched during the act — the act's own command excluded. */
  readonly commands: ReadonlyArray<Unstamped<CommandMessage>>
}

/**
 * One timeline.
 *
 * Consecutive `run` calls continue the SAME log and the SAME processor cursors,
 * which is how a saga is tested: each call reports only what it caused, and the
 * world it caused it in is whatever the previous calls left behind.
 */
export interface TestFixture {
  run(scenario: Scenario, opts?: { within?: Duration }): Promise<RunOutcome>
}

const DEFAULT_WITHIN = 5000

/** Wire `scope` against resources the fixture owns, and run scenarios at it. */
export function testFixture(scope: FixtureScope, opts: FixtureOptions = {}): TestFixture {
  const realTime = opts.realTime ?? false
  const base: Clock = opts.clock ?? (realTime ? Date.now : () => FIXTURE_EPOCH)
  let offset = 0
  /** The fixture's clock: the base instant, plus whatever `wait` has jumped. */
  const clock: Clock = () => base() + offset
  const uow = (): UnitOfWork => unitOfWork(clock)

  const log = inMemoryEventStore()
  const eventStore = recordingEventStore(log)
  const snapshotStore = inMemorySnapshotStore()
  const tokenStore = inMemoryTokenStore()
  const deadLetterQueue = inMemoryDeadLetterQueue()
  const commandBus = recordingCommandBus(simpleCommandBus(uow))
  const queryBus = recordingQueryBus(simpleQueryBus(uow))
  const eventScheduler = controllableScheduler(clock)

  // ---- what the scope asked for -------------------------------------------
  const lists = scope(eventStore, snapshotStore)

  /**
   * Whether anything in the scope is beyond the fixture's reach.
   *
   * The fixture can only jump a clock it owns and only settle a processor it
   * drives. A scope that brought its own store, its own scheduler or an
   * already-built processor over foreign resources is a REAL-INFRASTRUCTURE
   * scope: `wait` cannot fake time for it, and its claims have to be re-judged
   * until they settle rather than judged once.
   */
  let foreign = false
  function ownStore(store: EventStore | undefined): void {
    if (store !== undefined && store !== eventStore) foreign = true
  }
  function ownScheduler(scheduler: EventScheduler | undefined): void {
    if (scheduler !== undefined && scheduler !== eventScheduler) foreign = true
  }

  const defaultProcessor = eventProcessor({
    name: "fixture",
    eventStore,
    tokenStore,
    unitOfWork: uow,
  })

  /**
   * Which log each delivery reads, by the processor's durable name.
   *
   * Quiescing means "every delivery has reached the head of the log it reads" —
   * and a processor the scope built over its own store reads a DIFFERENT log, so
   * comparing it against the fixture's head would wait forever for events that
   * were never going to arrive there.
   */
  const readsFrom = new Map<string, EventStore>()

  /** Complete a partial processor with the fixture's resources; keep a whole one. */
  function processorFor(entry: FixtureEventHandler): EventProcessor {
    const declared = entry.processor
    if (declared === undefined) return defaultProcessor
    const built =
      typeof declared === "function"
        ? declared(eventStore, tokenStore, uow, deadLetterQueue)
        : declared
    if (built.eventStore !== eventStore || built.tokenStore !== tokenStore) foreign = true
    readsFrom.set(built.name, built.eventStore)
    return built
  }

  function sited(entry: StateEntry): StateEntry {
    const [state, options] = Array.isArray(entry)
      ? (entry as readonly [Sited<StateModule<any, any>>, StateOptions])
      : ([entry as Sited<StateModule<any, any>>, undefined] as const)
    ownStore(state.eventStore)
    const withSite = { eventStore, snapshotStore, ...state }
    return options === undefined ? withSite : [withSite, options]
  }

  const app = kronos({
    states: (lists.states ?? []).map(sited),
    commandHandlers: (lists.commandHandlers ?? []).map((h) => {
      ownStore(h.eventStore)
      ownScheduler(h.eventScheduler)
      return {
        eventStore,
        snapshotStore,
        eventScheduler,
        ...h,
        commandBus,
        queryBus,
      } as CommandHandlerEntry
    }),
    queryHandlers: (lists.queryHandlers ?? []).map((h) => {
      ownStore(h.eventStore)
      return { eventStore, snapshotStore, ...h, queryBus } as QueryHandlerEntry
    }),
    eventHandlers: (lists.eventHandlers ?? []).map((h) => {
      ownStore(h.eventStore)
      ownScheduler(h.eventScheduler)
      return {
        eventScheduler,
        ...h,
        commandBus,
        queryBus,
        // Last, so a PARTIAL processor is replaced by the built one rather than
        // handed to `kronos` as a function it has no idea what to do with.
        processor: processorFor(h),
      } as EventHandlerEntry
    }),
  })

  const processors: ReadonlyArray<RunningProcessor> = [...app.processors.values()]

  // `kronos` starts every processor. The fixture drives them by hand — parking
  // them to fast-forward a `given`, resuming them for an act — so the first
  // thing it does is take them back. An in-flight `start()` flips `running` on
  // after its own await, so parking waits for the turn of the loop and then
  // stops them; every step begins by awaiting this.
  const taken = (async () => {
    await settle()
    for (const processor of processors) processor.stop()
  })()

  async function parkAll(): Promise<void> {
    for (const processor of processors) processor.stop()
    await settle()
    for (const processor of processors) processor.stop()
  }

  async function resumeAll(): Promise<void> {
    for (const processor of processors) await processor.start()
  }

  /**
   * Close the observation window and open a new one.
   *
   * The STORE keeps its events and the scheduler keeps its armed schedules —
   * only the recordings are cleared, because the timeline is longer than one act
   * and the next act still happens in the world this one left behind.
   */
  function resetRecorders(): void {
    eventStore.reset()
    commandBus.reset()
    queryBus.reset()
  }

  /**
   * Which schedules existed, and in what state, when the current act began.
   *
   * Schedules are the one recording that cannot simply be cleared: an armed
   * schedule is LIVE STATE, and a deadline armed in one act is very often the
   * subject of the next. So instead of forgetting them, the fixture remembers
   * their states and reports the ones that CHANGED — which covers a schedule
   * newly armed, one that fired, and one that was cancelled, without inventing a
   * separate rule for each.
   */
  let priorSchedules = new Map<string, string>()
  function markSchedules(): void {
    priorSchedules = new Map(eventScheduler.schedules.map((s) => [s.token.id, s.status]))
  }

  /**
   * Turn a scenario's event value into a real message, stamped from the fixture
   * clock.
   *
   * A hole is refused here rather than written: `any()` is a claim about a value
   * somebody else produced, and a fact with a hole in it is not a fact.
   */
  function toEventMessage(value: EventValue<any>): EventMessage {
    const name = qualifiedNameToString(value.descriptor.name)
    if (containsHole(value.payload)) {
      throw new Error(
        `@kronos-ts/test: event(${name}, …) contains \`any()\`, but this event is a FACT — ` +
          `a \`given\` seeds the log and a \`when\` arrives in it, so every field has to have a ` +
          `value. \`any()\` is only meaningful in \`then\`, where it declines to pin what ` +
          `something else produced.`,
      )
    }
    return {
      kind: "event",
      identifier: generateIdentifier(),
      name: value.descriptor.name,
      version: value.descriptor.version,
      payload: value.payload,
      metadata: value.metadata ?? emptyMetadata(),
      timestamp: clock(),
      tags: value.descriptor.tags ? value.descriptor.tags(value.payload) : [],
    }
  }

  /**
   * History: append the facts, then move every cursor PAST them without invoking
   * a single handler.
   *
   * That is the difference between a `given` and a `when`, and it is the one the
   * old fixture got wrong by letting the automations replay the past. `given`
   * describes the world as it already is — the automations that would have fired
   * already fired, long ago, and firing them now would make the world one the
   * test never described.
   */
  async function applyGiven(events: ReadonlyArray<EventValue<any>>): Promise<void> {
    await parkAll()
    if (events.length > 0) await eventStore.append(events.map(toEventMessage))
    const head = await log.getHeadPosition()
    for (const processor of processors) await processor.resetTokens(head)
    resetRecorders()
    markSchedules()
    await resumeAll()
  }

  /**
   * Drive every processor to the head of the log, let what it dispatched settle,
   * and repeat until nothing moves.
   *
   * A processor reports `caughtUp` only after a batch it awaited to completion,
   * and its position is compared against the head — so an automation that
   * dispatched a command that appended is still in flight when the loop looks:
   * the head moved, and the loop goes round again.
   */
  async function quiesce(): Promise<void> {
    if (processors.length === 0) return
    const deadline = Date.now() + DEFAULT_WITHIN
    let previous = ""
    for (;;) {
      const heads = new Map<string, bigint>()
      for (const processor of processors) {
        const store = readsFrom.get(processor.name) ?? eventStore
        heads.set(processor.name, await store.getHeadPosition())
      }
      const settled = processors.every((p) => {
        const status = p.status()
        return status.caughtUp && status.position >= heads.get(p.name)!
      })
      const current = [...heads.values()].map(String).join(",")
      // Settled AND the logs stopped moving. Both halves are needed: an
      // automation that dispatched a command that appended is still in flight
      // when the loop first looks, and the moving head is what says so.
      if (settled && current === previous) return
      previous = current
      if (Date.now() > deadline) {
        const behind = processors
          .map(
            (p) =>
              `"${p.name}" at ${p.status().position} of ${heads.get(p.name)}` +
              `${p.status().caughtUp ? "" : " (busy)"}`,
          )
          .join(", ")
        throw new Error(
          `@kronos-ts/test: the scope's automations did not go quiet within ${DEFAULT_WITHIN}ms. ` +
            `${behind}. An event handler that dispatches a command whose events re-trigger it ` +
            `will never settle.`,
        )
      }
      await settle()
    }
  }

  /** Time passing: jump the clock (or really wait), fire what is due, settle. */
  async function wait(duration: Duration): Promise<void> {
    if (realTime) {
      await new Promise<void>((resolve) => setTimeout(resolve, duration))
    } else if (foreign) {
      throw new Error(
        `@kronos-ts/test: \`wait(${duration})\` cannot move time for this scope. The scope brought ` +
          `resources the fixture does not own — its own event store, its own scheduler, or a ` +
          `processor built over both — so there is no clock here to jump and no timer here to ` +
          `fire. Either let the fixture create the resources (take them as the scope's ` +
          `parameters), or pass \`{ realTime: true }\` and the wait will genuinely elapse.`,
      )
    } else {
      offset += duration
    }

    const due = eventScheduler.due()
    if (due.length > 0) await eventStore.append(due)
    await quiesce()
  }

  /** What the recorders currently hold, minus the act's own message. */
  function observe(
    actIdentifier: string | undefined,
  ): Omit<Observed, "result" | "threw" | "thrown"> {
    return {
      events: [...eventStore.appended],
      commands: commandBus.dispatched.filter((m) => m.identifier !== actIdentifier),
      schedules: eventScheduler.schedules.filter(
        (s) => priorSchedules.get(s.token.id) !== s.status,
      ),
    }
  }

  return {
    async run(scenario: Scenario, runOpts?: { within?: Duration }): Promise<RunOutcome> {
      await taken
      resetRecorders()
      markSchedules()
      await resumeAll()

      const act = actionOf(scenario)
      let result: unknown
      let thrown: unknown
      let threw = false
      let actIdentifier: string | undefined

      for (const step of scenario.steps) {
        if (step.kind === "given") {
          await applyGiven(step.events)
          continue
        }
        if (step.kind === "wait") {
          await wait(step.duration)
          continue
        }

        const action = step.action
        try {
          if (action.kind === "command") {
            // The message is built HERE rather than through `send` for one
            // reason: the fixture has to know its identifier, so the act's own
            // command can be told apart from the ones its handler dispatched.
            // Otherwise every `then` that mentions a command would have to
            // restate the command the scenario just performed.
            const message: Unstamped<CommandMessage> = {
              kind: "command",
              identifier: generateIdentifier(),
              name: action.descriptor.name,
              payload: action.payload,
              metadata: action.metadata ?? emptyMetadata(),
            }
            actIdentifier = message.identifier
            result = await commandBus.dispatch(message)
          } else if (action.kind === "query") {
            const message: Unstamped<QueryMessage> = {
              kind: "query",
              identifier: generateIdentifier(),
              name: action.descriptor.name,
              payload: action.payload,
              metadata: action.metadata ?? emptyMetadata(),
            }
            actIdentifier = message.identifier
            result = await queryBus.query(message)
          } else {
            // An event ARRIVES: it is appended, and the automations DO react —
            // which is the whole point of the shape. It is not history.
            await eventStore.append([toEventMessage(action)])
          }
        } catch (error) {
          thrown = error
          threw = true
        }
        await quiesce()
      }

      const claimsResult = scenario.then.some((a) => a.kind === "result")
      if (claimsResult && act.kind === "event") {
        throw new Error(
          `@kronos-ts/test: \`result(…)\` claims the act's answer, but the act is an EVENT ` +
            `arriving — an event answers nobody. Assert what it CAUSED instead: the events it ` +
            `led to, or the commands its automations dispatched.`,
        )
      }

      const claimsError = scenario.then.some((a) => a.kind === "error")
      if (threw && !claimsError) {
        // The scenario did not claim a throw, so the throw is the news — not a
        // diff of the events an act that never ran did not append.
        throw thrown
      }

      const within = runOpts?.within ?? opts.within ?? DEFAULT_WITHIN
      const deadline = Date.now() + within
      for (;;) {
        const outcome = observe(actIdentifier)
        const failure = evaluate(scenario, { ...outcome, threw, thrown, result }, act.kind)
        if (failure === undefined) {
          return { result, events: outcome.events, commands: outcome.commands }
        }
        // An all-fixture scope is deterministic: what is not true now will not
        // become true, so waiting for it would only make failures slow.
        if (!foreign || Date.now() > deadline) throw new ScenarioAssertionError(failure)
        await new Promise<void>((resolve) => setTimeout(resolve, 20))
        await quiesce()
      }
    },
  }
}

/** The one act a scenario performs. The builder guarantees there is exactly one. */
function actionOf(scenario: Scenario): Action {
  for (const step of scenario.steps) {
    if (step.kind === "when") return step.action
  }
  // Unreachable through the builder: `then` only exists after `when`.
  throw new Error("@kronos-ts/test: this scenario has no `when` — there is nothing to run.")
}

/** True when a value has an `any()` hole anywhere inside it. */
function containsHole(value: unknown): boolean {
  if (isAny(value)) return true
  if (Array.isArray(value)) return value.some(containsHole)
  if (typeof value === "object" && value !== null) {
    return Object.values(value as Record<string, unknown>).some(containsHole)
  }
  return false
}

/** One turn of the event loop — where "let the in-flight work run" is spelled. */
function settle(): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, 0))
}
