import type {
  CommandBus,
  CommandHandler,
  CommandHandlerEntry,
  CommandMessage,
  EventHandler,
  EventHandlerEntry,
  EventMessage,
  EventProcessor,
  EventStore,
  QueryHandler,
  QueryHandlerEntry,
  QueryMessage,
  RunningProcessor,
  ScheduleStoreCapability,
  SequencedDeadLetterQueue,
  SnapshotCapableEventStore,
  SubscriptionCapableQueryBus,
  TokenStore,
  UnitOfWork,
} from "@kronos-ts/core"
import {
  correlatingHandler,
  emptyMetadata,
  eventProcessor,
  generateIdentifier,
  inMemoryDeadLetterQueue,
  inMemoryEventStore,
  inMemorySnapshottingEventStore,
  inMemoryTokenStore,
  kronos,
  localCommandBus,
  localQueryBus,
  qualifiedNameToString,
  tagsOf,
  unitOfWork,
} from "@kronos-ts/core"
import {
  controllableSchedulingEventStore,
  recordingCommandBus,
  recordingEventStore,
  recordingQueryBus,
  type ScheduleRecord,
} from "./recording.js"
import { type Action, type App, type Duration, type EventValue, isAny } from "./values.js"

// ---------------------------------------------------------------------------
// The SITE a scenario runs at — internal. `scenario` and `storyline` open one per
// run, drive it through their steps and close it. Nobody writes a site by hand.
//
// It creates the resources — ONE log (which caches folds AND holds deadlines),
// one cursor table, one dead-letter queue, two buses, one clock — or takes them
// from the configured infrastructure, wraps what it needs to judge a `then`
// around them, and wires the subject against them exactly as a composition root
// would.
//
// In memory, everything here is deterministic. The clock does not tick unless a
// scenario says `advance`; the log's scheduling tier has no timer; the
// processors are driven to the head of the log and asked whether they are
// finished. There are no sleeps.
// ---------------------------------------------------------------------------

/**
 * The instant a scenario's clock starts at when nobody says otherwise.
 *
 * A FIXED instant rather than the wall clock, because a timestamp a test can
 * predict is worth more than one that is technically current: an assertion can
 * name `FIXTURE_EPOCH + 30_000` and mean it.
 */
export const FIXTURE_EPOCH = Date.UTC(2024, 0, 1)

/**
 * A processor the subject has DESCRIBED but not built, because the resources it
 * reads from belong to the site.
 *
 * This is the slice idiom, typed: the slice closes out the semantics that are
 * its own — its durable name, its sequences, whether it parks poison pills — and
 * leaves the resources as its parameter list. A projection that wants global
 * order and no queue writes
 * `(eventStore, tokenStore, unitOfWork) => eventProcessor("name", eventStore, tokenStore, unitOfWork)`
 * and declines the fourth argument by assignability.
 */
export type PartialProcessor = (
  eventStore: SiteEventStore,
  tokenStore: TokenStore,
  unitOfWork: () => UnitOfWork,
  deadLetterQueue: SequencedDeadLetterQueue,
) => EventProcessor<UnitOfWork>

/** The log a site hands its subject: snapshot- and schedule-capable, so any state and any `ctx.schedule` works. */
export type SiteEventStore = SnapshotCapableEventStore & ScheduleStoreCapability

/**
 * WHAT A SCENARIO IS ABOUT: the three lists a slice carries.
 *
 * Handlers as a slice writes them — unwrapped and un-sited. An event handler may
 * carry its `processor`, whole or partial; without one it runs on a default
 * processor. States appear in no list: a handler closes over the state it
 * folds, so there is nothing to wire.
 */
export type Subject = {
  readonly commandHandlers?: ReadonlyArray<CommandHandler<any, any, any> & { readonly eventStore?: EventStore }>
  readonly queryHandlers?: ReadonlyArray<QueryHandler<any, any, any> & { readonly eventStore?: EventStore }>
  readonly eventHandlers?: ReadonlyArray<
    EventHandler<any, any> & {
      readonly processor?: EventProcessor<any> | PartialProcessor
    }
  >
}

/**
 * A composition root as a function of the resources: what a module that builds
 * its own processors and stores is. Handed to `scenario` in place of a subject,
 * it is called with the site's resources and returns the lists.
 */
export type Scope = (resources: SiteResources) => Subject

/** The resources a site hands a {@link Scope}. */
export type SiteResources = {
  readonly eventStore: SiteEventStore
  readonly commandBus: CommandBus
  readonly queryBus: SubscriptionCapableQueryBus
  readonly unitOfWork: () => UnitOfWork
  readonly tokenStore: TokenStore
  readonly deadLetterQueue: SequencedDeadLetterQueue
}

/**
 * WHAT A HOST RUNS ON, for scenarios that should run on it rather than in memory.
 *
 * Every member is optional: what the infrastructure leaves out, the site
 * supplies in memory. The usual case for a read model on Postgres brings the
 * transaction family and the handler wrapper and keeps the log in memory —
 * which keeps scenarios deterministic, because the site still drives every
 * processor itself:
 *
 * ```ts
 * configure({
 *   infrastructure: async (unitOfWork) => {
 *     const pg = await freshDatabase()
 *     return {
 *       unitOfWork: postgresUnitOfWork(unitOfWork, pg),   // decorate what you were handed
 *       tokenStore: postgresTokenStore(pg),
 *       wrap: (handler, kind) => (kind === "command" ? handler : withDb(handler, pg)),
 *       close: () => pg.close(),
 *       pg,                                                // anything else rides to `outcome.app`
 *     }
 *   },
 * })
 * ```
 *
 * Bringing an `eventStore` (or a processor over a store of its own) makes the
 * site FOREIGN: it can no longer move time, and `then` re-judges its claims
 * until they hold or `within` passes, because the world keeps working where the
 * site cannot watch it settle.
 */
export type Infrastructure = {
  readonly unitOfWork?: () => UnitOfWork
  readonly eventStore?: EventStore
  readonly commandBus?: CommandBus
  readonly queryBus?: SubscriptionCapableQueryBus
  readonly tokenStore?: TokenStore
  readonly deadLetterQueue?: SequencedDeadLetterQueue
  /** Wrap every handler the subject hands over, as the composition root does. */
  readonly wrap?: <H extends (message: any, context: any) => any>(handler: H, kind: "command" | "query" | "event") => H
  /** Release what the infrastructure opened. Called once the scenario is judged. */
  readonly close?: () => Promise<void> | void
  readonly [extra: string]: unknown
}

/**
 * How a host builds its infrastructure — HANDED A TASK FACTORY, NOT A CLOCK.
 *
 * `unitOfWork` arrives already reading the site's clock — `() => unitOfWork(clock)`.
 * Decorate it the way a deployed root decorates one and hand back what you
 * built; nothing downstream has to be told about time. `clock` is there for the
 * one case that needs the raw arrow: infrastructure with its own schedule book.
 *
 * Called once per scenario, so each scenario gets a world of its own.
 */
export type InfrastructureFactory = (
  unitOfWork: () => UnitOfWork,
  clock: () => number,
) => Infrastructure | Promise<Infrastructure>

/**
 * A CLOCK YOU CAN MOVE — a clock, plus the one verb that moves it.
 */
export type AdvanceableClock = (() => number) & {
  /** Move time forward. What was due before is due now. */
  advance(duration: Duration): void
}

/**
 * A clock that starts at `from` and moves only when you say so.
 *
 * It is a CLOSURE over its own offset, which is what makes it work: everything
 * downstream captured the arrow and reads through it, so a move is seen by
 * every task minted afterwards.
 */
export function advanceableClock(from: number = FIXTURE_EPOCH): AdvanceableClock {
  let offset = 0
  const read = () => from + offset
  return Object.assign(read, {
    advance(duration: Duration): void {
      offset += duration
    },
  })
}

/** How a scenario runs. Set once for the test run with `configure`, override per scenario. */
export type ScenarioOptions = {
  /** What to run on. Absent: everything in memory. */
  readonly infrastructure?: InfrastructureFactory
  /**
   * The clock. Absent: an {@link advanceableClock} at {@link FIXTURE_EPOCH}, built
   * fresh per scenario, so `advance` works. A plain `() => number` is read and
   * never moved.
   */
  readonly clock?: () => number
  /** How long a FOREIGN site's claims are re-judged before they fail. Default 5000ms. */
  readonly within?: Duration
}

let defaults: ScenarioOptions = {}

/**
 * Set the defaults every scenario and storyline in this test run uses — once,
 * typically from a test preload — so a spec says what it tests and nothing about
 * where it runs. Calling it again replaces the defaults; a scenario's own
 * options still win over them.
 *
 * ```ts
 * // test/setup.ts, preloaded by bunfig.toml
 * configure({ infrastructure: postgres })
 * ```
 */
export function configure(options: ScenarioOptions): void {
  defaults = { ...options }
}

/** The defaults, merged with a scenario's own options. */
export function resolveOptions(own: ScenarioOptions | undefined): ScenarioOptions {
  return { ...defaults, ...own }
}

const DEFAULT_WITHIN = 5000

/** What an act did, before it is judged. */
export type Acted = {
  readonly result: unknown
  readonly threw: boolean
  readonly thrown: unknown
  readonly actIdentifier: string | undefined
}

/** The running world a scenario drives. */
export type Site = {
  readonly app: App
  /** Whether claims have to be re-judged until they settle. */
  readonly foreign: boolean
  readonly within: Duration
  /** Facts that already happened: projections fold them, automations do not act on them. */
  history(events: ReadonlyArray<EventValue<any>>): Promise<void>
  advance(duration: Duration): Promise<void>
  act(action: Action): Promise<Acted>
  /** Start a new observation window: what is recorded from here on is the next act's. */
  window(): void
  observe(actIdentifier: string | undefined): {
    events: ReadonlyArray<EventMessage>
    commands: ReadonlyArray<CommandMessage>
    schedules: ReadonlyArray<ScheduleRecord>
  }
  quiesce(): Promise<void>
  close(): Promise<void>
}

/** Open a site for one run: resources, the subject wired against them, processors parked. */
export async function openSite(subject: Subject | Scope, options: ScenarioOptions): Promise<Site> {
  const supplied_clock = options.clock
  const advanceable: AdvanceableClock | undefined =
    supplied_clock === undefined
      ? advanceableClock()
      : typeof (supplied_clock as AdvanceableClock).advance === "function"
        ? (supplied_clock as AdvanceableClock)
        : undefined
  const clock: () => number = advanceable ?? supplied_clock!

  const siteUnitOfWork = (): UnitOfWork => unitOfWork(clock)
  const supplied: Infrastructure = (await options.infrastructure?.(siteUnitOfWork, clock)) ?? {}
  const uow: () => UnitOfWork = supplied.unitOfWork ?? siteUnitOfWork

  // THE LOG. The site composes what a host composes — snapshotting AND
  // scheduling — unless the infrastructure brought a log of its own. The
  // controllable scheduling tier is held by its own name, because `advance`
  // asks it what is due and appends that back in through the OUTERMOST store,
  // so a fired deadline is recorded exactly like a handler's append.
  const log = inMemoryEventStore()
  const scheduling =
    supplied.eventStore === undefined
      ? controllableSchedulingEventStore(inMemorySnapshottingEventStore(log), clock)
      : undefined

  /**
   * HISTORY MODE. While `given` is being folded, handlers run — a projection has
   * to write what the past left behind — but nothing they would set in motion is
   * let out: a command an event handler dispatches is not dispatched (its `send`
   * resolves to `undefined`), and a deadline it arms is not armed. The
   * automations that would have fired already fired, long ago.
   */
  let history = false

  const eventStore = recordingEventStore(
    holdingSchedulesInHistory((supplied.eventStore ?? scheduling) as EventStore, () => history) as Parameters<
      typeof recordingEventStore
    >[0],
  ) as unknown as SiteEventStore & {
    readonly appended: ReadonlyArray<EventMessage>
    reset(): void
  }
  const tokenStore = (supplied.tokenStore ?? inMemoryTokenStore()) as TokenStore
  const deadLetterQueue = (supplied.deadLetterQueue ?? inMemoryDeadLetterQueue()) as SequencedDeadLetterQueue
  const recordedCommands = recordingCommandBus(supplied.commandBus ?? localCommandBus(uow))
  const commandBus = holdingCommandsInHistory(recordedCommands, () => history)
  const queryBus = recordingQueryBus(supplied.queryBus ?? localQueryBus(siteUnitOfWork))

  const resources: SiteResources = {
    eventStore,
    commandBus,
    queryBus,
    unitOfWork: uow,
    tokenStore,
    deadLetterQueue,
  }
  const lists: Subject = typeof subject === "function" ? subject(resources) : subject

  let foreign = supplied.eventStore !== undefined
  function ownStore(store: EventStore | undefined): void {
    if (store !== undefined && store !== eventStore) foreign = true
  }

  const defaultProcessor = eventProcessor("scenario", eventStore, tokenStore, uow)
  const readsFrom = new Map<string, EventStore>()

  function processorFor(declared: EventProcessor<any> | PartialProcessor | undefined): EventProcessor<any> {
    if (declared === undefined) return defaultProcessor
    const built = typeof declared === "function" ? declared(eventStore, tokenStore, uow, deadLetterQueue) : declared
    if (built.eventStore !== eventStore || built.tokenStore !== tokenStore) {
      if (built.eventStore !== eventStore) foreign = true
    }
    readsFrom.set(built.name, built.eventStore)
    return built
  }

  /**
   * The handler as the site runs it: the infrastructure's wrapper inside,
   * correlation outside, so what a handler gives birth to carries the id pair of
   * the message it was handling and `then` can assert a causal chain.
   */
  function wrapped<H extends (message: any, context: any) => any>(handler: H, kind: "command" | "query" | "event"): H {
    const inner = supplied.wrap ? supplied.wrap(handler, kind) : handler
    return correlatingHandler(inner as any) as unknown as H
  }

  const app = kronos({
    commandHandlers: (lists.commandHandlers ?? []).map((h) => {
      ownStore(h.eventStore)
      return {
        eventStore,
        ...h,
        handler: wrapped(h.handler, "command"),
        commandBus,
        queryBus,
      } as unknown as CommandHandlerEntry<UnitOfWork, any>
    }),
    queryHandlers: (lists.queryHandlers ?? []).map((h) => {
      ownStore(h.eventStore)
      return {
        eventStore,
        ...h,
        handler: wrapped(h.handler, "query"),
        queryBus,
      } as unknown as QueryHandlerEntry<UnitOfWork, any>
    }),
    eventHandlers: (lists.eventHandlers ?? []).map(
      (h) =>
        ({
          ...h,
          handler: wrapped(h.handler, "event"),
          commandBus,
          queryBus,
          // No `eventStore` default: an event handler's ctx falls back to the log
          // its PROCESSOR reads. Last, so a partial processor is replaced by the
          // built one.
          processor: processorFor(h.processor),
        }) as unknown as EventHandlerEntry<UnitOfWork, any>,
    ),
  })

  const processors: ReadonlyArray<RunningProcessor> = [...app.processors.values()]
  const within = options.within ?? DEFAULT_WITHIN

  // `kronos` starts every processor. The site drives them by hand, so the first
  // thing it does is take them back.
  await settle()
  for (const processor of processors) processor.stop()

  async function parkAll(): Promise<void> {
    for (const processor of processors) processor.stop()
    await settle()
    for (const processor of processors) processor.stop()
  }

  async function resumeAll(): Promise<void> {
    for (const processor of processors) await processor.start()
  }

  function storeOf(processor: RunningProcessor): EventStore {
    return readsFrom.get(processor.name) ?? eventStore
  }

  let priorSchedules = new Map<string, string>()
  function markSchedules(): void {
    priorSchedules = new Map((scheduling?.schedules ?? []).map((s) => [s.token.id, s.status]))
  }

  function window(): void {
    eventStore.reset()
    recordedCommands.reset()
    queryBus.reset()
    markSchedules()
  }

  function toEventMessage(value: EventValue<any>): EventMessage {
    const name = qualifiedNameToString(value.descriptor.name)
    if (containsHole(value.payload)) {
      throw new Error(
        `@kronos-ts/test: event(${name}, …) contains \`any()\`, but this event is a FACT — ` +
          `a \`given\` fills the log and a \`when\` arrives in it, so every field has to have a ` +
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
      tags: tagsOf(value.descriptor, value.payload, value.metadata ?? emptyMetadata()),
    }
  }

  async function quiesce(): Promise<void> {
    if (processors.length === 0) return
    const deadline = Date.now() + DEFAULT_WITHIN
    let previous = ""
    for (;;) {
      const heads = new Map<string, bigint>()
      for (const processor of processors) heads.set(processor.name, await storeOf(processor).getHeadPosition())
      const settled = processors.every((p) => {
        const status = p.status()
        return status.caughtUp && status.position >= heads.get(p.name)!
      })
      const current = [...heads.values()].map(String).join(",")
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
          `@kronos-ts/test: the subject's event handlers did not go quiet within ${DEFAULT_WITHIN}ms. ` +
            `${behind}. An event handler that dispatches a command whose events re-trigger it ` +
            `will never settle, and one that keeps failing holds its processor where it is.`,
        )
      }
      await settle()
    }
  }

  return {
    app: { ...supplied, ...resources } as App,
    get foreign() {
      return foreign
    },
    within,

    /**
     * HISTORY: append the facts, then hand them to every processor as a REPLAY.
     *
     * The processors fold them — that is what makes a read model show what the
     * past left behind — with `ctx.isReplay()` true, the same signal a
     * production replay gives. Whatever an event handler would set in motion
     * while they do is held back (see `history` above).
     *
     * The replay is two token resets, which is how core spells it: first to the
     * head, so each processor has "got to" the end of the history, then back to
     * where the history began, which core turns into a replay up to that head.
     */
    async history(events) {
      if (events.length === 0) return
      await parkAll()
      const before = new Map<string, bigint>()
      for (const processor of processors) before.set(processor.name, await storeOf(processor).getHeadPosition())
      await eventStore.append(events.map(toEventMessage))
      for (const processor of processors) {
        const store = storeOf(processor)
        const head = await store.getHeadPosition()
        await processor.resetTokens(head)
        const from = before.get(processor.name)!
        if (head > from) await processor.resetTokens(from)
      }
      history = true
      try {
        await resumeAll()
        await quiesce()
      } finally {
        history = false
      }
      window()
    },

    async advance(duration) {
      if (advanceable === undefined || foreign) {
        throw new Error(
          "@kronos-ts/test: this scenario cannot move time — its clock is not one it can move, or its " +
            "infrastructure brought a log whose deadlines the scenario cannot reach. Leave `clock` " +
            "unset (or pass `advanceableClock()`) and keep the log in memory to `advance`.",
        )
      }
      advanceable.advance(duration)
      if (scheduling !== undefined) {
        const due = scheduling.due()
        if (due.length > 0) await eventStore.append(due)
      }
      await quiesce()
    },

    async act(action) {
      await resumeAll()
      let result: unknown
      let thrown: unknown
      let threw = false
      let actIdentifier: string | undefined
      try {
        if (action.kind === "command") {
          // Built here rather than through `send`, so the site knows its
          // identifier and can tell the act's own command apart from the ones
          // its handler dispatched.
          const message: CommandMessage = {
            kind: "command",
            identifier: generateIdentifier(),
            name: action.descriptor.name,
            payload: action.payload,
            metadata: action.metadata ?? emptyMetadata(),
          }
          actIdentifier = message.identifier
          result = await commandBus.dispatch(message)
        } else if (action.kind === "query") {
          const message: QueryMessage = {
            kind: "query",
            identifier: generateIdentifier(),
            name: action.descriptor.name,
            payload: action.payload,
            metadata: action.metadata ?? emptyMetadata(),
          }
          actIdentifier = message.identifier
          result = await queryBus.query(message)
        } else {
          // An event ARRIVES: appended, and the automations DO react.
          await eventStore.append([toEventMessage(action)])
        }
      } catch (error) {
        thrown = error
        threw = true
      }
      await quiesce()
      return { result, threw, thrown, actIdentifier }
    },

    window,

    observe(actIdentifier) {
      return {
        events: [...eventStore.appended],
        commands: recordedCommands.dispatched.filter((m) => m.identifier !== actIdentifier),
        schedules: (scheduling?.schedules ?? []).filter((s) => priorSchedules.get(s.token.id) !== s.status),
      }
    },

    quiesce,

    async close() {
      try {
        await app.stop()
      } finally {
        await supplied.close?.()
      }
    },
  }
}

/** A command bus whose dispatches are held back while history is being folded. */
function holdingCommandsInHistory<B extends CommandBus<any>>(bus: B, inHistory: () => boolean): B {
  return new Proxy(bus, {
    get(target, key, receiver) {
      if (key === "dispatch") {
        return (message: CommandMessage, ...rest: unknown[]) =>
          inHistory() ? Promise.resolve(undefined) : (target.dispatch as any).call(target, message, ...rest)
      }
      const value = Reflect.get(target, key, receiver)
      return typeof value === "function" ? value.bind(target) : value
    },
  })
}

/** A log whose deadlines are not armed (or cancelled) while history is being folded. */
function holdingSchedulesInHistory<E extends EventStore>(store: E, inHistory: () => boolean): E {
  return new Proxy(store, {
    get(target, key, receiver) {
      if (key === "schedule" && typeof (target as any).schedule === "function") {
        return (...args: unknown[]) =>
          inHistory() ? Promise.resolve({ id: `history-${generateIdentifier()}` }) : (target as any).schedule(...args)
      }
      if (key === "cancelSchedule" && typeof (target as any).cancelSchedule === "function") {
        return (...args: unknown[]) =>
          inHistory() ? Promise.resolve({ kind: "not-found" }) : (target as any).cancelSchedule(...args)
      }
      const value = Reflect.get(target, key, receiver)
      return typeof value === "function" ? value.bind(target) : value
    },
  })
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
export function settle(): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, 0))
}
