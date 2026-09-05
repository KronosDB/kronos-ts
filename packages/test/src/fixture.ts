import {
  correlating,
  correlatingHandler,
  emptyMetadata,
  eventProcessor,
  generateIdentifier,
  inMemoryDeadLetterQueue,
  inMemoryEventStore,
  inMemorySnapshottingEventStore,
  inMemoryTokenStore,
  kronos,
  qualifiedNameToString,
  localCommandBus,
  localQueryBus,
  unitOfWork,
} from "@kronos-ts/core"
import type {
  CorrelatingUnitOfWork,
  CommandBus,
  CommandHandler,
  CommandHandlerEntry,
  CommandMessage,
  EventHandler,
  EventHandlerEntry,
  EventMessage,
  Message,
  Metadata,
  EventProcessor,
  EventStore,
  ScheduleStoreCapability,
  QueryHandler,
  QueryHandlerEntry,
  QueryMessage,
  RunningProcessor,
  SequencedDeadLetterQueue,
  Sited,
  SnapshotCapableEventStore,
  SubscriptionCapableQueryBus,
  TokenStore,
  UnitOfWork,
} from "@kronos-ts/core"
import { evaluate, ScenarioAssertionError, type Observed } from "./diff.js"
import {
  controllableSchedulingEventStore,
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
// resources — ONE log (which caches folds AND holds deadlines), one cursor
// table, one dead-letter queue, two buses, one clock — and HANDS THEM to the scope,
// which is a FUNCTION of them. That is the whole inversion: production's
// composition root is also a function of its resources, so a scope written for
// the fixture is a scope you can deploy, and a scope written for production is
// one you can test. Nothing is replaced behind anybody's back.
//
// Everything in here is deterministic. The clock does not tick unless a scenario
// says `wait`; the log's scheduling tier has no timer; the processors are driven to the head
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
  eventStore: FixtureEventStore,
  tokenStore: TokenStore,
  unitOfWork: () => FixtureUnitOfWork,
  deadLetterQueue: SequencedDeadLetterQueue,
) => EventProcessor<FixtureUnitOfWork>

/** An event handler entry as a scope writes one: the processor may still be partial. */
export type FixtureEventHandler = Sited<EventHandler<any, any>, FixtureEventStore> & {
  readonly processor?: EventProcessor<FixtureUnitOfWork> | PartialProcessor
}

/**
 * What the fixture's tasks are: a unit of work that CARRIES. The site composes
 * `correlating(unitOfWork(clock))`, so a scope's partial processor is handed a
 * factory of these and hands back a processor over them — without the scope
 * ever having to name the type.
 */
export type FixtureUnitOfWork = CorrelatingUnitOfWork

/**
 * What a scope returns: the three lists `kronos` takes.
 *
 * The same three, with the same meanings. A scope is not a special test shape —
 * it is a composition root whose resources arrive as arguments. States appear
 * in no list here for the same reason they appear in none there: a handler
 * closes over the state it folds, so there is nothing for the fixture to wire.
 */
export type FixtureLists = {
  readonly commandHandlers?: ReadonlyArray<Sited<CommandHandler<any, any>, FixtureEventStore>>
  readonly queryHandlers?: ReadonlyArray<Sited<QueryHandler<any, any>, FixtureEventStore>>
  readonly eventHandlers?: ReadonlyArray<FixtureEventHandler>
}

/**
 * The log the fixture hands a scope: in-memory, RECORDING, SNAPSHOT-CAPABLE and
 * SCHEDULE-CAPABLE.
 *
 * ONE OBJECT, capabilities and all. It used to be three — a log, a snapshot
 * cache and a scheduler, handed over as separate parameters — and a scope that
 * wanted either had to accept them and put each in the right field. There is no
 * second or third resource now, because both are tiers ON the log: a scope
 * loading a state that declares a policy typechecks against this because this
 * can serve it, a scope calling `ctx.schedule` typechecks for the same reason,
 * and the same scope against a bare `inMemoryEventStore()` does neither.
 */
export type EventRecording = {
  readonly appended: ReadonlyArray<EventMessage>
  reset(): void
}

export type FixtureEventStore = SnapshotCapableEventStore & ScheduleStoreCapability & EventRecording

/**
 * WHAT A SCOPE IS HANDED: every resource, already wrapped for recording.
 *
 * The scope used to receive one log and the fixture kept the rest to itself,
 * so a scope could not be the composition root it claimed to be. It receives
 * the whole arrangement now — the same names a deployed root holds — and the
 * objects are the RECORDED ones, so entries pointing at them are what the
 * fixture judges.
 */
export type FixtureResources<
  U extends CorrelatingUnitOfWork = FixtureUnitOfWork,
  E extends EventStore = FixtureEventStore,
> = {
  readonly eventStore: E & EventRecording
  readonly commandBus: CommandBus<U>
  readonly queryBus: SubscriptionCapableQueryBus<U>
  readonly unitOfWork: () => U
  readonly tokenStore: TokenStore<U>
  readonly deadLetterQueue: SequencedDeadLetterQueue<U>
}

/**
 * A composition root as a function of the resources the fixture owns.
 *
 * ONE STORE PARAMETER. The old second parameter — a snapshot store — is gone
 * along with the seam it belonged to; a scope that caches folds takes the one
 * log and puts it on its entries, exactly as a scope that does not.
 *
 * ```ts
 * const fixture = testFixture((eventStore) => courses(eventStore))
 * const fixture = testFixture((eventStore) => ({ commandHandlers: [{ ...openCourse, eventStore }], … }))
 * ```
 */
export type FixtureScope<
  U extends CorrelatingUnitOfWork = FixtureUnitOfWork,
  E extends EventStore = FixtureEventStore,
> = (resources: FixtureResources<U, E>) => FixtureLists

/**
 * THE ARRANGEMENT A HOST ACTUALLY DEPLOYS, handed to the fixture instead of
 * conjured by it.
 *
 * The fixture used to build every one of these itself and hand the scope only
 * a log — which made it the one library here that conjures its delegates, and
 * meant a test could never run against the infrastructure it was going to
 * ship on. It takes them now and WRAPS what it is given: the recorders it
 * needs to judge a `then` go AROUND your bus and your log, and they are
 * capability-preserving, so a kronosdb bus stays a kronosdb bus.
 *
 * ```ts
 * testFixture(scope, {
 *   infrastructure: (unitOfWork) => {
 *     const uow = postgresUnitOfWork(unitOfWork, pg)   // decorate what you were handed
 *     return {
 *       unitOfWork: uow,
 *       eventStore: postgresSnapshottingEventStore(postgresEventStore(pg, …), pg, …),
 *       commandBus: localCommandBus(uow),
 *       queryBus: localQueryBus(uow),
 *       tokenStore: postgresTokenStore(pg),
 *     }
 *   },
 * })
 * ```
 *
 * `tokenStore` and `deadLetterQueue` are the only optional members: absent,
 * the fixture supplies in-memory ones, which is what a scope with no
 * persistence family wants and what every existing test gets.
 */
export type FixtureInfrastructure<
  U extends CorrelatingUnitOfWork = FixtureUnitOfWork,
  E extends EventStore = EventStore,
> = {
  readonly unitOfWork: () => U
  readonly eventStore: E
  readonly commandBus: CommandBus<U>
  readonly queryBus: SubscriptionCapableQueryBus<U>
  readonly tokenStore?: TokenStore<U>
  readonly deadLetterQueue?: SequencedDeadLetterQueue<U>
}

/**
 * How a host builds its infrastructure — HANDED A TASK FACTORY, NOT A CLOCK.
 *
 * The fixture needs two things of every task: that it reads the fixture's
 * clock, so `wait` can move time, and that it CARRIES, so `then` can assert a
 * causal chain. Both used to be things a host had to know and repeat —
 * `postgresUnitOfWork(() => correlating(unitOfWork(clock)), pg)`, with a
 * dropped `clock` or a missing `correlating` being a quiet way to get a test
 * that cannot move time or cannot see causation.
 *
 * So `unitOfWork` arrives already both — `() => correlating(unitOfWork(clock))`.
 * Decorate it the way a deployed root decorates one — every adapter's
 * unit-of-work decorator is `(next, client) => …` and composes onto it — and
 * hand back what you built. Nothing downstream has to be told about time,
 * because everything downstream is built from this.
 *
 * `clock` is the second argument for the one case that genuinely needs the raw
 * arrow: infrastructure with its own schedule book (`inMemorySchedulingEventStore(next, { clock })`).
 * It is a function and must stay one — it reads a base instant plus an offset
 * `wait` advances, so whatever captured the ARROW sees time move and whatever
 * captured a NUMBER is frozen where it was built.
 */
export type InfrastructureFactory<
  U extends CorrelatingUnitOfWork = FixtureUnitOfWork,
  E extends EventStore = EventStore,
> = (unitOfWork: () => FixtureUnitOfWork, clock: () => number) => FixtureInfrastructure<U, E>

/**
 * A CLOCK YOU CAN MOVE — a clock, plus the one verb that moves it.
 *
 * The capability is the type: a fixture given one of these can run scenarios
 * that `.advance`, and a fixture given a plain `() => number` cannot, so the
 * refusal is a compile error at `run` rather than a throw when the scenario
 * reaches the step. Reading it is reading a clock; nothing else changes.
 */
export type AdvanceableClock = (() => number) & {
  /** Move time forward. What was due before is due now. */
  advance(duration: Duration): void
}

/**
 * A clock that starts at `from` and moves only when you say so.
 *
 * ```ts
 * const clock = advanceableClock()
 * const fixture = testFixture(scope, { clock })    // …can run `.advance` scenarios
 * ```
 *
 * It is a CLOSURE over its own offset, which is what makes it work: everything
 * downstream captured the arrow and reads through it, so a move is seen by
 * every task minted afterwards. Capture `clock()` instead and you have frozen a
 * number — which is why every seam in core spells it `clock?: () => number`.
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

/** Is this clock one the fixture can move? */
export type IfAdvanceable<C, Capable, Bare> = C extends AdvanceableClock ? Capable : Bare

/** The clock an options record carries — a bare fixture gets an advanceable one. */
export type ClockOf<O extends FixtureOptions> = O["clock"] extends undefined
  ? AdvanceableClock
  : undefined extends O["clock"]
    ? AdvanceableClock
    : NonNullable<O["clock"]>

export type FixtureOptions = {
  /**
   * How long to keep re-judging the claims before calling them failed. Only ever
   * used against a scope that brought resources the fixture does not own — an
   * all-in-memory scope is deterministic, so a claim that does not hold on the
   * first look will not hold on the second either, and waiting would be theatre.
   * Default: 5000ms.
   */
  readonly within?: Duration
  /**
   * The clock everything under this fixture reads.
   *
   * Absent, the fixture builds an {@link advanceableClock} at
   * {@link FIXTURE_EPOCH} — so scenarios can `.advance`. Give it a plain
   * `() => number` (`Date.now`, say) and the fixture reads time without ever
   * moving it, which is the honest arrangement for real infrastructure: use
   * `.await(until)` to wait for the world instead of pretending to hurry it.
   */
  readonly clock?: (() => number) | AdvanceableClock
  /**
   * The infrastructure to run against — see {@link InfrastructureFactory}.
   * Absent, the fixture builds the in-memory stack it always did, which is
   * what a scope with no persistence family wants.
   */
  readonly infrastructure?: InfrastructureFactory<any, any>
}

/** What one act did. `events` and `commands` cover THIS act only. */
export type RunOutcome = {
  /** A command handler's return, or a query's answer. `undefined` for an event act. */
  readonly result: unknown
  /** Events appended during the act — automations included, `given` excluded. */
  readonly events: ReadonlyArray<EventMessage>
  /** Commands dispatched during the act — the act's own command excluded. */
  readonly commands: ReadonlyArray<CommandMessage>
}

/**
 * One timeline.
 *
 * Consecutive `run` calls continue the SAME log and the SAME processor cursors,
 * which is how a saga is tested: each call reports only what it caused, and the
 * world it caused it in is whatever the previous calls left behind.
 */
export type TestFixture<A extends boolean = boolean> = {
  /**
   * Run one scenario. A scenario that `.advance`s the clock is accepted only by
   * a fixture that was given a clock it can move — otherwise the refusal lands
   * HERE, at the line that pairs the two, instead of throwing mid-run.
   */
  run(
    scenario: true extends A ? Scenario<boolean> : Scenario<false>,
    opts?: { within?: Duration },
  ): Promise<RunOutcome>
  /**
   * Stop the processors the fixture assembled and release their pollers.
   * A test runner that force-exits never needs this; a plain script does.
   */
  stop(): Promise<void>
}

const DEFAULT_WITHIN = 5000

/** Wire `scope` against resources the fixture owns, and run scenarios at it. */
export function testFixture<O extends FixtureOptions = FixtureOptions>(
  scope: FixtureScope,
  opts: O = {} as O,
): TestFixture<IfAdvanceable<ClockOf<O>, true, false>> {
  /**
   * THE CLOCK, and whether this fixture can move it.
   *
   * Given an {@link AdvanceableClock}, `.advance` works and the type says so.
   * Given a plain clock — or none — the fixture reads it and never moves it,
   * which is the honest answer for real infrastructure: a postgres poller and
   * a kronosdb server keep their own time and nothing here can hurry them.
   */
  const supplied_clock = opts.clock
  const advanceable: AdvanceableClock | undefined =
    supplied_clock !== undefined && typeof (supplied_clock as AdvanceableClock).advance === "function"
      ? (supplied_clock as AdvanceableClock)
      : opts.clock === undefined
        ? advanceableClock()
        : undefined
  const clock: () => number = advanceable ?? (supplied_clock as () => number)
  /**
   * The fixture's tasks CORRELATE.
   *
   * A fixture is a composition root, so it makes a composition root's choices,
   * and this is one of them: scenarios are about causal chains — a command
   * appends an event, an automation reacts to it and dispatches another command
   * — and a `then` that names `metadata` should be able to see that chain. So
   * the fixture composes what a host composes: a correlating unit of work here,
   * `correlatingHandler(handler, correlationFrom)` around every handler the
   * scope hands it (see `carrying` below), with the id pair as its cargo —
   * written out below like any host writes it — because the id pair is what a
   * test can meaningfully assert about.
   *
   * A scope that wants different cargo wraps its own handlers before returning
   * them; wrapping is idempotent in effect (the second attach writes the same
   * keys), so composing on top of this costs nothing.
   */
  // WHAT THE HOST BROUGHT, IF ANYTHING — built ONCE, from the fixture's clock,
  // so a factory that reads that clock moves when `wait` moves. Once: calling
  // the factory per task would mint a fresh pool, a fresh log and a fresh
  // registry every time, and nothing would share a transaction with anything.
  /**
   * The unit-of-work factory the fixture guarantees: on ITS clock, and
   * CARRYING. A host's factory decorates this rather than rebuilding it, so
   * neither property can be dropped on the way through.
   */
  const fixtureUnitOfWork = (): FixtureUnitOfWork => correlating(unitOfWork(clock))

  const supplied = opts.infrastructure?.(fixtureUnitOfWork, clock) as
    | FixtureInfrastructure
    | undefined

  const uow: () => FixtureUnitOfWork = supplied?.unitOfWork ?? fixtureUnitOfWork

  const log = inMemoryEventStore()
  // THE FIXTURE COMPOSES WHAT A HOST COMPOSES — snapshotting AND scheduling —
  // and it is the SAME stack a host writes, around the same one object. Three
  // wrappers, all ADDITIVE, so every capability survives every layer above it:
  // a scope loading a snapshotting state typechecks against what comes out, and
  // so does a scope that arms a deadline.
  //
  // THE ORDER IS THE ONE THE STORY REQUIRES. The recorder is OUTERMOST so
  // `appended` is what left the fixture, whatever the read path underneath
  // does. The controllable scheduling tier sits UNDER it and is held by its own
  // name below, because the fixture drives it directly: `wait` asks what is due
  // and appends the result back in through the OUTERMOST store, so a fired
  // deadline is recorded exactly like a handler's append instead of slipping in
  // beneath the recorder.
  //
  // THE SCHEDULE BOOK IS THE FIXTURE'S ONLY WHEN THE LOG IS. `wait` fires due
  // deadlines by asking this directly, and there is nothing to ask when the
  // host brought its own log — a postgres poller and a kronosdb server hold
  // their schedules where this cannot reach, and nothing here hurries them.
  const scheduling =
    supplied === undefined
      ? controllableSchedulingEventStore(inMemorySnapshottingEventStore(log), clock)
      : undefined

  // EVERY RESOURCE: the host's where it brought one, the fixture's otherwise.
  // The recorders go on the OUTSIDE of whatever arrived — they are
  // capability-preserving, so a wrapped bus is still whatever it was.
  const eventStore = recordingEventStore(
    (supplied?.eventStore ?? scheduling) as Parameters<typeof recordingEventStore>[0],
  ) as FixtureEventStore
  const tokenStore = (supplied?.tokenStore ?? inMemoryTokenStore()) as TokenStore<FixtureUnitOfWork>
  const deadLetterQueue = (supplied?.deadLetterQueue ??
    inMemoryDeadLetterQueue()) as SequencedDeadLetterQueue<FixtureUnitOfWork>
  const commandBus = recordingCommandBus(supplied?.commandBus ?? localCommandBus(uow))
  const queryBus = recordingQueryBus(supplied?.queryBus ?? localQueryBus(uow))

  // ---- what the scope asked for -------------------------------------------
  const lists = scope({
    eventStore,
    commandBus,
    queryBus,
    unitOfWork: uow,
    tokenStore,
    deadLetterQueue,
  })

  /**
   * Whether anything in the scope is beyond the fixture's reach.
   *
   * The fixture can only jump a clock it owns and only settle a processor it
   * drives. A scope that brought its own store or an already-built processor
   * over foreign resources is a REAL-INFRASTRUCTURE scope: `wait` cannot fake
   * time for it, and its claims have to be re-judged until they settle rather
   * than judged once.
   *
   * ONE CHECK COVERS BOTH NOW. A scope used to be able to bring a foreign
   * SCHEDULER as well as a foreign store, so there were two of these; a
   * scheduler is a tier on a log, so bringing one IS bringing a store.
   */
  let foreign = supplied !== undefined
  function ownStore(store: EventStore | undefined): void {
    if (store !== undefined && store !== eventStore) foreign = true
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
  function processorFor(entry: FixtureEventHandler): EventProcessor<FixtureUnitOfWork> {
    const declared = entry.processor
    if (declared === undefined) return defaultProcessor
    const built =
      typeof declared === "function"
        ? declared(
            eventStore,
            tokenStore as TokenStore,
            uow,
            deadLetterQueue as SequencedDeadLetterQueue,
          )
        : declared
    if (built.eventStore !== eventStore || built.tokenStore !== tokenStore) foreign = true
    readsFrom.set(built.name, built.eventStore)
    return built
  }

  /**
   * The handler, wrapped so what it gives birth to carries the id pair of the
   * message it was handling. The scope wrote a plain handler; the SITE decides
   * what propagates, exactly as a deployed composition root would.
   *
   * The cargo is the fixture's own two lines — the chain is inherited or seeded,
   * the cause is the parent, unconditionally, so the causal graph walks one hop
   * at a time. Any host writes exactly this.
   */
  const correlationFrom = (parent: Message): Metadata => ({
    correlationId: String(parent.metadata.correlationId ?? parent.identifier),
    causationId: String(parent.identifier),
  })

  function carrying<H extends { readonly handler: any }>(entry: H): H {
    return { ...entry, handler: correlatingHandler(entry.handler, correlationFrom) }
  }

  const app = kronos<FixtureUnitOfWork, FixtureEventStore>({
    commandHandlers: (lists.commandHandlers ?? []).map((h) => {
      ownStore(h.eventStore)
      return carrying({
        eventStore,
        ...h,
        commandBus,
        queryBus,
      }) as CommandHandlerEntry<FixtureUnitOfWork, FixtureEventStore>
    }),
    queryHandlers: (lists.queryHandlers ?? []).map((h) => {
      ownStore(h.eventStore)
      return carrying({
        eventStore,
        ...h,
        queryBus,
      }) as QueryHandlerEntry<FixtureUnitOfWork, FixtureEventStore>
    }),
    eventHandlers: (lists.eventHandlers ?? []).map((h) => {
      ownStore(h.eventStore)
      return carrying({
        ...h,
        commandBus,
        queryBus,
        // NO `eventStore` DEFAULT HERE, deliberately. An event handler's
        // `ctx` falls back to the log its PROCESSOR reads (see `contextFor` in
        // `running-processor.ts`), which is the fixture's store in every
        // arrangement the fixture owns — and injecting one would quietly
        // redirect a handler whose scope built a processor over its own log.
        // Last, so a PARTIAL processor is replaced by the built one rather than
        // handed to `kronos` as a function it has no idea what to do with.
        processor: processorFor(h),
      }) as EventHandlerEntry<FixtureUnitOfWork, FixtureEventStore>
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
    priorSchedules = new Map((scheduling?.schedules ?? []).map((s) => [s.token.id, s.status]))
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

  /**
   * MOVE THE CLOCK, fire what that made due, settle.
   *
   * There is no `realTime` flag and no runtime refusal any more: whether this
   * fixture can move time is settled by whether it was given a clock it can
   * move, and that answer is in its TYPE — a scenario that advances does not
   * typecheck against a fixture that cannot. The only defensive line left is
   * for JavaScript callers.
   */
  async function advance(duration: Duration): Promise<void> {
    if (advanceable === undefined) {
      throw new Error(
        "@kronos-ts/test: this fixture cannot move time — it was not given a clock it can " +
          "move. Build one with `advanceableClock()` and pass it as `clock`, or use " +
          "`.await(until)` to wait for the world to catch up on its own.",
      )
    }
    advanceable.advance(duration)

    // Fired events go in through the OUTERMOST store, so the recorder sees
    // them. There is a book to ask only when the fixture built the log: a
    // host's own scheduler — a postgres poller, a kronosdb server — holds its
    // deadlines out of reach, and nothing here can make those fire early.
    if (scheduling !== undefined) {
      const due = scheduling.due()
      if (due.length > 0) await eventStore.append(due)
    }
    await quiesce()
  }

  /** What the recorders currently hold, minus the act's own message. */
  function observe(
    actIdentifier: string | undefined,
  ): Omit<Observed, "result" | "threw" | "thrown"> {
    return {
      events: [...eventStore.appended],
      commands: commandBus.dispatched.filter((m) => m.identifier !== actIdentifier),
      schedules: (scheduling?.schedules ?? []).filter(
        (s) => priorSchedules.get(s.token.id) !== s.status,
      ),
    }
  }

  return {
    async stop(): Promise<void> {
      await taken
      await app.stop()
    },
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
        if (step.kind === "advance") {
          await advance(step.duration)
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
        // THE SCENARIO SAYS WHICH. `then` judges the world as it stands: what
        // is not true now will not become true in a deterministic scope, and
        // waiting for it would only make failures slow. `await` judges the
        // same claims until they hold, which is what a world that keeps
        // working after the act — a projection behind a database, a processor
        // on another node — actually needs.
        if (scenario.judgement === "once" || Date.now() > deadline) {
          throw new ScenarioAssertionError(failure)
        }
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
