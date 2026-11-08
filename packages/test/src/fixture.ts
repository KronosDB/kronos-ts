import { emptyMetadata, generateIdentifier, qualifiedNameToString, tagsOf } from "@kronos-ts/core"
import type {
  App as KronosApp,
  CommandBus,
  CommandMessage,
  EventMessage,
  EventStore,
  QueryBus,
  QueryMessage,
  RunningProcessor,
} from "@kronos-ts/core"
import type { CommandRecording, EventRecording, ScheduleRecord, ScheduleRecording } from "./recording.js"
import { type Action, type Duration, type EventValue, isAny } from "./values.js"

// ---------------------------------------------------------------------------
// The fixture: a running app, and the recorders it was built with.
//
// A test composes its app the way `main.ts` does — `kronos({ … })` over entries
// that carry their stores and buses — and wraps in a recorder what the fixture
// should observe. The fixture builds nothing. It drives what it is handed: it
// takes the app's processors back from `kronos`, folds history into them,
// performs the act, settles them, and reads the recorders.
// ---------------------------------------------------------------------------

/**
 * The instant an {@link advanceableClock} starts at when nobody says otherwise.
 *
 * A FIXED instant rather than the wall clock, because a timestamp a test can
 * predict is worth more than one that is technically current: an assertion can
 * name `FIXTURE_EPOCH + 30_000` and mean it.
 */
export const FIXTURE_EPOCH = Date.UTC(2024, 0, 1)

/** A CLOCK YOU CAN MOVE — a clock, plus the one verb that moves it. */
export type AdvanceableClock = (() => number) & {
  /** Move time forward. What was due before is due now. */
  advance(duration: Duration): void
}

/**
 * A clock that starts at `from` and moves only when you say so. Build the app's
 * tasks on it — `() => unitOfWork(clock)` — and hand it to the fixture, which
 * moves it on `advance`.
 *
 * It is a CLOSURE over its own offset: everything that captured the arrow sees
 * a move, and whatever captured a NUMBER is frozen where it was built.
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

/**
 * WHAT A FIXTURE IS HANDED: the running app and the objects it was built with.
 *
 * - `app` — what `kronos(…)` returned. Its processors are the ones the fixture
 *   drives.
 * - `eventStore` — the log the app's entries and processors use, wrapped in
 *   `recordingEventStore`. For `advance` and the `scheduled`/`cancelled` claims,
 *   its scheduling tier is `controllableSchedulingEventStore(…, clock)`.
 * - `commandBus` — the bus the command handlers subscribed to, wrapped in
 *   `recordingCommandBus`.
 * - `queryBus` — the bus the query handlers subscribed to. Needed for a query act.
 * - `clock` — the {@link AdvanceableClock} the app's tasks read. Needed for `advance`.
 * - `within` — when given, claims that do not hold are re-judged until they do
 *   or this many ms pass: for an app whose effects land where the fixture
 *   cannot watch them settle. Absent, a claim is judged once.
 * - `close` — released once the scenario is judged, after the app stops.
 *
 * Anything else rides along to `outcome.app` — a pool, a database client.
 */
export type FixtureParts = {
  readonly app: KronosApp
  readonly eventStore: EventStore & Partial<EventRecording>
  readonly commandBus: CommandBus<any> & Partial<CommandRecording>
  readonly queryBus?: QueryBus<any>
  readonly clock?: () => number
  readonly within?: Duration
  readonly close?: () => Promise<void> | void
  readonly [extra: string]: unknown
}

/** What an act did, before it is judged. */
export type Acted = {
  readonly result: unknown
  readonly threw: boolean
  readonly thrown: unknown
  readonly actIdentifier: string | undefined
}

/** The running world a scenario drives. Internal. */
export type World = {
  readonly parts: FixtureParts
  /** Facts that already happened: the processors fold them, nothing they set in motion gets out. */
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

const SETTLE_WITHIN = 5000

/** Take the app over: check what it was handed, park its processors. */
export async function openWorld(parts: FixtureParts): Promise<World> {
  const eventStore = parts.eventStore as EventStore & EventRecording & Partial<ScheduleRecording>
  const commandBus = parts.commandBus as CommandBus<any> & CommandRecording
  if (typeof parts.app?.processors?.values !== "function") {
    throw usage("`app` is not what `kronos(…)` returned. Pass the running app: `fixture({ app: kronos({ … }), … })`.")
  }
  if (typeof (eventStore as Partial<EventRecording>).reset !== "function" || !("appended" in eventStore)) {
    throw usage(
      "the `eventStore` is not recorded. Wrap the log the app's entries and processors use: " +
        "`const eventStore = recordingEventStore(inMemoryEventStore())`, and build the app with that object.",
    )
  }
  if (typeof (commandBus as Partial<CommandRecording>).hold !== "function") {
    throw usage(
      "the `commandBus` is not recorded. Wrap the bus the command handlers subscribe to: " +
        "`const commandBus = recordingCommandBus(localCommandBus(uow))`, and build the app with that object.",
    )
  }

  const processors: ReadonlyArray<RunningProcessor> = [...parts.app.processors.values()]
  const clock = parts.clock ?? Date.now

  // `kronos` starts every processor. The fixture drives them by hand, so the
  // first thing it does is take them back.
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

  let priorSchedules = new Map<string, string>()
  function markSchedules(): void {
    priorSchedules = new Map((eventStore.schedules ?? []).map((s) => [s.token.id, s.status]))
  }

  function window(): void {
    eventStore.reset()
    commandBus.reset()
    markSchedules()
  }

  function hold(on: boolean): void {
    commandBus.hold(on)
    eventStore.hold?.(on)
  }

  function toEventMessage(value: EventValue<any>): EventMessage {
    const name = qualifiedNameToString(value.descriptor.name)
    if (containsHole(value.payload)) {
      throw usage(
        `event(${name}, …) contains \`any()\`, but this event is a FACT — a \`given\` fills the log and ` +
          "a `when` arrives in it, so every field has to have a value. `any()` is only meaningful in " +
          "`then`, where it declines to pin what something else produced.",
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

  /**
   * Drive every processor to the head of the log, let what it dispatched settle,
   * and repeat until nothing moves. The app's processors are expected to read
   * the `eventStore` the fixture was handed.
   */
  async function quiesce(): Promise<void> {
    if (processors.length === 0) return
    const deadline = Date.now() + SETTLE_WITHIN
    let previous = ""
    for (;;) {
      const head = await eventStore.getHeadPosition()
      const settled = processors.every((p) => {
        const status = p.status()
        return status.caughtUp && status.position >= head
      })
      const current = String(head)
      if (settled && current === previous) return
      previous = current
      if (Date.now() > deadline) {
        const behind = processors
          .map((p) => `"${p.name}" at ${p.status().position} of ${head}${p.status().caughtUp ? "" : " (busy)"}`)
          .join(", ")
        throw usage(
          `the app's event handlers did not go quiet within ${SETTLE_WITHIN}ms. ${behind}. An event handler ` +
            "that dispatches a command whose events re-trigger it never settles, one that keeps failing holds " +
            "its processor where it is, and a processor reading another log than the fixture's `eventStore` " +
            "is never seen to catch up.",
        )
      }
      await settle()
    }
  }

  let closed = false

  return {
    parts,

    /**
     * HISTORY: append the facts, then hand them to every processor as a REPLAY —
     * first a reset to the head, so each processor has "got to" the end of the
     * history, then back to where it began, which core turns into a replay up to
     * that head. The processors fold the facts with `ctx.isReplay()` true, the
     * signal a production replay gives, while the recorders HOLD whatever an
     * event handler would set in motion: a command it dispatches is not sent, a
     * deadline it arms is not armed.
     */
    async history(events) {
      if (events.length === 0) return
      await parkAll()
      const before = await eventStore.getHeadPosition()
      await eventStore.append(events.map(toEventMessage))
      const head = await eventStore.getHeadPosition()
      for (const processor of processors) {
        await processor.resetTokens(head)
        await processor.resetTokens(before)
      }
      hold(true)
      try {
        await resumeAll()
        await quiesce()
      } finally {
        hold(false)
      }
      window()
    },

    async advance(duration) {
      const movable = parts.clock as AdvanceableClock | undefined
      if (typeof movable?.advance !== "function") {
        throw usage(
          "`advance` needs a clock it can move. Build the app's tasks on `advanceableClock()` — " +
            "`const clock = advanceableClock(); const uow = () => unitOfWork(clock)` — and pass `clock` to the fixture.",
        )
      }
      if (typeof eventStore.due !== "function") {
        throw usage(
          "`advance` needs a scheduling tier it can fire. Put `controllableSchedulingEventStore(…, clock)` " +
            "under the recorder: `recordingEventStore(controllableSchedulingEventStore(inMemoryEventStore(), clock))`.",
        )
      }
      movable.advance(duration)
      const due = eventStore.due()
      if (due.length > 0) await eventStore.append(due)
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
          // Built here rather than through `send`, so the fixture knows its
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
          if (parts.queryBus === undefined) {
            throw usage("a query act needs the `queryBus` the query handlers subscribed to. Pass it to the fixture.")
          }
          const message: QueryMessage = {
            kind: "query",
            identifier: generateIdentifier(),
            name: action.descriptor.name,
            payload: action.payload,
            metadata: action.metadata ?? emptyMetadata(),
          }
          actIdentifier = message.identifier
          result = await parts.queryBus.query(message)
        } else {
          // An event ARRIVES: appended, and the automations DO react.
          await eventStore.append([toEventMessage(action)])
        }
      } catch (error) {
        if (error instanceof Error && error.message.startsWith("@kronos-ts/test:")) throw error
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
        commands: commandBus.dispatched.filter((m) => m.identifier !== actIdentifier),
        schedules: (eventStore.schedules ?? []).filter((s) => priorSchedules.get(s.token.id) !== s.status),
      }
    },

    quiesce,

    async close() {
      if (closed) return
      closed = true
      try {
        await parts.app.stop()
      } finally {
        await parts.close?.()
      }
    },
  }
}

function usage(text: string): Error {
  return new Error(`@kronos-ts/test: ${text}`)
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
