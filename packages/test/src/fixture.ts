import { emptyMetadata, generateIdentifier, qualifiedNameToString, tagsOf } from "@kronos-ts/core"
import type {
  App,
  CommandBus,
  CommandMessage,
  EventMessage,
  EventStore,
  QueryBus,
  QueryMessage,
  RunningProcessor,
} from "@kronos-ts/core"
import type { Duration } from "./clock.js"
import type { CommandRecording, EventRecording, ScheduleRecord, ScheduleRecording } from "./recording.js"
import { type Action, type EventValue, isAny } from "./values.js"

// ---------------------------------------------------------------------------
// The fixture: a running app, read through what it reports.
//
// A test composes its app the way `main.ts` does — `kronos({ … })` over entries
// that carry their stores and buses — with the infrastructure wrapped once at
// the top: the log in `recordingEventStore`, the command bus in
// `recordingCommandBus`. The fixture is handed the app and nothing else. It
// finds the log and the buses on `app.eventStores`, `app.commandBuses` and
// `app.queryBuses`, takes the app's processors back from `kronos`, folds history
// into them, performs the act, settles them, and reads the recorders.
// ---------------------------------------------------------------------------

/** An app whose log and command bus are recorded: what `fixture` accepts. */
export type RecordedApp = App<EventStore & EventRecording, CommandBus<any> & CommandRecording, QueryBus<any>>

/** What an act did, before it is judged. */
export type Acted = {
  readonly result: unknown
  readonly threw: boolean
  readonly thrown: unknown
  readonly actIdentifier: string | undefined
}

/** The running world a scenario drives. Internal. */
export type World = {
  readonly app: App
  /** Facts that already happened: the processors fold them, nothing they set in motion gets out. */
  history(events: ReadonlyArray<EventValue<any>>): Promise<void>
  advance(duration: Duration): Promise<void>
  act(action: Action): Promise<Acted>
  /** Start a new observation window: what is recorded from here on is the next act's. */
  window(): void
  observe(actIdentifier: string | undefined): {
    events: ReadonlyArray<EventMessage>
    commands: ReadonlyArray<CommandMessage>
    schedules: ReadonlyArray<ScheduleRecord> | undefined
  }
  quiesce(): Promise<void>
  close(): Promise<void>
}

const SETTLE_WITHIN = 5000

/** Take the app over: find its log and buses, check they are recorded, park its processors. */
export async function openWorld(app: App): Promise<World> {
  if (typeof app?.processors?.values !== "function" || typeof app?.eventStores?.values !== "function") {
    throw usage("`fixture(app)` takes what `kronos(…)` returned: `fixture(kronos({ … }))`.")
  }
  const eventStore = only(app.eventStores, "event store", "event stores") as
    | (EventStore & EventRecording & Partial<ScheduleRecording>)
    | undefined
  const commandBus = only(app.commandBuses, "command bus", "command buses") as
    | (CommandBus<any> & CommandRecording)
    | undefined
  const queryBus = only(app.queryBuses, "query bus", "query buses")

  if (eventStore !== undefined && (typeof eventStore.reset !== "function" || !("appended" in eventStore))) {
    throw usage(
      "the app's event store is not recorded. Wrap the log once, where the app's infrastructure is built: " +
        "`const eventStore = recordingEventStore(inMemoryEventStore())`, and build the app with that object.",
    )
  }
  if (commandBus !== undefined && typeof commandBus.hold !== "function") {
    throw usage(
      "the app's command bus is not recorded. Wrap the bus once, where the app's infrastructure is built: " +
        "`const commandBus = recordingCommandBus(localCommandBus(uow))`, and build the app with that object.",
    )
  }

  const processors: ReadonlyArray<RunningProcessor> = [...app.processors.values()]
  // History and arrivals are stamped from the clock the log's deadlines wait
  // for, which is the one the app's tasks are built on; without one, from now.
  const clock = eventStore?.clock ?? Date.now

  function log(): EventStore & EventRecording & Partial<ScheduleRecording> {
    if (eventStore === undefined) {
      throw usage("this app has no event store, so there is no log to put history in or an event into.")
    }
    return eventStore
  }

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
    priorSchedules = new Map((eventStore?.schedules ?? []).map((s) => [s.token.id, s.status]))
  }

  function window(): void {
    eventStore?.reset()
    commandBus?.reset()
    markSchedules()
  }

  function hold(on: boolean): void {
    commandBus?.hold(on)
    eventStore?.hold(on)
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
   * and repeat until nothing moves. With one store reported, it is the log every
   * processor reads.
   */
  async function quiesce(): Promise<void> {
    if (processors.length === 0) return
    const deadline = Date.now() + SETTLE_WITHIN
    let previous = ""
    for (;;) {
      const head = await log().getHeadPosition()
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
            "its processor where it is.",
        )
      }
      await settle()
    }
  }

  let closed = false

  return {
    app,

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
      const eventStore = log()
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
      const eventStore = log()
      if (typeof eventStore.advance !== "function") {
        throw usage(
          "`advance` needs a log that holds its deadlines against a clock it can move. Give the recorder one: " +
            "`const clock = advanceableClock(); const eventStore = recordingEventStore(store, { clock })`, " +
            "and build the app's tasks on the same clock: `() => unitOfWork(clock)`.",
        )
      }
      await eventStore.advance(duration)
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
          if (commandBus === undefined) throw usage("a command act needs a command bus, and this app has none.")
          result = await commandBus.dispatch(message)
        } else if (action.kind === "query") {
          if (queryBus === undefined) throw usage("a query act needs a query bus, and this app has none.")
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
          await log().append([toEventMessage(action)])
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
        events: [...(eventStore?.appended ?? [])],
        commands: (commandBus?.dispatched ?? []).filter((m) => m.identifier !== actIdentifier),
        schedules: eventStore?.schedules?.filter((s) => priorSchedules.get(s.token.id) !== s.status),
      }
    },

    quiesce,

    async close() {
      if (closed) return
      closed = true
      await app.stop()
    },
  }
}

/**
 * The one object of a kind the app reports, or none. More than one is refused:
 * a fixture drives one module, so it is built from that module's configuration.
 */
function only<T>(reported: ReadonlySet<T>, one: string, many: string): T | undefined {
  if (reported.size > 1) {
    throw usage(
      `this app has ${reported.size} ${many}, and a fixture drives one. Build the app you hand it from the ` +
        `configuration of the module under test, so it has one event store, one command bus and one query bus.`,
    )
  }
  return reported.values().next().value
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
