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
// `app.queryBuses`, appends history, performs the act, settles the app's
// processors after each, and reads the recorders.
//
// The processors are the app's own and keep running, as `kronos` started them.
// The fixture never stops, resets or replays them: a `given` event reaches them
// the way any appended event does, and what they do with it happens.
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
  /** Facts the act starts from: appended, and settled like any other event. */
  history(events: ReadonlyArray<EventValue<any>>): Promise<void>
  advance(duration: Duration): Promise<void>
  /** A fresh observation window, the act, and the settle after it. */
  act(action: Action): Promise<Acted>
  /** A new scenario: an empty deadline book, and a fresh observation window. */
  begin(): void
  observe(actIdentifier: string | undefined): {
    events: ReadonlyArray<EventMessage>
    commands: ReadonlyArray<CommandMessage>
    schedules: ReadonlyArray<ScheduleRecord> | undefined
  }
  quiesce(): Promise<void>
}

const SETTLE_WITHIN = 5000

/** Open the app: find its log and buses, check they are recorded, settle its processors. */
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
  if (commandBus !== undefined && (typeof commandBus.reset !== "function" || !("dispatched" in commandBus))) {
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

  let priorSchedules = new Map<string, string>()
  function markSchedules(): void {
    priorSchedules = new Map((eventStore?.schedules ?? []).map((s) => [s.token.id, s.status]))
  }

  function window(): void {
    eventStore?.reset()
    commandBus?.reset()
    markSchedules()
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
          .map((p) => {
            const status = p.status()
            const state = !status.running ? " (stopped)" : status.caughtUp ? "" : " (busy)"
            return `"${p.name}" at ${status.position} of ${head}${state}`
          })
          .join(", ")
        throw usage(
          `the app's event handlers did not go quiet within ${SETTLE_WITHIN}ms. ${behind}. An event handler ` +
            "that dispatches a command whose events re-trigger it never settles, one that keeps failing holds " +
            "its processor where it is, and a stopped processor never moves: hand the fixture the app `kronos` " +
            "started, before it is stopped.",
        )
      }
      await settle()
    }
  }

  // Whatever the log already holds is handled before the first scenario
  // opens its window, so none of it lands in that scenario's outcome.
  await quiesce()

  return {
    app,

    /**
     * HISTORY: append the facts through the recorded log and settle, exactly as
     * an event appended in production is handled. The processors handle them
     * live (`ctx.isReplay()` is false), and what an event handler sets in
     * motion happens: a command it dispatches is sent, a deadline it arms is
     * armed. None of it is in the outcome, because the act opens its own window.
     */
    async history(events) {
      if (events.length === 0) return
      await log().append(events.map(toEventMessage))
      await quiesce()
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
      // What happened before the act is not the act's: the window opens here.
      window()
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

    begin() {
      eventStore?.resetSchedules?.()
      window()
    },

    observe(actIdentifier) {
      return {
        events: [...(eventStore?.appended ?? [])],
        commands: (commandBus?.dispatched ?? []).filter((m) => m.identifier !== actIdentifier),
        schedules: eventStore?.schedules?.filter((s) => priorSchedules.get(s.token.id) !== s.status),
      }
    },

    quiesce,
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
