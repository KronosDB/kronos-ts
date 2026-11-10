/**
 * THE TYPE TEST FOR WHAT `fixture(app)` DEMANDS. Listed in the root
 * `tsconfig.json` `files` array, so `tsc --noEmit` judges it; a
 * `@ts-expect-error` that stops erroring turns that gate red.
 *
 * What it pins, all read off the app's own type (`App.eventStores`, …):
 * - an app whose log or command bus is not recorded does not compile;
 * - the query bus need not be recorded;
 * - a processor reading an unrecorded log counts as an unrecorded log;
 * - `advance`, `scheduled` and `cancelled` exist only when the log was given a
 *   clock — `recordingEventStore(store, { clock })`.
 */
import {
  commandHandler,
  eventHandler,
  eventProcessor,
  inMemoryEventStore,
  inMemoryTokenStore,
  kronos,
  localCommandBus,
  localQueryBus,
  qn,
  command as commandDescriptor,
  event as eventDescriptor,
  unitOfWork,
  type CommandBus,
  type EventStore,
} from "@kronos-ts/core"
import { z } from "zod"
import { advanceableClock } from "../clock.js"
import { recordingCommandBus, recordingEventStore, recordingQueryBus } from "../recording.js"
import { fixture } from "../scenario.js"
import { cancelled, command, event, events, type Outcome, scheduled } from "../values.js"

const Open = commandDescriptor({ name: qn("t", "Open"), payload: z.object({ id: z.string() }) })
const Opened = eventDescriptor({ name: qn("t", "Opened"), payload: z.object({ id: z.string() }), tags: { id: (p) => p.id } })

const open = commandHandler(Open, async ({ payload }, ctx) => {
  ctx.append(Opened, payload)
})
const onOpened = eventHandler(Opened, async () => {})

const clock = advanceableClock()
const uow = () => unitOfWork(clock)
const queryBus = localQueryBus(uow)
const recordedBus = recordingCommandBus(localCommandBus(uow))

/** The app as a test builds it: one log, one command bus, wrapped once at the top. */
function appOver<S extends EventStore, B extends CommandBus<any> = typeof recordedBus>(
  eventStore: S,
  commandBus: B = recordedBus as unknown as B,
) {
  return kronos({
    commandHandlers: [{ ...open, eventStore, commandBus, queryBus }],
    eventHandlers: [
      { ...onOpened, commandBus, queryBus, processor: eventProcessor("views", eventStore, inMemoryTokenStore(), uow) },
    ],
  })
}

const opened = events(event(Opened, { id: "1" }))
const mine = ({ events }: Outcome) => void events

// ---------------------------------------------------------------------------
// (a) Recorded, no clock: scenarios, but no time.
// ---------------------------------------------------------------------------

const recorded = fixture(appOver(recordingEventStore(inMemoryEventStore())))
export const recordedRuns = recorded.when(command(Open, { id: "1" })).then(opened, mine)

// @ts-expect-error — `advance` needs a log that holds its deadlines against a clock
export const noAdvance = recorded.advance(1_000)

// @ts-expect-error — `scheduled` claims a deadline, and this log holds none
export const noScheduled = recorded.when(command(Open, { id: "1" })).then(scheduled(event(Opened, { id: "1" }), 1_000))

// @ts-expect-error — nor does `cancelled`
export const noCancelled = recorded.when(command(Open, { id: "1" })).then(cancelled(event(Opened, { id: "1" })))

// ---------------------------------------------------------------------------
// (b) Recorded, with a clock: time, and claims about deadlines.
// ---------------------------------------------------------------------------

const timed = fixture(appOver(recordingEventStore(inMemoryEventStore(), { clock })))
export const timedRuns = timed
  .advance(1_000)
  .when(command(Open, { id: "1" }))
  .advance(30_000)
  .then(opened, mine, scheduled(event(Opened, { id: "1" }), 1_000), cancelled(event(Opened, { id: "1" })))

/** ✓ `.and` keeps the fixture's time. */
export const timedContinues = timedRuns.and.advance(1_000)

/** ✓ The app may be handed as a promise, and keeps its type. */
export const promised = fixture(Promise.resolve(appOver(recordingEventStore(inMemoryEventStore(), { clock })))).advance(1)

/** ✓ The query bus need not be recorded — and may be. */
export const recordedQueries = fixture(
  kronos({ commandHandlers: [{ ...open, eventStore: recordingEventStore(inMemoryEventStore()), commandBus: recordedBus, queryBus: recordingQueryBus(queryBus) }] }),
)

// ---------------------------------------------------------------------------
// (c) Not recorded: refused.
// ---------------------------------------------------------------------------

// @ts-expect-error — the app's log is not recorded
export const unrecordedLog = fixture(appOver(inMemoryEventStore()))

// @ts-expect-error — the app's command bus is not recorded
export const unrecordedBus = fixture(appOver(recordingEventStore(inMemoryEventStore()), localCommandBus(uow)))

const log = recordingEventStore(inMemoryEventStore())
export const unrecordedProcessorLog = fixture(
  // @ts-expect-error — a processor reads a log that is not recorded
  kronos({
    commandHandlers: [{ ...open, eventStore: log, commandBus: recordedBus, queryBus }],
    eventHandlers: [
      {
        ...onOpened,
        commandBus: recordedBus,
        queryBus,
        processor: eventProcessor("views", inMemoryEventStore(), inMemoryTokenStore(), uow),
      },
    ],
  }),
)
