/**
 * THE TYPE TEST FOR WHAT AN APP REPORTS. Listed in the root `tsconfig.json`
 * `files` array, so `tsc --noEmit` judges it; a `@ts-expect-error` that stops
 * erroring turns that gate red.
 *
 * What it pins: `App.eventStores`, `commandBuses` and `queryBuses` carry the
 * TYPES of what the entries were wired with, every capability included —
 * a processor's log too, through every wrapper on the processor — and a list
 * that is absent contributes nothing (`never`), not "some bus".
 */
import { z } from "zod"
import { qn, command, event } from "../messaging/messages.js"
import { query } from "../query-handling/query.js"
import { inMemoryEventStore } from "../event-sourcing/in-memory.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import type { ScheduleStoreCapability } from "../event-scheduling/scheduler.js"
import { inMemorySchedulingEventStore } from "../event-scheduling/in-memory-scheduling-event-store.js"
import type { CommandBus } from "../command-handling/bus.js"
import { localCommandBus } from "../command-handling/local-bus.js"
import { commandHandler } from "../command-handling/handler.js"
import type { QueryBus } from "../query-handling/bus.js"
import { localQueryBus } from "../query-handling/local-bus.js"
import { queryHandler } from "../query-handling/handler.js"
import { eventHandler } from "../event-processing/handler.js"
import { inMemoryTokenStore } from "../event-processing/token-store.js"
import { inMemoryDeadLetterQueue } from "../event-processing/dead-letter-queue.js"
import { batched, concurrent, deadLettering, eventProcessor, segmented } from "../event-processing/processor.js"
import { unitOfWork } from "../unit-of-work/unit-of-work.js"
import { kronos, type App } from "../kronos.js"

/** A capability a wrapper adds, the way a recorder or a scheduling tier does. */
type Marked = { readonly marked: true }
declare function marked<E extends EventStore>(store: E): E & Marked
declare function markedBus<B extends CommandBus<any>>(bus: B): B & Marked
declare function markedQueryBus<B extends QueryBus<any>>(bus: B): B & Marked

const Add = command({ name: qn("t", "Add"), payload: z.object({ id: z.string() }) })
const Added = event({ name: qn("t", "Added"), payload: z.object({ id: z.string() }), tags: { id: (p) => p.id } })
const Count = query({ name: qn("t", "Count"), payload: z.object({}), result: z.number() })

const add = commandHandler(Add, async () => {})
const count = queryHandler(Count, async () => 0)
const onAdded = eventHandler(Added, async () => {})

const store = marked(inMemoryEventStore())
const commandBus = markedBus(localCommandBus(unitOfWork))
const queryBus = markedQueryBus(localQueryBus(unitOfWork))
const byId = (e: { identifier: string }) => e.identifier

// ---------------------------------------------------------------------------
// (a) One of each, shared: each is reported with what its wrapper added.
// ---------------------------------------------------------------------------

const processor = eventProcessor("views", store, inMemoryTokenStore(), unitOfWork)
const app = kronos({
  commandHandlers: [{ ...add, eventStore: store, commandBus, queryBus }],
  queryHandlers: [{ ...count, queryBus }],
  eventHandlers: [{ ...onAdded, commandBus, queryBus, processor }],
})

export const storesKeepTheirType: ReadonlySet<EventStore & Marked> = app.eventStores
export const commandBusesKeepTheirType: ReadonlySet<CommandBus<any> & Marked> = app.commandBuses
export const queryBusesKeepTheirType: ReadonlySet<QueryBus<any> & Marked> = app.queryBuses

/** ✓ A reported app is still an `App`: code that takes the plain type keeps taking it. */
export const stillAnApp: App = app

// ---------------------------------------------------------------------------
// (b) A processor's log is reported with ITS type, through every wrapper.
// ---------------------------------------------------------------------------

const wrappedProcessor = segmented(
  deadLettering(concurrent(batched(eventProcessor("views", store, inMemoryTokenStore(), unitOfWork), 100), byId), inMemoryDeadLetterQueue(), byId),
  2,
  byId,
)
const processorOnly = kronos({ eventHandlers: [{ ...onAdded, commandBus, queryBus, processor: wrappedProcessor }] })
export const processorStoreKeepsItsType: ReadonlySet<EventStore & Marked> = processorOnly.eventStores

/** A processor over a log that does NOT claim the capability makes the app's stores not all claim it. */
const plainProcessor = eventProcessor("plain", inMemoryEventStore(), inMemoryTokenStore(), unitOfWork)
const mixed = kronos({
  commandHandlers: [{ ...add, eventStore: store, commandBus, queryBus }],
  eventHandlers: [{ ...onAdded, commandBus, queryBus, processor: plainProcessor }],
})
// @ts-expect-error — the processor's log is a plain store, so not every store is Marked
export const mixedIsNotAllMarked: ReadonlySet<EventStore & Marked> = mixed.eventStores

// ---------------------------------------------------------------------------
// (c) Several stores: a union, each member with its own capabilities, and no
// boot error. Two wrappers that share nothing are not collapsed to a floor.
// ---------------------------------------------------------------------------

const scheduling = marked(inMemorySchedulingEventStore(inMemoryEventStore()))
const twoStores = kronos({
  commandHandlers: [
    { ...add, eventStore: store, commandBus, queryBus },
    { ...add, eventStore: scheduling, commandBus, queryBus },
  ],
})
export const bothMarked: ReadonlySet<EventStore & Marked> = twoStores.eventStores
// @ts-expect-error — only one of the two can schedule
export const notBothScheduling: ReadonlySet<EventStore & ScheduleStoreCapability> = twoStores.eventStores

// ---------------------------------------------------------------------------
// (d) An absent list reports nothing: an app of only query handlers has no
// command bus and, without a log on its entries, no store.
// ---------------------------------------------------------------------------

type Element<T> = T extends ReadonlySet<infer I> ? I : never
type IsNever<T> = [T] extends [never] ? true : false

const readsOnly = kronos({ queryHandlers: [{ ...count, queryBus }] })
export const noCommandBus: IsNever<Element<typeof readsOnly.commandBuses>> = true
export const noStore: IsNever<Element<typeof readsOnly.eventStores>> = true
export const butAQueryBus: IsNever<Element<typeof readsOnly.queryBuses>> = false
