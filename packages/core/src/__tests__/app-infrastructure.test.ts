import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, command, event } from "../messaging/messages.js"
import { query } from "../query-handling/query.js"
import { inMemoryEventStore } from "../event-sourcing/in-memory.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import { localCommandBus } from "../command-handling/local-bus.js"
import { commandHandler } from "../command-handling/handler.js"
import { localQueryBus } from "../query-handling/local-bus.js"
import { queryHandler } from "../query-handling/handler.js"
import { eventHandler } from "../event-processing/handler.js"
import { inMemoryTokenStore } from "../event-processing/token-store.js"
import { eventProcessor } from "../event-processing/processor.js"
import { interceptingCommandBus } from "../index.js"
import { unitOfWork } from "../unit-of-work/unit-of-work.js"
import { kronos } from "../kronos.js"

// ---------------------------------------------------------------------------
// What an app reports: the stores and buses its entries carried, collected by
// object identity from every entry and every processor.
// ---------------------------------------------------------------------------

const Add = command({ name: qn("t", "Add"), payload: z.object({ id: z.string() }) })
const Remove = command({ name: qn("t", "Remove"), payload: z.object({ id: z.string() }) })
const Added = event({ name: qn("t", "Added"), payload: z.object({ id: z.string() }), tags: { id: (p) => p.id } })
const Count = query({ name: qn("t", "Count"), payload: z.object({}), result: z.number() })

const add = commandHandler(Add, async () => {})
const remove = commandHandler(Remove, async () => {})
const count = queryHandler(Count, async () => 0)
const onAdded = eventHandler(Added, async () => {})

describe("App reports its infrastructure", () => {
  it("reports each shared object once, however many entries carry it", async () => {
    const eventStore = inMemoryEventStore()
    const commandBus = localCommandBus(unitOfWork)
    const queryBus = localQueryBus(unitOfWork)
    const processor = eventProcessor("views", eventStore, inMemoryTokenStore(), unitOfWork)
    const app = kronos({
      commandHandlers: [add, remove].map((h) => ({ ...h, eventStore, commandBus, queryBus })),
      queryHandlers: [{ ...count, eventStore, queryBus }],
      eventHandlers: [{ ...onAdded, eventStore, commandBus, queryBus, processor }],
    })
    try {
      expect([...app.eventStores]).toEqual([eventStore])
      expect([...app.commandBuses]).toEqual([commandBus])
      expect([...app.queryBuses]).toEqual([queryBus])
    } finally {
      await app.stop()
    }
  })

  it("reports the object an entry carries — the outermost wrapper, not what it wraps", async () => {
    const eventStore = inMemoryEventStore()
    const local = localCommandBus(unitOfWork)
    const commandBus = interceptingCommandBus(local, (m) => m)
    const queryBus = localQueryBus(unitOfWork)
    const app = kronos({ commandHandlers: [{ ...add, eventStore, commandBus, queryBus }] })
    try {
      expect([...app.commandBuses]).toEqual([commandBus])
      expect(app.commandBuses.has(local)).toBe(false)
    } finally {
      await app.stop()
    }
  })

  it("reports a processor's log even when no entry carries it", async () => {
    const entryStore = inMemoryEventStore()
    const processorStore = inMemoryEventStore()
    const commandBus = localCommandBus(unitOfWork)
    const queryBus = localQueryBus(unitOfWork)
    const processor = eventProcessor("views", processorStore, inMemoryTokenStore(), unitOfWork)
    const app = kronos({
      commandHandlers: [{ ...add, eventStore: entryStore, commandBus, queryBus }],
      eventHandlers: [{ ...onAdded, commandBus, queryBus, processor }],
    })
    try {
      expect([...app.eventStores]).toEqual([entryStore, processorStore])
    } finally {
      await app.stop()
    }
  })

  it("allows several of each: modules that keep their own, and two wrappers over one bus", async () => {
    const billingLog = inMemoryEventStore()
    const shippingLog = inMemoryEventStore()
    const local = localCommandBus(unitOfWork)
    const billingBus = interceptingCommandBus(local, (m) => m)
    const shippingBus = interceptingCommandBus(local, (m) => m)
    const queryBus = localQueryBus(unitOfWork)
    const app = kronos({
      commandHandlers: [
        { ...add, eventStore: billingLog, commandBus: billingBus, queryBus },
        { ...remove, eventStore: shippingLog, commandBus: shippingBus, queryBus },
      ],
    })
    try {
      expect([...app.eventStores]).toEqual<EventStore[]>([billingLog, shippingLog])
      expect([...app.commandBuses]).toEqual([billingBus, shippingBus])
      expect([...app.queryBuses]).toEqual([queryBus])
    } finally {
      await app.stop()
    }
  })

  it("reports nothing for a list that is absent", async () => {
    const queryBus = localQueryBus(unitOfWork)
    const app = kronos({ queryHandlers: [{ ...count, queryBus }] })
    try {
      expect(app.eventStores.size).toBe(0)
      expect(app.commandBuses.size).toBe(0)
      expect([...app.queryBuses]).toEqual([queryBus])
    } finally {
      await app.stop()
    }
  })
})
