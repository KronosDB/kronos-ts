import assert from "node:assert/strict"
import { after, test } from "node:test"
import {
  eventHandler,
  eventProcessor,
  inMemoryEventStore,
  inMemoryTokenStore,
  kronos,
  localCommandBus,
  localQueryBus,
  unitOfWork,
  withNamespace,
} from "@kronos-ts/core"
import {
  advanceableClock,
  command,
  event,
  events,
  fixture,
  noEvents,
  query,
  recordingCommandBus,
  recordingEventStore,
  result,
  ScenarioAssertionError,
  scheduled,
} from "@kronos-ts/test"
import { z } from "zod"
import { AddTool, GetInventory, RentTool, ToolAdded, ToolRented, addTool, memoryInventory, rentTool } from "./toolstore.mjs"

const SEVEN_DAYS = 7 * 24 * 60 * 60 * 1000
const RentalOverdue = withNamespace("toolstore").event("RentalOverdue", {
  payload: z.object({ toolId: z.string(), customerId: z.string() }),
  tags: { toolId: (payload) => payload.toolId },
})
const armOverdue = eventHandler(ToolRented, async ({ payload }, ctx) => {
  await ctx.scheduleAfter(RentalOverdue, payload, SEVEN_DAYS)
})

function toolStore() {
  const clock = advanceableClock()
  const uow = () => unitOfWork(clock)
  const eventStore = recordingEventStore(inMemoryEventStore(), { clock })
  const commandBus = recordingCommandBus(localCommandBus(uow))
  const queryBus = localQueryBus(uow)
  const inventoryView = eventProcessor("inventory-view", eventStore, inMemoryTokenStore(), uow)
  const deadlines = eventProcessor("rental-deadlines", eventStore, inMemoryTokenStore(), uow)
  const inventory = memoryInventory()
  return kronos({
    commandHandlers: [addTool, rentTool].map((entry) => ({ ...entry, eventStore, commandBus, queryBus })),
    eventHandlers: [
      ...inventory.eventHandlers.map((entry) => ({ ...entry, commandBus, queryBus, processor: inventoryView })),
      { ...armOverdue, eventStore, commandBus, queryBus, processor: deadlines },
    ],
    queryHandlers: [{ ...inventory.queryHandler, queryBus }],
  })
}

const app = toolStore()
const scenarios = fixture(app)
after(() => app.stop())

test("a scenario claims the result and the appended events", async () => {
  await scenarios
    .given(event(ToolAdded, { toolId: "t-1", name: "Cordless drill" }))
    .when(command(RentTool, { toolId: "t-1", customerId: "ada" }))
    .then(result({ ok: true }), events(event(ToolRented, { toolId: "t-1", customerId: "ada" })))
})

test("a refusal is a result with no events", async () => {
  await scenarios
    .when(command(RentTool, { toolId: "t-9", customerId: "ada" }))
    .then(result({ ok: false, rejection: "TOOL_UNAVAILABLE", toolId: "t-9" }), noEvents())
})

test("an event handler's schedule is claimed and fires when the clock passes it", async () => {
  await scenarios
    .when(event(ToolRented, { toolId: "t-5", customerId: "grace" }))
    .then(scheduled(event(RentalOverdue, { toolId: "t-5", customerId: "grace" }), SEVEN_DAYS))
})

test("a storyline follows the read model as events are appended", async () => {
  const fresh = toolStore()
  try {
    await fixture(fresh)
      .storyline(query(GetInventory, {}))
      .shows([])
      .after(event(ToolAdded, { toolId: "t-2", name: "Tile saw" }))
      .shows([{ toolId: "t-2", available: true }])
      .after(event(ToolRented, { toolId: "t-2", customerId: "linus" }))
      .shows([{ toolId: "t-2", available: false }])
  } finally {
    await fresh.stop()
  }
})

test("a failed claim rejects with a ScenarioAssertionError that names the difference", async () => {
  await assert.rejects(
    scenarios.when(command(AddTool, { toolId: "t-3", name: "Table saw" })).then(events(event(ToolAdded, { toolId: "t-3", name: "Drill" }))),
    (error) => error instanceof ScenarioAssertionError && error.message.includes('name: expected "Drill"'),
  )
})
