import assert from "node:assert/strict"
import { test } from "node:test"
import {
  commandHandler,
  consoleLogger,
  eventProcessor,
  inMemoryEventStore,
  inMemoryTokenStore,
  isConflict,
  kronos,
  localCommandBus,
  localQueryBus,
  loggingHandler,
  query,
  send,
  unitOfWork,
  validatingHandler,
} from "@kronos-ts/core"
import { AddTool, GetInventory, RentTool, addTool, caughtUp, memoryInventory, rentTool } from "./toolstore.mjs"

function toolStore() {
  const log = consoleLogger({ write: () => {} })
  const eventStore = inMemoryEventStore()
  const commandBus = localCommandBus(unitOfWork)
  const queryBus = localQueryBus(unitOfWork)
  const processor = eventProcessor("inventory-view", eventStore, inMemoryTokenStore(), unitOfWork)
  const inventory = memoryInventory()
  const app = kronos({
    commandHandlers: [addTool, rentTool].map((entry) => ({
      ...entry,
      handler: loggingHandler(validatingHandler(entry.handler, entry.descriptor), log),
      eventStore,
      commandBus,
      queryBus,
    })),
    eventHandlers: inventory.eventHandlers.map((entry) => ({ ...entry, commandBus, queryBus, processor })),
    queryHandlers: [{ ...inventory.queryHandler, queryBus }],
  })
  return { app, eventStore, commandBus, queryBus }
}

test("a command appends, is refused as a result, and lands once when sent again", async () => {
  const { app, eventStore, commandBus } = toolStore()
  try {
    assert.deepEqual(await send(commandBus, AddTool, { toolId: "t-1", name: "Cordless drill" }), { ok: true })
    assert.deepEqual(await send(commandBus, AddTool, { toolId: "t-1", name: "Cordless drill" }), { ok: true })
    assert.deepEqual(await send(commandBus, RentTool, { toolId: "t-1", customerId: "ada" }), { ok: true })
    assert.deepEqual(await send(commandBus, RentTool, { toolId: "t-1", customerId: "ada" }), { ok: true })
    assert.deepEqual(await send(commandBus, RentTool, { toolId: "t-1", customerId: "grace" }), {
      ok: false,
      rejection: "TOOL_UNAVAILABLE",
      toolId: "t-1",
    })
    assert.equal(await eventStore.getHeadPosition(), 2n)
  } finally {
    await app.stop()
  }
})

test("validatingHandler refuses a payload its schema refuses", async () => {
  const { app, commandBus } = toolStore()
  try {
    await assert.rejects(send(commandBus, AddTool, { toolId: "", name: "Cordless drill" }))
  } finally {
    await app.stop()
  }
})

test("two decisions on the same events: one appends, the other fails its append condition", async () => {
  const { app, commandBus } = toolStore()
  try {
    await send(commandBus, AddTool, { toolId: "t-2", name: "Tile saw" })
    const attempts = await Promise.allSettled([
      send(commandBus, RentTool, { toolId: "t-2", customerId: "linus" }),
      send(commandBus, RentTool, { toolId: "t-2", customerId: "grace" }),
    ])
    const fulfilled = attempts.filter((attempt) => attempt.status === "fulfilled")
    const rejected = attempts.filter((attempt) => attempt.status === "rejected")
    assert.equal(fulfilled.length, 1)
    assert.equal(rejected.length, 1)
    assert.ok(isConflict(rejected[0].reason))
  } finally {
    await app.stop()
  }
})

test("a processor delivers stored events and a query reads what its handlers wrote", async () => {
  const { app, eventStore, commandBus, queryBus } = toolStore()
  try {
    await send(commandBus, AddTool, { toolId: "t-1", name: "Cordless drill" })
    await send(commandBus, AddTool, { toolId: "t-2", name: "Tile saw" })
    await send(commandBus, RentTool, { toolId: "t-1", customerId: "ada" })
    await caughtUp(app.processors.get("inventory-view"), eventStore)
    assert.deepEqual(await query(queryBus, GetInventory, {}), [
      { toolId: "t-1", available: false },
      { toolId: "t-2", available: true },
    ])
  } finally {
    await app.stop()
  }
})

test("loggingHandler gives a handler ctx.log, bound to the message it handles", async () => {
  const lines = []
  const log = consoleLogger({ write: (line) => lines.push(line) })
  const loggedAddTool = commandHandler(AddTool, async (message, ctx) => {
    ctx.log.info("adding a tool", { toolId: message.payload.toolId })
    return addTool.handler(message, ctx)
  })
  const eventStore = inMemoryEventStore()
  const commandBus = localCommandBus(unitOfWork)
  const queryBus = localQueryBus(unitOfWork)
  const app = kronos({
    commandHandlers: [{ ...loggedAddTool, handler: loggingHandler(loggedAddTool.handler, log), eventStore, commandBus, queryBus }],
  })
  try {
    await send(commandBus, AddTool, { toolId: "t-3", name: "Table saw" })
    const line = lines.find((candidate) => candidate.includes("adding a tool"))
    assert.ok(line, `no log line among ${JSON.stringify(lines)}`)
    assert.ok(line.includes("t-3"))
    assert.ok(line.includes("AddTool"), `the line names the message it was written for: ${line}`)
  } finally {
    await app.stop()
  }
})
