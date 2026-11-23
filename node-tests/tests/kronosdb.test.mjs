import assert from "node:assert/strict"
import { test } from "node:test"
import {
  eventProcessor,
  inMemoryTokenStore,
  isConflict,
  jsonSerializer,
  kronos,
  localCommandBus,
  localQueryBus,
  query,
  send,
  unitOfWork,
} from "@kronos-ts/core"
import { kronosDbCommandBus, kronosDbConnection, kronosDbEventStore, kronosDbQueryBus } from "@kronos-ts/kronosdb"
import { AddTool, GetInventory, RentTool, addTool, caughtUp, memoryInventory, rentTool } from "./toolstore.mjs"
import { needs } from "./services.mjs"

const skip = needs("KRONOSDB_HOST")
const context = process.env.KRONOSDB_CONTEXT ?? "default"

test("events, commands and queries go through a KronosDB server over its native protocol", { skip }, async () => {
  const kdb = await kronosDbConnection({
    host: process.env.KRONOSDB_HOST,
    port: Number(process.env.KRONOSDB_PORT ?? 50051),
    context,
    componentName: "kronos-node-tests",
    serializer: jsonSerializer(),
  })
  const eventStore = kronosDbEventStore(kdb, context)
  const commandBus = kronosDbCommandBus(localCommandBus(unitOfWork), kdb, "node-tests")
  const queryBus = kronosDbQueryBus(localQueryBus(unitOfWork), kdb, "node-tests")
  const processor = eventProcessor("inventory-view", eventStore, inMemoryTokenStore(), unitOfWork)
  const inventory = memoryInventory()
  const app = kronos({
    commandHandlers: [addTool, rentTool].map((entry) => ({ ...entry, eventStore, commandBus, queryBus })),
    eventHandlers: inventory.eventHandlers.map((entry) => ({ ...entry, commandBus, queryBus, processor })),
    queryHandlers: [{ ...inventory.queryHandler, queryBus }],
  })
  await kdb.start()

  // The server's log outlives the test, so every run uses tools of its own.
  const run = crypto.randomUUID()
  const [drill, saw] = [`${run}-drill`, `${run}-saw`]
  try {
    await send(commandBus, AddTool, { toolId: drill, name: "Cordless drill" })
    await send(commandBus, AddTool, { toolId: saw, name: "Tile saw" })
    assert.deepEqual(await send(commandBus, RentTool, { toolId: drill, customerId: "ada" }), { ok: true })
    assert.deepEqual(await send(commandBus, RentTool, { toolId: drill, customerId: "grace" }), {
      ok: false,
      rejection: "TOOL_UNAVAILABLE",
      toolId: drill,
    })

    const attempts = await Promise.allSettled([
      send(commandBus, RentTool, { toolId: saw, customerId: "linus" }),
      send(commandBus, RentTool, { toolId: saw, customerId: "grace" }),
    ])
    const outcomes = attempts.map((attempt) =>
      attempt.status === "fulfilled" ? attempt.value.ok : isConflict(attempt.reason) ? "conflict" : attempt.reason,
    )
    assert.deepEqual(outcomes.sort(), ["conflict", true].sort())

    await caughtUp(app.processors.get("inventory-view"), eventStore)
    const rows = await query(queryBus, GetInventory, {})
    assert.deepEqual(
      rows.filter((row) => row.toolId.startsWith(run)),
      [
        { toolId: drill, available: false },
        { toolId: saw, available: false },
      ],
    )
  } finally {
    await app.stop()
    await kdb.close()
  }
})
