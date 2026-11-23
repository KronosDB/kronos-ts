import assert from "node:assert/strict"
import { test } from "node:test"
import {
  eventHandler,
  eventProcessor,
  isConflict,
  kronos,
  localCommandBus,
  localQueryBus,
  query,
  queryHandler,
  send,
  unitOfWork,
} from "@kronos-ts/core"
import {
  postgresEventStore,
  postgresHandler,
  postgresPool,
  postgresTokenStore,
  postgresUnitOfWork,
} from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"
import { postgresAdapter } from "@kronos-ts/postgres/adapters/postgres"
import { AddTool, GetInventory, RentTool, ToolAdded, ToolRented, addTool, caughtUp, rentTool } from "./toolstore.mjs"
import { needs } from "./services.mjs"

const skip = needs("KRONOS_TEST_DATABASE_URL")
const serverUrl = process.env.KRONOS_TEST_DATABASE_URL

const projection = [
  eventHandler(ToolAdded, async ({ payload }, ctx) => {
    await ctx.sql().query(`INSERT INTO inventory (tool_id, available) VALUES ($1, true) ON CONFLICT (tool_id) DO NOTHING`, [payload.toolId])
  }),
  eventHandler(ToolRented, async ({ payload }, ctx) => {
    await ctx.sql().query(`UPDATE inventory SET available = false WHERE tool_id = $1`, [payload.toolId])
  }),
]
const getInventory = queryHandler(GetInventory, async (_query, ctx) => {
  const rows = await ctx.sql().query(`SELECT tool_id, available FROM inventory ORDER BY tool_id`)
  return rows.map((row) => ({ toolId: row.tool_id, available: row.available }))
})

/** A database of its own, dropped afterwards. */
async function withDatabase(adapterFor, body) {
  const name = `node_${crypto.randomUUID().replaceAll("-", "")}`
  const admin = postgresPool(serverUrl, { bootstrap: false })
  await admin.start()
  await admin.query(`CREATE DATABASE ${name}`)
  const url = new URL(serverUrl)
  url.pathname = `/${name}`
  const pg = postgresPool(adapterFor(url.toString()))
  try {
    await pg.start()
    await pg.query(`CREATE TABLE IF NOT EXISTS inventory (tool_id text PRIMARY KEY, available boolean NOT NULL)`)
    await body(pg)
  } finally {
    await pg.close()
    await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
    await admin.close()
  }
}

function toolStore(pg) {
  const processing = postgresUnitOfWork(unitOfWork, pg)
  const eventStore = postgresEventStore(pg)
  const commandBus = localCommandBus(processing)
  const queryBus = localQueryBus(unitOfWork)
  const processor = eventProcessor("inventory-view", eventStore, postgresTokenStore(pg), processing)
  const app = kronos({
    commandHandlers: [addTool, rentTool].map((entry) => ({ ...entry, eventStore, commandBus, queryBus })),
    eventHandlers: projection.map((entry) => ({
      ...entry,
      handler: postgresHandler(entry.handler, pg),
      commandBus,
      queryBus,
      processor,
    })),
    queryHandlers: [{ ...getInventory, handler: postgresHandler(getInventory.handler, pg), queryBus }],
  })
  return { app, eventStore, commandBus, queryBus }
}

const adapters = {
  pg: (connectionString) => pgAdapter({ connectionString }),
  "postgres.js": (connectionString) => postgresAdapter({ connectionString, clientOptions: { onnotice: () => {} } }),
}

for (const [driver, adapterFor] of Object.entries(adapters)) {
  test(`${driver}: events, positions and the read model commit in Postgres and survive a restart`, { skip }, async () => {
    await withDatabase(adapterFor, async (pg) => {
      const first = toolStore(pg)
      await send(first.commandBus, AddTool, { toolId: "t-1", name: "Cordless drill" })
      await send(first.commandBus, AddTool, { toolId: "t-2", name: "Tile saw" })
      assert.deepEqual(await send(first.commandBus, RentTool, { toolId: "t-1", customerId: "ada" }), { ok: true })
      await caughtUp(first.app.processors.get("inventory-view"), first.eventStore)
      await first.app.stop()

      const second = toolStore(pg)
      try {
        assert.deepEqual(await send(second.commandBus, RentTool, { toolId: "t-1", customerId: "grace" }), {
          ok: false,
          rejection: "TOOL_UNAVAILABLE",
          toolId: "t-1",
        })
        await send(second.commandBus, AddTool, { toolId: "t-3", name: "Table saw" })
        await caughtUp(second.app.processors.get("inventory-view"), second.eventStore)
        assert.deepEqual(await query(second.queryBus, GetInventory, {}), [
          { toolId: "t-1", available: false },
          { toolId: "t-2", available: true },
          { toolId: "t-3", available: true },
        ])
      } finally {
        await second.app.stop()
      }
    })
  })

  test(`${driver}: two decisions on the same events, one fails its append condition`, { skip }, async () => {
    await withDatabase(adapterFor, async (pg) => {
      const { app, commandBus } = toolStore(pg)
      try {
        await send(commandBus, AddTool, { toolId: "t-1", name: "Cordless drill" })
        const attempts = await Promise.allSettled([
          send(commandBus, RentTool, { toolId: "t-1", customerId: "linus" }),
          send(commandBus, RentTool, { toolId: "t-1", customerId: "grace" }),
        ])
        const outcomes = attempts.map((attempt) =>
          attempt.status === "fulfilled" ? attempt.value.ok : isConflict(attempt.reason) ? "conflict" : attempt.reason,
        )
        assert.deepEqual(outcomes.sort(), ["conflict", true].sort())
      } finally {
        await app.stop()
      }
    })
  })
}
