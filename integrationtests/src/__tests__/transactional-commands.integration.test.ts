/**
 * Integration test for transactional command handlers (@kronos-ts/postgres).
 *
 * Proves the framework's contract: a command handler is one atomic unit. Its
 * appended events AND any non-event writes commit — or roll back — together.
 *
 *   command → ctx.load() → <user CRUD via the UoW connection> + ctx.append(WidgetUpdated)
 *           → one COMMIT (success)  OR  one ROLLBACK (handler throws)
 *
 * Two framework pieces make this work, and NOTHING ELSE is framework-provided:
 *   1. The command bus opens each handler's unit of work from the configured
 *      factory, and `postgresUnitOfWork` is a LAZY transactional one — so the
 *      command's UoW carries a postgres transaction, opened on the first
 *      writer. The composition root below builds the bus around THAT factory;
 *      spreading it on afterwards would leave the bus on a plain
 *      non-transactional UoW.
 *   2. postgresTransaction(ctx.unitOfWork) + PostgresAdapterTransaction.unwrap()
 *      — the postgres adapter's own TYPED accessor hands back the live driver
 *      connection so a user's own query builder (or raw `tx.query(...)`) rides
 *      the same tx. The unit of work travels as the handler's ctx, so user-land
 *      glue takes it as a parameter. There is no `ctx.transaction`: the base
 *      unit of work has no transaction concept, and only the adapter that owns
 *      the driver can type one.
 *
 * The Drizzle client here is built in USER LAND (see `uowDb`), exactly as an
 * application would — the framework does not ship one. This test doubles as
 * the reference for that pattern.
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { z } from "zod"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { Pool, type PoolClient } from "pg"
import { drizzle } from "drizzle-orm/node-postgres"
import { pgTable, text } from "drizzle-orm/pg-core"
import { qn, send } from "@kronos-ts/core"
import { command, event, commandHandler, jsonSerializer } from "@kronos-ts/core"
import { state } from "@kronos-ts/core"
import { type EventStore, descriptorBasedTagResolver } from "@kronos-ts/core"
import { kronos, type App } from "@kronos-ts/core"
import {
  correlation,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  localCommandBus,
  localQueryBus,
  type UnitOfWork,
  type CommandBus,
  type QueryBus,
} from "@kronos-ts/core"
import {
  postgresPool,
  postgresEventStore,
  postgresTransaction,
  postgresUnitOfWork,
  type PostgresResource,
} from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"

/**
 * The two things `kronos` needs that are not handlers. The UoW runner is
 * named once and handed to `localCommandBus` (which captures it at
 * construction) — writing it on an adjacent line is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork): { commandBus: CommandBus; queryBus: QueryBus } {
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(uow), correlation),
  }
}

// ============================================================================
// User-owned Drizzle schema + the few lines a user writes to bind Drizzle to
// the active UnitOfWork transaction. This is NOT framework code.
// ============================================================================

const widgets = pgTable("widgets", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
})
const schema = { widgets }

/**
 * A Drizzle client bound to the command's UoW connection — user-land glue.
 * `postgresTransaction` OPENS the (lazy) transaction on first use, so this is
 * the writer that triggers it. Taking the unit of work as a parameter is the
 * whole shape change: it is handed down, never looked up — and the accessor
 * that types it belongs to the adapter, not to the handle.
 */
async function uowDb(ctx: { unitOfWork: UnitOfWork }) {
  const tx = await postgresTransaction(ctx.unitOfWork)
  return drizzle(tx.unwrap<PoolClient>(), { schema })
}

// ============================================================================
// Domain — a "cruddy" Widget: state row edited in-place + an Updated event
// ============================================================================

const EditWidget = command({
  name: qn("tx-commands", "EditWidget"),
  payload: z.object({ id: z.string(), name: z.string(), boom: z.boolean().optional() }),
  routingKey: "id",
})

const WidgetUpdated = event({
  name: qn("tx-commands", "WidgetUpdated"),
  payload: z.object({ id: z.string(), name: z.string() }),
  tags: { widgetId: (p) => p.id },
})

type WidgetState = { name: string; revisions: number }
const Widget = state({
  id: { widgetId: z.string() },
  tags: (id) => ({ widgetId: id.widgetId }),
  evolve: [
    () => ({ name: "", revisions: 0 }) as WidgetState,
    [WidgetUpdated, (s, { payload: e }) => ({ name: e.name, revisions: s.revisions + 1 })],
  ],
})

const editWidget = commandHandler(EditWidget, async ({ payload: cmd }, ctx) => {
  await ctx.load(Widget, { widgetId: cmd.id })

  // CRUD write through a user-built Drizzle client — rides the UoW tx.
  const db = await uowDb(ctx)
  await db
    .insert(widgets)
    .values({ id: cmd.id, name: cmd.name })
    .onConflictDoUpdate({ target: widgets.id, set: { name: cmd.name } })

  // Event append — same UoW, same transaction.
  ctx.append(WidgetUpdated, { id: cmd.id, name: cmd.name })

  // Force the whole UoW to roll back AFTER both writes — proves atomicity.
  if (cmd.boom) throw new Error("boom — force rollback")
})

// ============================================================================
// Tests
// ============================================================================

const runId = Math.random().toString(36).slice(2, 8)
const wid = (name: string) => `${name}-${runId}`

describe("transactional commands — user CRUD atomic with appended events", () => {
  let container: StartedTestContainer
  let connectionString: string
  let pgPool: Pool
  let app: App
  let pool: PostgresResource
  let eventStore: EventStore
  let buses: { commandBus: CommandBus; queryBus: QueryBus }

  beforeAll(async () => {
    container = await new GenericContainer("postgres:16-alpine")
      .withEnvironment({
        POSTGRES_PASSWORD: "tx_commands",
        POSTGRES_DB: "tx_commands",
        POSTGRES_USER: "tx_commands",
      })
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
      .start()

    connectionString = `postgresql://tx_commands:tx_commands@${container.getHost()}:${container.getMappedPort(5432)}/tx_commands`

    pgPool = new Pool({ connectionString })
    await pgPool.query("CREATE TABLE IF NOT EXISTS widgets (id text PRIMARY KEY, name text NOT NULL)")

    // The pool is the only thing with a lifetime; the event store and the
    // unit-of-work factory are plain functions of it, and they share a
    // transaction BECAUSE they share the pool.
    pool = postgresPool(pgAdapter({ connectionString }))
    await pool.start()
    eventStore = postgresEventStore(pool, {
      serializer: jsonSerializer(),
      tagResolver: descriptorBasedTagResolver(),
    })

    // The whole point of this file: the command bus is built around postgres's
    // lazy transactional UoW factory, so a handler's appends and its own CRUD
    // ride the same transaction.
    buses = inMemoryBuses(postgresUnitOfWork(unitOfWork, pool))
    app = kronos({
      commandHandlers: [{ ...editWidget, eventStore, ...buses }],
    })
  }, 60_000)

  afterAll(async () => {
    await app?.stop()
    await pool?.close()
    await pgPool?.end()
    await container?.stop()
  })

  async function widgetRow(id: string): Promise<{ name: string } | undefined> {
    const res = await pgPool.query<{ name: string }>("SELECT name FROM widgets WHERE id = $1", [id])
    return res.rows[0]
  }

  async function updatedEvents(id: string): Promise<number> {
    const { events } = await eventStore.source({
      query: { tags: { widgetId: id } },
    })
    return events.filter((e) => e.name.name === "WidgetUpdated").length
  }

  it("commit: the user's row write and the event append both persist", async () => {
    const id = wid("ok")

    await send(buses.commandBus, EditWidget, { id, name: "Hello" })

    expect((await widgetRow(id))?.name).toBe("Hello")
    expect(await updatedEvents(id)).toBe(1)
  })

  it("rollback: a throw after the write undoes BOTH the row and the event", async () => {
    const id = wid("boom")

    await expect(
      send(buses.commandBus, EditWidget, { id, name: "Nope", boom: true }),
    ).rejects.toThrow("boom")

    // The INSERT ran on the same connection the UoW rolled back — so the row
    // must be gone, proving it was never on a separate autocommit path.
    expect(await widgetRow(id)).toBeUndefined()
    expect(await updatedEvents(id)).toBe(0)
  })

  it("the UoW transaction is observable in the handler via unwrap()", async () => {
    // Covered implicitly by the commit/rollback cases (uowDb() throws if the
    // tx is absent); this asserts a successful second edit increments cleanly.
    const id = wid("twice")

    await send(buses.commandBus, EditWidget, { id, name: "v1" })
    await send(buses.commandBus, EditWidget, { id, name: "v2" })

    expect((await widgetRow(id))?.name).toBe("v2")
    expect(await updatedEvents(id)).toBe(2)
  })
})
