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
 *   1. The command bus runs handlers through the configured unitOfWorkFactory,
 *      and postgres() provides a LAZY transactional one — so the command's UoW
 *      carries a postgres transaction, opened on the first writer. The
 *      composition root below builds the bus around THAT factory; spreading it
 *      on afterwards would leave the bus on a plain non-transactional UoW.
 *   2. getOrBeginActiveTransaction() + PostgresAdapterTransaction.unwrap() —
 *      hands back the live driver connection so a user's own query builder
 *      (or raw `tx.query(...)`) rides the same tx.
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
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  commandHandler,
  EventCriteria,
  getOrBeginActiveTransaction,
  jsonSerializer,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { type EventStore, descriptorBasedTagResolver } from "@kronos-ts/eventsourcing"
import { createApp, inMemoryComponents, module, type App } from "@kronos-ts/app"
import { postgres, type PostgresAdapterTransaction } from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"

/** The backend handle `postgres()` returns — its type is not re-exported. */
type PostgresBackend = Awaited<ReturnType<typeof postgres>>

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
 * `getOrBeginActiveTransaction` opens the (lazy) UoW transaction on first use,
 * so this is the writer that triggers it.
 */
async function uowDb() {
  const tx = await getOrBeginActiveTransaction<PostgresAdapterTransaction>()
  if (!tx) throw new Error("no active UnitOfWork transaction")
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
  tags: (p) => [tag("widgetId", p.id)],
})

type WidgetState = { name: string; revisions: number }
const Widget = state({
  name: "Widget",
  id: { widgetId: z.string() },
  initial: () => ({ name: "", revisions: 0 }) as WidgetState,
  criteria: (id) => EventCriteria.havingTags(tag("widgetId", id.widgetId)),
  evolve: (on) => [
    on(WidgetUpdated, (s, { payload: e }) => ({ name: e.name, revisions: s.revisions + 1 })),
  ],
})

const editWidget = commandHandler(EditWidget, async ({ payload: cmd }, ctx) => {
  await ctx.load(Widget, { widgetId: cmd.id })

  // CRUD write through a user-built Drizzle client — rides the UoW tx.
  const db = await uowDb()
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
  let pool: Pool
  let app: App
  let backend: PostgresBackend

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

    pool = new Pool({ connectionString })
    await pool.query("CREATE TABLE IF NOT EXISTS widgets (id text PRIMARY KEY, name text NOT NULL)")

    backend = await postgres({
      adapter: pgAdapter({ connectionString }),
      serializer: jsonSerializer(),
      tagResolver: descriptorBasedTagResolver(),
    })

    // The whole point of this file: the command bus is built around postgres's
    // lazy transactional UoW factory, so a handler's appends and its own CRUD
    // ride the same transaction.
    app = createApp({
      components: {
        ...inMemoryComponents({ unitOfWorkFactory: backend.components.unitOfWorkFactory }),
        ...backend.components,
      },
      modules: [module("tx-commands", Widget, editWidget)],
    })
    await backend.start()
  }, 60_000)

  afterAll(async () => {
    await app?.stop()
    await backend?.close()
    await pool?.end()
    await container?.stop()
  })

  function eventStore(): EventStore {
    return backend.components.eventStore
  }

  async function widgetRow(id: string): Promise<{ name: string } | undefined> {
    const res = await pool.query<{ name: string }>("SELECT name FROM widgets WHERE id = $1", [id])
    return res.rows[0]
  }

  async function updatedEvents(id: string): Promise<number> {
    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("widgetId", id)),
    })
    return events.filter((e) => e.name.name === "WidgetUpdated").length
  }

  it("commit: the user's row write and the event append both persist", async () => {
    const id = wid("ok")

    await app.commandGateway.send(EditWidget, { id, name: "Hello" })

    expect((await widgetRow(id))?.name).toBe("Hello")
    expect(await updatedEvents(id)).toBe(1)
  })

  it("rollback: a throw after the write undoes BOTH the row and the event", async () => {
    const id = wid("boom")

    await expect(
      app.commandGateway.send(EditWidget, { id, name: "Nope", boom: true }),
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

    await app.commandGateway.send(EditWidget, { id, name: "v1" })
    await app.commandGateway.send(EditWidget, { id, name: "v2" })

    expect((await widgetRow(id))?.name).toBe("v2")
    expect(await updatedEvents(id)).toBe(2)
  })
})
