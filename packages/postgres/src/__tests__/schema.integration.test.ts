import { describe, it, expect, beforeAll, afterAll, afterEach } from "bun:test"
import { IsolationLevel, type PostgresAdapter } from "../adapter.js"
import { bunSqlAdapter } from "../adapters/bun-sql.js"
import { pgAdapter } from "../adapters/pg.js"
import { postgresAdapter } from "../adapters/postgres.js"
import { bootstrapSchema, KRONOS_SCHEMA_LOCK_KEY } from "../schema.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"

let pg: RunningPostgres
/**
 * A second client with ONE connection, to look at the server while the adapter
 * under test works.
 */
let observer: PostgresAdapter
let adapter: PostgresAdapter | undefined

beforeAll(async () => {
  pg = await startPostgresContainer()
  observer = pgAdapter({ connectionString: pg.connectionString, poolConfig: { max: 1 } })
  await observer.connect()
}, 60_000)

afterEach(async () => {
  // End every session but the observer's, so one still waiting for a lock
  // cannot keep its pool from closing or hold the lock into the next test.
  await observer.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
      WHERE pid <> pg_backend_pid() AND datname = current_database()`,
  )
  await adapter?.disconnect().catch(() => {})
  adapter = undefined
})

afterAll(async () => {
  await observer.disconnect()
  await pg.stop()
}, 30_000)

async function advisoryLocks(state: "held" | "awaited"): Promise<number> {
  const rows = await observer.query<{ n: string }>(
    `SELECT count(*)::text AS n FROM pg_locks WHERE locktype = 'advisory' AND granted = $1`,
    [state === "held"],
  )
  return Number(rows[0]!.n)
}

async function tableExists(name: string): Promise<boolean> {
  const rows = await observer.query<{ exists: boolean }>(
    `SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $1) AS exists`,
    [name],
  )
  return rows[0]!.exists
}

/** Resolves with `"timed out"` when `work` has not settled after `ms`. */
function within<T>(work: Promise<T>, ms: number): Promise<T | "timed out"> {
  return Promise.race([work, new Promise<"timed out">((resolve) => setTimeout(() => resolve("timed out"), ms))])
}

const adapters: ReadonlyArray<[string, () => PostgresAdapter]> = [
  ["pgAdapter", () => pgAdapter({ connectionString: pg.connectionString })],
  ["postgresAdapter", () => postgresAdapter({ connectionString: pg.connectionString })],
  ["bunSqlAdapter", () => bunSqlAdapter({ connectionString: pg.connectionString })],
]

describe.each(adapters)("bootstrapSchema over %s", (_name, make) => {
  it("leaves no advisory lock held when the pool is serving other statements", async () => {
    adapter = make()
    await adapter.connect()
    // Other traffic, so consecutive statements of the bootstrap are handed
    // different connections.
    const traffic = Array.from({ length: 30 }, () => adapter!.query("SELECT pg_sleep(0.02)"))

    const bootstraps = await Promise.all(
      Array.from({ length: 4 }, () => within(bootstrapSchema(adapter!).then(() => "done" as const), 10_000)),
    )
    await Promise.all(traffic)

    expect(bootstraps).toEqual(["done", "done", "done", "done"])
    expect(await advisoryLocks("held")).toBe(0)
    expect(await advisoryLocks("awaited")).toBe(0)
    expect(await tableExists("kronos_events")).toBe(true)
    expect(await tableExists("kronos_dead_letters")).toBe(true)
  }, 30_000)

  it("waits while another session holds the schema lock, and runs once it is released", async () => {
    adapter = make()
    await adapter.connect()
    const tables = {
      events: "waiting_events",
      snapshots: "waiting_snapshots",
      scheduled: "waiting_scheduled",
      tokens: "waiting_tokens",
      deadLetters: "waiting_dead_letters",
    }
    for (const table of Object.values(tables)) await observer.query(`DROP TABLE IF EXISTS ${table}`)

    let release!: () => void
    const released = new Promise<void>((resolve) => {
      release = resolve
    })
    let holding!: () => void
    const held = new Promise<void>((resolve) => {
      holding = resolve
    })
    const other = pgAdapter({ connectionString: pg.connectionString })
    const holder = other.transaction(IsolationLevel.READ_COMMITTED, async (tx) => {
      await tx.query(`SELECT pg_advisory_xact_lock($1)`, [KRONOS_SCHEMA_LOCK_KEY])
      holding()
      await released
    })
    await held

    let finished = false
    const bootstrap = bootstrapSchema(adapter, { tableNames: tables }).then(() => {
      finished = true
    })
    await new Promise((resolve) => setTimeout(resolve, 300))
    expect(finished).toBe(false)
    expect(await tableExists("waiting_events")).toBe(false)

    release()
    await holder
    await other.disconnect()
    await bootstrap
    expect(await tableExists("waiting_events")).toBe(true)
    expect(await tableExists("waiting_dead_letters")).toBe(true)
    expect(await advisoryLocks("held")).toBe(0)
  }, 30_000)
})
