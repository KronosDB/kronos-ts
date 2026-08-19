import { describe, it, expect, beforeAll, afterAll } from "bun:test"
import { descriptorBasedTagResolver, jsonSerializer } from "@kronos-ts/core"
import { pgAdapter } from "../adapters/pg.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"
import { postgresPool } from "../postgres-pool.js"
import { postgresEventStore } from "../postgres-event-store.js"
import { postgresSnapshotStore } from "../postgres-snapshot-store.js"
import { postgresTokenStore } from "../postgres-token-store.js"
import { postgresDeadLetterQueue } from "../postgres-dead-letter-queue.js"

let pg: RunningPostgres

beforeAll(async () => {
  pg = await startPostgresContainer()
}, 60_000)

afterAll(async () => {
  await pg.stop()
}, 30_000)

async function tableExists(
  pool: {
    queryOne: <R extends Record<string, unknown>>(sql: string, p?: unknown[]) => Promise<R | null>
  },
  name: string,
): Promise<boolean> {
  const row = await pool.queryOne<{ exists: boolean }>(
    `SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $1) AS exists`,
    [name],
  )
  return row!.exists
}

describe("postgresPool", () => {
  it("takes a connection string, and start() connects + bootstraps the whole family's schema", async () => {
    // No driver is named at the call site: the connection-string form loads the
    // pg adapter itself, which is what keeps the package free of a static
    // dependency on any client library.
    const pool = postgresPool(pg.connectionString)

    await pool.start()
    try {
      for (const table of [
        "kronos_events",
        "kronos_snapshots",
        "kronos_scheduled_events",
        "kronos_token_entries",
        "kronos_dead_letters",
      ]) {
        expect(await tableExists(pool, table)).toBe(true)
      }
    } finally {
      await pool.close()
    }
  })

  it("start() is idempotent — two calls await one connect", async () => {
    const pool = postgresPool(pg.connectionString)
    try {
      await Promise.all([pool.start(), pool.start()])
      expect(await tableExists(pool, "kronos_events")).toBe(true)
    } finally {
      await pool.close()
    }
  })

  it("every store in the family is a plain function of the pool", async () => {
    // The bundle is gone: a host names the pieces it wants and nothing else is
    // constructed. Each of these is built from the SAME pool, which is what
    // makes them share a transaction identity.
    const pool = postgresPool(pg.connectionString)
    await pool.start()
    try {
      const eventStore = postgresEventStore(pool, {
        serializer: jsonSerializer(),
        tagResolver: descriptorBasedTagResolver(),
      })
      const snapshotStore = postgresSnapshotStore(pool, { serializer: jsonSerializer() })
      const tokenStore = postgresTokenStore(pool)
      const deadLetters = postgresDeadLetterQueue(pool)

      expect(typeof eventStore.append).toBe("function")
      expect(typeof eventStore.source).toBe("function")
      expect(typeof eventStore.open).toBe("function")
      expect(typeof snapshotStore.store).toBe("function")
      expect(typeof snapshotStore.load).toBe("function")
      expect(typeof tokenStore.claimToken).toBe("function")
      expect(typeof deadLetters.enqueue).toBe("function")
    } finally {
      await pool.close()
    }
  })

  it("with bootstrap: false does not create the schema (consumer owns migrations)", async () => {
    const adapter = pgAdapter({ connectionString: pg.connectionString })

    // Connect directly to drop the tables, simulating a fresh environment.
    await adapter.connect()
    await adapter.query(`DROP TABLE IF EXISTS kronos_events CASCADE`)
    await adapter.query(`DROP TABLE IF EXISTS kronos_snapshots CASCADE`)
    await adapter.disconnect()

    // The adapter form: the pool still owns start()/close(), it just does not
    // own the schema.
    const pool = postgresPool(pgAdapter({ connectionString: pg.connectionString }), {
      bootstrap: false,
    })
    await pool.start()
    try {
      expect(await tableExists(pool, "kronos_events")).toBe(false)
    } finally {
      await pool.close()
    }
  })

  it("honours overridden table names — the stores read them off the pool", async () => {
    const pool = postgresPool(pg.connectionString, {
      tableNames: {
        events: "alt_events",
        snapshots: "alt_snapshots",
        scheduled: "alt_scheduled",
        tokens: "alt_tokens",
        deadLetters: "alt_dead_letters",
      },
    })
    await pool.start()
    try {
      expect(pool.tables.events).toBe("alt_events")
      expect(await tableExists(pool, "alt_events")).toBe(true)
      expect(await tableExists(pool, "alt_dead_letters")).toBe(true)

      // The store never took a table name: it reads the pool's.
      const tokenStore = postgresTokenStore(pool)
      await tokenStore.initializeSegments("alt-proc", 1)
      const rows = await pool.query(`SELECT segment FROM alt_tokens WHERE processor_name = $1`, [
        "alt-proc",
      ])
      expect(rows.length).toBe(1)
    } finally {
      await pool.close()
    }
  })

  it("refuses to serve queries before start() when built from a connection string", async () => {
    const pool = postgresPool(pg.connectionString)
    await expect(pool.query("SELECT 1")).rejects.toThrow(/await pool\.start\(\)/)
  })
})
