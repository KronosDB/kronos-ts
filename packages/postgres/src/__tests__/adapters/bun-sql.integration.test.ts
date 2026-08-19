import { describe, it, expect, beforeAll, afterAll } from "bun:test"
import { bunSqlAdapter, type BunSqlAdapterConfig } from "../../adapters/bun-sql.js"
import { IsolationLevel, type PostgresAdapter } from "../../adapter.js"
import { startPostgresContainer, type RunningPostgres } from "../testcontainers-setup.js"

// This suite only runs under Bun with Bun.SQL available. If Bun.SQL is missing
// (very old Bun or Node), the suite is skipped at beforeAll and every test
// uses it.skipIf(!supported) to cleanly skip rather than fail.

let pg: RunningPostgres
let adapter: PostgresAdapter
let supported = true

beforeAll(async () => {
  const g = globalThis as { Bun?: { SQL?: unknown } }
  if (!g.Bun?.SQL) {
    supported = false
    return
  }
  pg = await startPostgresContainer()
  adapter = bunSqlAdapter({ connectionString: pg.connectionString } satisfies BunSqlAdapterConfig)
  await adapter.connect()
  await adapter.query(`CREATE TABLE IF NOT EXISTS test_rows (id BIGSERIAL PRIMARY KEY, name TEXT)`)
}, 60_000)

afterAll(async () => {
  if (!supported) return
  await adapter.disconnect()
  await pg.stop()
}, 30_000)

describe("bunSqlAdapter — query / queryOne", () => {
  it.skipIf(!supported)("query() returns rows from a literal SELECT", async () => {
    const rows = await adapter.query<{ n: number }>(`SELECT 1 AS n`)
    expect(rows).toEqual([{ n: 1 }])
  })

  it.skipIf(!supported)("queryOne() returns the single row", async () => {
    const row = await adapter.queryOne<{ answer: number }>(`SELECT 42 AS answer`)
    expect(row).toEqual({ answer: 42 })
  })

  it.skipIf(!supported)("queryOne() returns null on zero rows", async () => {
    const row = await adapter.queryOne(`SELECT * FROM (VALUES (1)) AS t(n) WHERE n = 99`)
    expect(row).toBeNull()
  })

  it.skipIf(!supported)("queryOne() throws when more than one row is returned", async () => {
    await expect(
      adapter.queryOne(`SELECT * FROM (VALUES (1),(2)) AS t(n)`),
    ).rejects.toThrow(/more than one row/i)
  })
})

describe("bunSqlAdapter — transactions", () => {
  it.skipIf(!supported)("commits a successful transaction", async () => {
    const inserted = await adapter.transaction(IsolationLevel.READ_COMMITTED, async (tx) => {
      const rows = await tx.query<{ id: string }>(
        `INSERT INTO test_rows (name) VALUES ($1) RETURNING id`,
        ["committed"],
      )
      return rows[0]!.id
    })
    const rows = await adapter.query<{ id: string }>(
      `SELECT id FROM test_rows WHERE id = $1`,
      [inserted],
    )
    expect(rows).toHaveLength(1)
  })

  it.skipIf(!supported)("rolls back on rejected callback and re-throws the original error", async () => {
    let threw: Error | undefined
    try {
      await adapter.transaction(IsolationLevel.READ_COMMITTED, async (tx) => {
        await tx.query(`INSERT INTO test_rows (name) VALUES ($1)`, ["rolled-back"])
        throw new Error("forced rollback")
      })
    } catch (e) {
      threw = e as Error
    }
    expect(threw).toBeDefined()
    expect(threw!.message).toBe("forced rollback")

    const rows = await adapter.query<{ id: string }>(
      `SELECT id FROM test_rows WHERE name = $1`,
      ["rolled-back"],
    )
    expect(rows).toHaveLength(0)
  })

  it.skipIf(!supported)("supports all three isolation levels (no syntax error)", async () => {
    for (const lvl of [
      IsolationLevel.READ_COMMITTED,
      IsolationLevel.REPEATABLE_READ,
      IsolationLevel.SERIALIZABLE,
    ]) {
      const rows = await adapter.transaction(lvl, async (tx) => tx.query<{ n: number }>(`SELECT 1 AS n`))
      expect(rows).toEqual([{ n: 1 }])
    }
  })
})

describe("bunSqlAdapter — SQLSTATE pass-through (D-12.12 wiring)", () => {
  it.skipIf(!supported)("preserves SQLSTATE on .code unchanged (KR001 surfaces from a PL/pgSQL RAISE)", async () => {
    let caught: { code?: string } | undefined
    try {
      await adapter.query(`
        DO $$ BEGIN
          RAISE EXCEPTION 'forced violation' USING ERRCODE = 'KR001';
        END $$;
      `)
    } catch (e) {
      caught = e as { code?: string }
    }
    expect(caught).toBeDefined()
    expect(caught!.code).toBe("KR001")
  })
})

describe("bunSqlAdapter — LISTEN/NOTIFY", () => {
  it.skipIf(!supported)("listen() callback fires (native LISTEN if supported, else polling shim fires within 1s)", async () => {
    const received: Array<string | undefined> = []
    const sub = await adapter.listen("kronos_bun_test_chan", (payload) => {
      received.push(payload)
    })

    // Note on the polling shim: if Bun.sql lacks native LISTEN (this Bun version),
    // the callback fires with `undefined` payload on every 250ms tick. The test
    // only asserts the callback fires AT LEAST ONCE within 1s — it does NOT
    // assert payload content when the shim is active.
    const hasNativeListenSupport =
      typeof (new (
        (globalThis as { Bun: { SQL: new (c: { url: string }) => unknown } }).Bun.SQL
      )({ url: "unused" }) as { listen?: unknown }).listen === "function"

    if (hasNativeListenSupport) {
      // With native LISTEN: assert payload delivery
      await adapter.query(`NOTIFY kronos_bun_test_chan, 'hello'`)
      const deadline = Date.now() + 2000
      while (received.length === 0 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 25))
      }
      expect(received.length).toBeGreaterThan(0)
      expect(received[0]).toBe("hello")
    } else {
      // Polling shim: assert callback fires at least once within 1s
      const deadline = Date.now() + 1000
      while (received.length === 0 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 50))
      }
      expect(received.length).toBeGreaterThan(0)
    }

    await sub.unlisten()
  })
})

describe("bunSqlAdapter — lifecycle", () => {
  it.skipIf(!supported)("disconnect() is idempotent (second call is a no-op, not an error)", async () => {
    // Use a separate adapter so we don't tear down the suite's shared one.
    const a = bunSqlAdapter({ connectionString: pg.connectionString })
    await a.connect()
    await a.disconnect()
    await expect(a.disconnect()).resolves.toBeUndefined()
  })
})
