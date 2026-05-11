import { describe, it, expect, beforeAll, afterAll } from "bun:test"
import { postgresAdapter, type PostgresAdapterConfig } from "../../adapters/postgres.js"
import { IsolationLevel, type PostgresAdapter } from "../../adapter.js"
import { startPostgresContainer, type RunningPostgres } from "../testcontainers-setup.js"

let pg: RunningPostgres
let adapter: PostgresAdapter

beforeAll(async () => {
  pg = await startPostgresContainer()
  adapter = postgresAdapter({ connectionString: pg.connectionString } satisfies PostgresAdapterConfig)
  await adapter.connect()
  await adapter.query(`CREATE TABLE IF NOT EXISTS test_rows (id BIGSERIAL PRIMARY KEY, name TEXT)`)
}, 60_000)

afterAll(async () => {
  await adapter.disconnect()
  await pg.stop()
}, 30_000)

describe("postgresAdapter — query / queryOne", () => {
  it("query() returns rows from a literal SELECT", async () => {
    const rows = await adapter.query<{ n: number }>(`SELECT 1 AS n`)
    expect(rows).toEqual([{ n: 1 }])
  })

  it("queryOne() returns the single row", async () => {
    const row = await adapter.queryOne<{ answer: number }>(`SELECT 42 AS answer`)
    expect(row).toEqual({ answer: 42 })
  })

  it("queryOne() returns null on zero rows", async () => {
    const row = await adapter.queryOne(`SELECT * FROM (VALUES (1)) AS t(n) WHERE n = 99`)
    expect(row).toBeNull()
  })

  it("queryOne() throws when more than one row is returned", async () => {
    await expect(
      adapter.queryOne(`SELECT * FROM (VALUES (1),(2)) AS t(n)`),
    ).rejects.toThrow(/more than one row/i)
  })
})

describe("postgresAdapter — transactions", () => {
  it("commits a successful transaction", async () => {
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

  it("rolls back on rejected callback and re-throws the original error", async () => {
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

  it("supports all three isolation levels (no syntax error)", async () => {
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

describe("postgresAdapter — SQLSTATE pass-through (D-12.12 wiring)", () => {
  it("preserves SQLSTATE on .code unchanged (KR001 surfaces from a PL/pgSQL RAISE)", async () => {
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

describe("postgresAdapter — LISTEN/NOTIFY", () => {
  it("delivers NOTIFY payload to the listen() callback and unlistens cleanly", async () => {
    const received: string[] = []
    const sub = await adapter.listen("kronos_postgres_test_chan", (payload) => {
      if (payload) received.push(payload)
    })
    // NOTIFY needs to land on a different connection than the listener.
    await adapter.query(`NOTIFY kronos_postgres_test_chan, 'hello'`)
    // Wait up to 2s for the listener to fire.
    const deadline = Date.now() + 2000
    while (received.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 25))
    }
    expect(received).toEqual(["hello"])
    await sub.unlisten()
    // After unlisten, no further deliveries.
    await adapter.query(`NOTIFY kronos_postgres_test_chan, 'after_unlisten'`)
    await new Promise((r) => setTimeout(r, 250))
    expect(received).toEqual(["hello"])
  })
})

describe("postgresAdapter — lifecycle", () => {
  it("disconnect() is idempotent (second call is a no-op, not an error)", async () => {
    // Use a separate adapter so we don't tear down the suite's shared one.
    const a = postgresAdapter({ connectionString: pg.connectionString })
    await a.connect()
    await a.disconnect()
    await expect(a.disconnect()).resolves.toBeUndefined()
  })
})
