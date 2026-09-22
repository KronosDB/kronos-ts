/**
 * Integration coverage for @kronos-ts/postgres's per-statement observability
 * (`postgresHandler` under a traced context), proving it against a REAL Postgres and
 * a REAL query builder — Drizzle over `drizzle-orm/postgres-js` — rather than
 * a fake transaction.
 *
 * Lives here (not in packages/postgres) because `drizzle-orm` is only
 * resolvable from this workspace's node_modules.
 *
 * Proves:
 *   - a handler doing `drizzle(ctx.sql().unwrap()).insert(...)` runs in the
 *     task's transaction: rolled back on handler error, committed on success,
 *     together with Kronos's own writes through `ctx.sql().query(...)`;
 *   - every statement — Kronos's own AND Drizzle's — becomes exactly one
 *     "db.statement" span on the handler's trace, and nothing else.
 */
import assert from "node:assert/strict"
import { describe, expect, it, beforeAll, afterAll, beforeEach } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import { drizzle } from "drizzle-orm/postgres-js"
import { pgTable, text } from "drizzle-orm/pg-core"
import { unitOfWork, type UnitOfWork } from "@kronos-ts/core"
import {
  postgresPool,
  postgresUnitOfWork,
  postgresTransaction,
  postgresHandler,
  type PostgresCapability,
  type PostgresResource,
  type SpanningTrace,
} from "@kronos-ts/postgres"
import { postgresAdapter } from "@kronos-ts/postgres/adapters/postgres"

const widgets = pgTable("obs_widgets", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
})

type RecordedSpan = { readonly options: Record<string, unknown> | undefined }

/** A trace test double that records every wrap — name only, never args. */
function fakeTrace(): { trace: SpanningTrace; spans: RecordedSpan[] } {
  const spans: RecordedSpan[] = []
  const trace: SpanningTrace = {
    span(fn, options) {
      return ((...args: unknown[]) => {
        spans.push({ options: options as Record<string, unknown> | undefined })
        return fn(...args)
      }) as never
    },
  }
  return { trace, spans }
}

type HandlerCtx = PostgresCapability & { readonly unitOfWork: UnitOfWork; readonly trace: SpanningTrace }

describe("postgres observability + drizzle over the task's transaction", () => {
  let container: StartedTestContainer
  let connectionString: string
  let pool: PostgresResource

  beforeAll(async () => {
    container = await new GenericContainer("postgres:16-alpine")
      .withExposedPorts(5432)
      .withEnvironment({ POSTGRES_PASSWORD: "test", POSTGRES_USER: "test", POSTGRES_DB: "test" })
      .withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2))
      .start()
    const port = container.getMappedPort(5432)
    const host = container.getHost()
    connectionString = `postgresql://test:test@${host}:${port}/test`

    pool = postgresPool(postgresAdapter({ connectionString }), { bootstrap: false })
    await pool.start()
    await pool.query(`CREATE TABLE IF NOT EXISTS obs_widgets (id text primary key, name text not null)`)
    await pool.query(`CREATE TABLE IF NOT EXISTS obs_markers (id text primary key)`)
  }, 120_000)

  afterAll(async () => {
    await pool?.close()
    await container?.stop()
  })

  beforeEach(async () => {
    await pool.query(`DELETE FROM obs_widgets`)
    await pool.query(`DELETE FROM obs_markers`)
  })

  it("commits Kronos's write and Drizzle's write TOGETHER, each spanned once", async () => {
    const make = postgresUnitOfWork(unitOfWork, pool)
    const { trace, spans } = fakeTrace()
    const handler = postgresHandler(
      async (message: { id: string; name: string }, ctx: HandlerCtx) => {
        const sql = ctx.sql()
        await sql.query("INSERT INTO obs_markers (id) VALUES ($1)", [message.id])
        const db = drizzle(sql.unwrap())
        await db.insert(widgets).values({ id: message.id, name: message.name })
      },
      pool,
    )

    await make().execute(async (uow) => {
      await postgresTransaction(uow)
      await handler({ id: "w-1", name: "Widget One" }, { unitOfWork: uow, trace } as HandlerCtx)
    })

    const rows = await pool.query<{ id: string; name: string }>("SELECT id, name FROM obs_widgets")
    expect(rows).toEqual([{ id: "w-1", name: "Widget One" }])
    const markers = await pool.query<{ id: string }>("SELECT id FROM obs_markers")
    expect(markers.length).toBe(1)

    // ONE span for Kronos's own insert, ONE for Drizzle's — both under the
    // handler's trace, each carrying the statement it ran and never its values.
    expect(spans.map((span) => span.options?.name)).toEqual(["db.statement", "db.statement"])
    const texts = spans.map((span) => (span.options?.attributes as Record<string, string>)["db.query.text"])
    expect(texts[0]).toBe("INSERT INTO obs_markers (id) VALUES ($1)")
    expect(texts[1]).toMatch(/^insert into "obs_widgets" \("id", "name"\) values \(\$1, \$2\)/)
    expect(JSON.stringify(spans)).not.toContain("Widget One")
  })

  it("rolls back Kronos's write and Drizzle's write TOGETHER on handler error", async () => {
    const make = postgresUnitOfWork(unitOfWork, pool)
    const { trace, spans } = fakeTrace()
    const handler = postgresHandler(
      async (message: { id: string; name: string }, ctx: HandlerCtx) => {
        const sql = ctx.sql()
        await sql.query("INSERT INTO obs_markers (id) VALUES ($1)", [message.id])
        const db = drizzle(sql.unwrap())
        await db.insert(widgets).values({ id: message.id, name: message.name })
        throw new Error("boom: handler failed after both writes")
      },
      pool,
    )

    await assert.rejects(
      make().execute(async (uow) => {
        await postgresTransaction(uow)
        await handler({ id: "w-2", name: "Widget Two" }, { unitOfWork: uow, trace } as HandlerCtx)
      }),
      /boom: handler failed after both writes/,
    )

    const rows = await pool.query("SELECT id FROM obs_widgets WHERE id = $1", ["w-2"])
    expect(rows.length).toBe(0)
    const markers = await pool.query("SELECT id FROM obs_markers WHERE id = $1", ["w-2"])
    expect(markers.length).toBe(0)

    // Both writes still ran (and were spanned) before the rollback — the span
    // count does not depend on whether the transaction commits.
    expect(spans.length).toBe(2)
  })

  it("no trace on the context: postgresHandler is unaffected — sql() is the plain transaction", async () => {
    // A SEPARATE pool over the same database, handled with NO trace on the
    // context — proves statement spans follow the context, and that a host
    // without tracing gets the real handle, not a view.
    const plainPool = postgresPool(postgresAdapter({ connectionString }), { bootstrap: false })
    await plainPool.start()
    try {
      const make = postgresUnitOfWork(unitOfWork, plainPool)
      let sawSql: unknown
      let opened: unknown
      const handler = postgresHandler(
        async (_message: unknown, ctx: PostgresCapability & { readonly unitOfWork: UnitOfWork }) => {
          sawSql = ctx.sql()
        },
        plainPool,
      )

      await make().execute(async (uow) => {
        opened = await postgresTransaction(uow)
        await handler({}, { unitOfWork: uow } as never)
      })

      // With no trace, sql() answers the REAL transaction handle
      // directly — no observability view sits in front of it.
      expect(sawSql).toBe(opened)
    } finally {
      await plainPool.close()
    }
  })
})
