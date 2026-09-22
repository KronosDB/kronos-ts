import { describe, it, expect } from "bun:test"
import { chainOf, unitOfWork } from "@kronos-ts/core"
import type { CommandHandlerContext } from "@kronos-ts/core"
import type { PostgresAdapter, PostgresAdapterTransaction, ListenSubscription } from "../adapter.js"
import type { IsolationLevel } from "../adapter.js"
import { postgresPool } from "../postgres-pool.js"
import { postgresTransaction, postgresUnitOfWork } from "../postgres-transaction.js"
import { postgresHandler, type PostgresCapability } from "../postgres-handler.js"
import { observedPoolView, observedTransactionView, type SpanningTrace } from "../postgres-observability.js"

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

type RecordedSpan = { readonly options: Record<string, unknown> | undefined }

/** A trace whose `span()` records every wrap — its name and attributes. */
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

function fakeAdapter(): PostgresAdapter {
  return {
    async query() {
      return []
    },
    async queryOne() {
      return null
    },
    async transaction<T>(
      _isolationLevel: IsolationLevel,
      fn: (tx: PostgresAdapterTransaction) => Promise<T>,
    ): Promise<T> {
      return fn({
        unwrap<U = unknown>(): U {
          return undefined as unknown as U
        },
        async query() {
          return []
        },
      })
    },
    async listen(): Promise<ListenSubscription> {
      return { async unlisten() {} }
    },
    async connect() {},
    async disconnect() {},
    unwrap<T = unknown>(): T {
      return undefined as unknown as T
    },
  }
}

/** A postgres.js / Bun.sql-shaped fake: callable tag with `.unsafe`, lazy pending queries. */
function fakePostgresJsLikeClient() {
  let executed = 0
  function makePending(label: string): {
    then: (onFulfilled?: unknown, onRejected?: unknown) => Promise<unknown>
    values: () => ReturnType<typeof makePending>
    raw: () => ReturnType<typeof makePending>
  } {
    return {
      then(onFulfilled?: unknown, onRejected?: unknown) {
        executed++
        return Promise.resolve([{ label }]).then(
          onFulfilled as (v: unknown) => unknown,
          onRejected as (e: unknown) => unknown,
        )
      },
      values() {
        return makePending(`${label}:values`)
      },
      raw() {
        return makePending(`${label}:raw`)
      },
    }
  }
  const client = ((..._args: unknown[]) => makePending("tag")) as unknown as {
    (...args: unknown[]): ReturnType<typeof makePending>
    unsafe(text: string, params?: unknown[]): ReturnType<typeof makePending>
  }
  client.unsafe = (text: string) => makePending(text)
  return { client, executedCount: () => executed }
}

/** A node-postgres-shaped fake: an object executing eagerly via `.query()`. */
function fakePgLikeClient() {
  let calls = 0
  const client = {
    async query(text: string, _params?: unknown[]) {
      calls++
      return { rows: [{ text }] }
    },
    release() {
      /* no-op */
    },
  }
  return { client, callCount: () => calls }
}

function commandHandlerFn(
  handler: (
    message: unknown,
    ctx: CommandHandlerContext & PostgresCapability & { readonly trace?: SpanningTrace },
  ) => Promise<void>,
) {
  return handler
}

function ctxWith(uow: ReturnType<typeof unitOfWork> | undefined, trace?: SpanningTrace): never {
  return { unitOfWork: uow, trace } as never
}

const message = { payload: { id: "w-1" } }

describe("postgresHandler — with no trace on the context", () => {
  it("sql() answers the plain pool", async () => {
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    let seen: unknown
    const handler = postgresHandler(
      commandHandlerFn(async (_m, ctx) => {
        seen = ctx.sql()
      }),
      pool,
    )

    await handler(message as never, { unitOfWork: unitOfWork() } as never)

    expect(seen).toBe(pool)
  })
})

describe("postgresHandler — with a trace on the context", () => {
  it("pool fallback: query()/queryOne() are each one 'db.statement' span carrying the text, never the params", async () => {
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    const { trace, spans } = fakeTrace()
    let seen: PostgresAdapter | undefined
    const handler = postgresHandler(
      commandHandlerFn(async (_m, ctx) => {
        seen = ctx.sql() as PostgresAdapter
      }),
      pool,
    )

    await handler(message as never, ctxWith(unitOfWork(), trace))
    expect(seen).not.toBe(pool) // it is a VIEW, not the pool itself
    await seen!.query("SELECT 1", ["secret-param"])
    await seen!.queryOne("SELECT 2", ["another-secret"])

    expect(spans.map((span) => span.options)).toEqual([
      { name: "db.statement", attributes: { "db.query.text": "SELECT 1" } },
      { name: "db.statement", attributes: { "db.query.text": "SELECT 2" } },
    ])
    expect(JSON.stringify(spans)).not.toContain("secret")
  })

  it("transaction: query() is spanned once per call, under the SAME trace", async () => {
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    const make = postgresUnitOfWork(unitOfWork, pool)
    const { trace, spans } = fakeTrace()

    const handler = postgresHandler(
      commandHandlerFn(async (_m, ctx) => {
        const sql = ctx.sql()
        await sql.query("UPDATE widgets SET name = $1 WHERE id = $2", ["shhh", "w-1"])
        await sql.query("SELECT * FROM widgets WHERE id = $1", ["w-1"])
      }),
      pool,
    )

    await make().execute(async (uow) => {
      await postgresTransaction(uow)
      await handler(message as never, ctxWith(uow, trace))
    })

    expect(spans.map((span) => span.options?.attributes)).toEqual([
      { "db.query.text": "UPDATE widgets SET name = $1 WHERE id = $2" },
      { "db.query.text": "SELECT * FROM widgets WHERE id = $1" },
    ])
    expect(JSON.stringify(spans)).not.toContain("shhh")
  })

  it("has no active span at all when the handler never calls sql()", async () => {
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    const { trace, spans } = fakeTrace()
    const handler = postgresHandler(commandHandlerFn(async () => {}), pool)

    await handler(message as never, ctxWith(unitOfWork(), trace))

    expect(spans.length).toBe(0)
  })

  it("describes itself as supplying sql, and demands nothing — a trace is welcome, never required", () => {
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    const handler = postgresHandler(commandHandlerFn(async () => {}), pool)
    const [link] = chainOf(handler)
    expect(link?.name).toBe("postgresHandler")
    expect(link?.supplies).toEqual(["sql"])
    expect(link?.uses).toBeUndefined()
  })
})

describe("observedTransactionView() / observedPoolView() — unwrap()", () => {
  it("pg-like driver client (has .query): spans .query(), eagerly as it always was", async () => {
    const { trace, spans } = fakeTrace()
    const { client, callCount } = fakePgLikeClient()
    const tx: PostgresAdapterTransaction = {
      async query() {
        return []
      },
      unwrap: () => client,
    }

    const view = observedTransactionView(tx, trace)
    const wrapped = view.unwrap<typeof client>()

    expect(callCount()).toBe(0)
    await wrapped.query("SELECT 1", ["p"])
    expect(callCount()).toBe(1)
    expect(spans.length).toBe(1)
    expect(spans[0]!.options).toEqual({ name: "db.statement", attributes: { "db.query.text": "SELECT 1" } })

    // node-postgres' config-object form carries its text the same way.
    await wrapped.query({ text: "SELECT 2", values: ["p"] })
    expect(spans[1]!.options).toEqual({ name: "db.statement", attributes: { "db.query.text": "SELECT 2" } })
    expect(JSON.stringify(spans)).not.toContain('"p"')

    // Other members pass through untouched.
    expect(typeof wrapped.release).toBe("function")
  })

  it("postgres.js / Bun.sql-like driver client: .unsafe(...).values() chain is NOT executed early", async () => {
    const { trace, spans } = fakeTrace()
    const { client, executedCount } = fakePostgresJsLikeClient()
    const tx: PostgresAdapterTransaction = {
      async query() {
        return []
      },
      unwrap: () => client,
    }

    const view = observedTransactionView(tx, trace)
    const wrapped = view.unwrap<typeof client>()

    const pending = wrapped.unsafe("SELECT 1", ["p"])
    expect(executedCount()).toBe(0) // calling .unsafe() must not execute
    expect(spans.length).toBe(0)

    const chained = pending.values()
    expect(executedCount()).toBe(0) // chaining .values() must not execute either
    expect(spans.length).toBe(0)

    const result = await chained // only awaiting triggers execution
    expect(executedCount()).toBe(1)
    expect(spans.length).toBe(1)
    expect(spans[0]!.options).toEqual({ name: "db.statement", attributes: { "db.query.text": "SELECT 1" } })
    expect(result).toEqual([{ label: "SELECT 1:values" }])
    expect(JSON.stringify(spans)).not.toContain('"p"')
  })

  it("postgres.js / Bun.sql-like tag call: template strings are joined back with $n placeholders", async () => {
    const { trace, spans } = fakeTrace()
    const { client } = fakePostgresJsLikeClient()
    const view = observedPoolView({ ...fakeAdapter(), unwrap: () => client }, trace)
    const wrapped = view.unwrap<typeof client>()

    const id = "w-1"
    await wrapped`SELECT * FROM widgets WHERE id = ${id} AND name = ${"shhh"}`
    expect(spans[0]!.options).toEqual({
      name: "db.statement",
      attributes: { "db.query.text": "SELECT * FROM widgets WHERE id = $1 AND name = $2" },
    })
    expect(JSON.stringify(spans)).not.toContain("w-1")
    expect(JSON.stringify(spans)).not.toContain("shhh")
  })

  it("pool view: everything besides query/queryOne passes straight through", async () => {
    const { trace } = fakeTrace()
    const pg = fakeAdapter()
    const view = observedPoolView(pg, trace)

    expect(view.transaction).toBe(pg.transaction)
    expect(view.listen).toBe(pg.listen)
    expect(view.connect).toBe(pg.connect)
    expect(view.disconnect).toBe(pg.disconnect)
    expect(view.query).not.toBe(pg.query)
    expect(view.queryOne).not.toBe(pg.queryOne)
  })
})
