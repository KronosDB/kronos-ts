import { describe, it, expect } from "bun:test"
import { unitOfWork } from "@kronos-ts/core"
import type { CommandHandler, CommandHandlerContext, EventHandlerContext, QueryHandlerContext } from "@kronos-ts/core"
import type { PostgresAdapter, PostgresAdapterTransaction, ListenSubscription } from "../adapter.js"
import type { IsolationLevel } from "../adapter.js"
import { postgresPool } from "../postgres-pool.js"
import { postgresTransaction, postgresUnitOfWork } from "../postgres-transaction.js"
import {
  postgresHandler,
  type PostgresCapability,
} from "../postgres-handler.js"

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
  }
}

// What is under test is the ctx the wrapper builds, so the handlers are plain
// functions — the wrapper takes a FUNCTION, and an entry only ever appears
// where the host spreads one.
function commandHandlerFn(
  handler: (message: unknown, ctx: CommandHandlerContext & PostgresCapability) => Promise<void>,
): (message: unknown, ctx: CommandHandlerContext & PostgresCapability) => Promise<void> {
  return handler
}
function eventHandlerFn(
  handler: (message: unknown, ctx: EventHandlerContext & PostgresCapability) => Promise<void>,
): (message: unknown, ctx: EventHandlerContext & PostgresCapability) => Promise<void> {
  return handler
}
function queryHandlerFn(
  handler: (message: unknown, ctx: QueryHandlerContext & PostgresCapability) => Promise<unknown>,
): (message: unknown, ctx: QueryHandlerContext & PostgresCapability) => Promise<unknown> {
  return handler
}

/** The minimum a handler body here touches — the rest of the ctx is unused. */
function ctxWith(uow: ReturnType<typeof unitOfWork> | undefined): never {
  return { unitOfWork: uow } as never
}

const message = { payload: { id: "w-1" } }

describe("postgresHandler", () => {
  it("adds sql() to a COMMAND handler's context, answering the pool outside a transaction", async () => {
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    let seen: unknown
    const handler = postgresHandler(
      commandHandlerFn(async (_m, ctx) => {
        seen = ctx.sql()
      }),
      pool,
    )

    await handler(message as never, ctxWith(unitOfWork()))

    expect(seen).toBe(pool)
  })

  it("answers the unit of work's TRANSACTION once one is open — same tx as every other writer", async () => {
    // The whole premise of the family: a handler's own writes and the token
    // store's writes are the same transaction because they read one registry.
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    const make = postgresUnitOfWork(unitOfWork, pool)

    let seen: unknown
    let opened: unknown
    const handler = postgresHandler(
      commandHandlerFn(async (_m, ctx) => {
        seen = ctx.sql()
      }),
      pool,
    )

    await make().execute(async (uow) => {
      opened = await postgresTransaction(uow)
      await handler(message as never, ctxWith(uow))
    })

    expect(seen).toBe(opened)
    expect(seen).not.toBe(pool)
  })

  it("never OPENS a transaction — sql() only observes", async () => {
    const adapter = fakeAdapter()
    let begins = 0
    const counting: PostgresAdapter = {
      ...adapter,
      async transaction(isolationLevel, fn) {
        begins++
        return adapter.transaction(isolationLevel, fn)
      },
    }
    const pool = postgresPool(counting, { bootstrap: false })
    const make = postgresUnitOfWork(unitOfWork, pool)
    const handler = postgresHandler(
      commandHandlerFn(async (_m, ctx) => {
        ctx.sql()
      }),
      pool,
    )

    await make().execute(async (uow) => {
      await handler(message as never, ctxWith(uow))
    })

    expect(begins).toBe(0)
  })

  it("is ONE function for all three kinds — event and query handlers too", async () => {
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    const seen: unknown[] = []

    const onEdited = postgresHandler(
      eventHandlerFn(async (_m, ctx) => {
        seen.push(ctx.sql())
      }),
      pool,
    )
    const readIt = postgresHandler(
      queryHandlerFn(async (_m, ctx) => {
        seen.push(ctx.sql())
        return null
      }),
      pool,
    )

    await onEdited(message as never, ctxWith(undefined))
    await readIt(message as never, ctxWith(undefined))

    expect(seen).toEqual([pool, pool])
  })

  it("leaves the ENTRY to the host — the spread carries every other field", async () => {
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    const entry: CommandHandler<any, any, CommandHandlerContext & PostgresCapability> = {
      kind: "command-handler",
      descriptor: {} as never,
      handler: async (_m, ctx: CommandHandlerContext & PostgresCapability) => {
        ctx.sql()
      },
    }
    const named = { ...entry, name: "editor" }

    const wrapped = { ...named, handler: postgresHandler(named.handler, pool) }
    expect(wrapped.kind).toBe("command-handler")
    expect(wrapped.name).toBe("editor")
    expect(wrapped.descriptor).toBe(entry.descriptor)
  })

  it("preserves the base context it was handed", async () => {
    const pool = postgresPool(fakeAdapter(), { bootstrap: false })
    let sawAppend = false
    const handler = postgresHandler(
      commandHandlerFn(async (_m, ctx) => {
        sawAppend = typeof ctx.append === "function"
      }),
      pool,
    )
    await handler(
      message as never,
      {
        unitOfWork: unitOfWork(),
        append: () => {},
      } as never,
    )
    expect(sawAppend).toBe(true)
  })
})
