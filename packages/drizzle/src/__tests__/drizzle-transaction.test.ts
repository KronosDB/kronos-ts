import { describe, expect, it } from "bun:test"
import { type CommandHandler, type CommandHandlerContext, unitOfWork } from "@kronos-ts/core"
import {
  activeDrizzleTransaction,
  type DrizzleCapability,
  type DrizzleDb,
  drizzleHandler,
  drizzleTransaction,
  drizzleUnitOfWork,
} from "../drizzle-transaction.js"

// ---------------------------------------------------------------------------
// Mock Drizzle database — records what the callback-scoped transaction did.
// ---------------------------------------------------------------------------

type MockDb = DrizzleDb & {
  readonly committed: boolean
  readonly rolledBack: boolean
  readonly opened: number
  readonly tx: unknown
}

function createMockDb(): MockDb {
  const state = { committed: false, rolledBack: false, opened: 0, tx: undefined as unknown }

  return {
    get committed() {
      return state.committed
    },
    get rolledBack() {
      return state.rolledBack
    },
    get opened() {
      return state.opened
    },
    get tx() {
      return state.tx
    },

    async transaction<T>(fn: (tx: any) => Promise<T>): Promise<T> {
      state.opened += 1
      const tx = { __mock: true }
      state.tx = tx
      try {
        const result = await fn(tx)
        state.committed = true
        return result
      } catch (err) {
        state.rolledBack = true
        throw err
      }
    },
  }
}

// ---------------------------------------------------------------------------
// drizzleUnitOfWork — begin / commit / rollback, driven by the UoW lifecycle
// ---------------------------------------------------------------------------

describe("drizzleUnitOfWork", () => {
  it("opens a transaction before the action runs", async () => {
    // given
    const db = createMockDb()
    const runUoW = drizzleUnitOfWork(unitOfWork, db)

    // when
    let seen: unknown
    await runUoW().execute(async (uow) => {
      seen = activeDrizzleTransaction(uow)
    })

    // then
    expect(db.opened).toBe(1)
    expect(seen).toBe(db.tx)
  })

  it("commits the transaction when the unit of work completes", async () => {
    // given
    const db = createMockDb()
    const runUoW = drizzleUnitOfWork(unitOfWork, db)

    // when
    await runUoW().execute(async () => {})

    // then
    expect(db.committed).toBe(true)
    expect(db.rolledBack).toBe(false)
  })

  it("rolls the transaction back when the unit of work fails", async () => {
    // given
    const db = createMockDb()
    const runUoW = drizzleUnitOfWork(unitOfWork, db)

    // when
    await expect(
      runUoW().execute(async () => {
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    // then
    expect(db.rolledBack).toBe(true)
    expect(db.committed).toBe(false)
  })

  it("opens exactly one transaction per unit of work", async () => {
    // given
    const db = createMockDb()
    const runUoW = drizzleUnitOfWork(unitOfWork, db)

    // when
    await runUoW().execute(async (uow) => {
      const a = await drizzleTransaction(uow)
      const b = await drizzleTransaction(uow)
      expect(a).toBe(b)
    })

    // then
    expect(db.opened).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// The accessor pair
// ---------------------------------------------------------------------------

describe("drizzleTransaction / activeDrizzleTransaction", () => {
  it("drizzleTransaction rejects a unit of work this adapter did not mint", async () => {
    expect(drizzleTransaction(unitOfWork())).rejects.toThrow(/drizzleUnitOfWork/)
  })

  it("activeDrizzleTransaction never opens one", async () => {
    // given a foreign unit of work
    expect(activeDrizzleTransaction(unitOfWork())).toBeUndefined()
    expect(activeDrizzleTransaction(undefined)).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// drizzleHandler — ONE wrapper over the handler FUNCTION, ctx gains db()
// ---------------------------------------------------------------------------

/** A command handler that REQUIRES the drizzle context — what the wrapper takes. */
function handlerReading(read: (ctx: CommandHandlerContext & DrizzleCapability) => void) {
  return async (_message: unknown, ctx: CommandHandlerContext & DrizzleCapability): Promise<void> => {
    read(ctx)
  }
}

describe("drizzleHandler", () => {
  it("gives the handler ctx the unit of work's transaction", async () => {
    // given
    const db = createMockDb()
    let handle: unknown
    const handler = drizzleHandler(
      handlerReading((ctx) => {
        handle = ctx.db()
      }),
      db,
    )

    // when
    const runUoW = drizzleUnitOfWork(unitOfWork, db)
    await runUoW().execute(async (uow) => {
      await handler({ payload: { id: "a" } } as never, { unitOfWork: uow } as never)
    })

    // then
    expect(handle).toBe(db.tx)
  })

  it("falls back to the base handle outside one of its transactions", async () => {
    // given
    const db = createMockDb()
    let handle: unknown
    const handler = drizzleHandler(
      handlerReading((ctx) => {
        handle = ctx.db()
      }),
      db,
    )

    // when — a unit of work with no drizzle transaction
    await handler({ payload: { id: "a" } } as never, { unitOfWork: unitOfWork() } as never)

    // then
    expect(handle).toBe(db)
    expect(db.opened).toBe(0)
  })

  it("leaves the ENTRY to the host — the spread carries every other field", async () => {
    // The wrapper knows nothing about entries; wrapping is the host's own
    // `{ ...h, handler: … }`, which is exactly why nothing else can be lost.
    const db = createMockDb()
    const entry: CommandHandler<any, any, CommandHandlerContext & DrizzleCapability> = {
      kind: "command-handler",
      descriptor: {} as never,
      handler: async (_message, ctx: CommandHandlerContext & DrizzleCapability) => {
        ctx.db()
      },
    }

    const wrapped = { ...entry, handler: drizzleHandler(entry.handler, db) }

    expect(wrapped.kind).toBe("command-handler")
    expect(wrapped.descriptor).toBe(entry.descriptor)
    expect(wrapped.handler).not.toBe(entry.handler)
    await wrapped.handler({} as never, { unitOfWork: unitOfWork() } as never)
  })
})
