import { describe, expect, it } from "bun:test"
import { type CommandHandler, unitOfWork } from "@kronos-ts/core"
import {
  activeKnexTransaction,
  type KnexClient,
  type KnexCommandContext,
  knexHandler,
  knexTransaction,
  knexUnitOfWork,
} from "../knex-transaction.js"

// ---------------------------------------------------------------------------
// Mock Knex instance — records what the callback-scoped transaction did.
// ---------------------------------------------------------------------------

type MockKnex = KnexClient & {
  readonly committed: boolean
  readonly rolledBack: boolean
  readonly opened: number
  readonly tx: unknown
}

function createMockKnex(): MockKnex {
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

    async transaction<T>(fn: (trx: any) => Promise<T>): Promise<T> {
      state.opened += 1
      const trx = { __mock: true }
      state.tx = trx
      try {
        const result = await fn(trx)
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
// knexUnitOfWork — begin / commit / rollback, driven by the UoW lifecycle
// ---------------------------------------------------------------------------

describe("knexUnitOfWork", () => {
  it("opens a transaction before the action runs", async () => {
    // given
    const knex = createMockKnex()
    const runUoW = knexUnitOfWork(unitOfWork, knex)

    // when
    let seen: unknown
    await runUoW().execute(async (uow) => {
      seen = activeKnexTransaction(uow)
    })

    // then
    expect(knex.opened).toBe(1)
    expect(seen).toBe(knex.tx)
  })

  it("commits the transaction when the unit of work completes", async () => {
    // given
    const knex = createMockKnex()
    const runUoW = knexUnitOfWork(unitOfWork, knex)

    // when
    await runUoW().execute(async () => {})

    // then
    expect(knex.committed).toBe(true)
    expect(knex.rolledBack).toBe(false)
  })

  it("rolls the transaction back when the unit of work fails", async () => {
    // given
    const knex = createMockKnex()
    const runUoW = knexUnitOfWork(unitOfWork, knex)

    // when
    await expect(
      runUoW().execute(async () => {
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    // then
    expect(knex.rolledBack).toBe(true)
    expect(knex.committed).toBe(false)
  })

  it("opens exactly one transaction per unit of work", async () => {
    // given
    const knex = createMockKnex()
    const runUoW = knexUnitOfWork(unitOfWork, knex)

    // when
    await runUoW().execute(async (uow) => {
      const a = await knexTransaction(uow)
      const b = await knexTransaction(uow)
      expect(a).toBe(b)
    })

    // then
    expect(knex.opened).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// The accessor pair
// ---------------------------------------------------------------------------

describe("knexTransaction / activeKnexTransaction", () => {
  it("knexTransaction rejects a unit of work this adapter did not mint", async () => {
    expect(knexTransaction(unitOfWork())).rejects.toThrow(/knexUnitOfWork/)
  })

  it("activeKnexTransaction never opens one", async () => {
    expect(activeKnexTransaction(unitOfWork())).toBeUndefined()
    expect(activeKnexTransaction(undefined)).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// knexHandler — ONE wrapper over the handler FUNCTION, ctx gains knex()
// ---------------------------------------------------------------------------

/** A command handler that REQUIRES the knex context — what the wrapper takes. */
function handlerReading(read: (ctx: KnexCommandContext) => void) {
  return async (_message: unknown, ctx: KnexCommandContext): Promise<void> => {
    read(ctx)
  }
}

describe("knexHandler", () => {
  it("gives the handler ctx the unit of work's transaction", async () => {
    // given
    const knex = createMockKnex()
    let handle: unknown
    const handler = knexHandler(
      handlerReading((ctx) => {
        handle = ctx.knex()
      }),
      knex,
    )

    // when
    const runUoW = knexUnitOfWork(unitOfWork, knex)
    await runUoW().execute(async (uow) => {
      await handler({ payload: { id: "a" } } as never, { unitOfWork: uow } as never)
    })

    // then
    expect(handle).toBe(knex.tx)
  })

  it("falls back to the base handle outside one of its transactions", async () => {
    // given
    const knex = createMockKnex()
    let handle: unknown
    const handler = knexHandler(
      handlerReading((ctx) => {
        handle = ctx.knex()
      }),
      knex,
    )

    // when — a unit of work with no knex transaction
    await handler({ payload: { id: "a" } } as never, { unitOfWork: unitOfWork() } as never)

    // then
    expect(handle).toBe(knex)
    expect(knex.opened).toBe(0)
  })

  it("leaves the ENTRY to the host — the spread carries every other field", async () => {
    // The wrapper knows nothing about entries; wrapping is the host's own
    // `{ ...h, handler: … }`, which is exactly why nothing else can be lost.
    const knex = createMockKnex()
    const entry: CommandHandler<any, any, KnexCommandContext> = {
      kind: "command-handler",
      descriptor: {} as never,
      handler: async (_message, ctx: KnexCommandContext) => {
        ctx.knex()
      },
    }

    const wrapped = { ...entry, handler: knexHandler(entry.handler, knex) }

    expect(wrapped.kind).toBe("command-handler")
    expect(wrapped.descriptor).toBe(entry.descriptor)
    expect(wrapped.handler).not.toBe(entry.handler)
    await wrapped.handler({} as never, { unitOfWork: unitOfWork() } as never)
  })
})
