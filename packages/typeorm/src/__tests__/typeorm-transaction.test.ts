import { describe, expect, it } from "bun:test"
import { type CommandHandlerDefinition, unitOfWork } from "@kronos-ts/core"
import {
  activeTypeormTransaction,
  type TypeormContext,
  type TypeormManager,
  typeormHandler,
  typeormTransaction,
  typeormUnitOfWork,
} from "../typeorm-transaction.js"

// ---------------------------------------------------------------------------
// Mock TypeORM data source — records what the callback-scoped transaction did.
// ---------------------------------------------------------------------------

interface MockDataSource extends TypeormManager {
  readonly committed: boolean
  readonly rolledBack: boolean
  readonly opened: number
  readonly tx: unknown
}

function createMockDataSource(): MockDataSource {
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

    async transaction<T>(fn: (entityManager: any) => Promise<T>): Promise<T> {
      state.opened += 1
      const entityManager = { __mock: true }
      state.tx = entityManager
      try {
        const result = await fn(entityManager)
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
// typeormUnitOfWork — begin / commit / rollback, driven by the UoW lifecycle
// ---------------------------------------------------------------------------

describe("typeormUnitOfWork", () => {
  it("opens a transaction before the action runs", async () => {
    // given
    const dataSource = createMockDataSource()
    const runUoW = typeormUnitOfWork(dataSource, unitOfWork)

    // when
    let seen: unknown
    await runUoW().execute(async (uow) => {
      seen = activeTypeormTransaction(uow)
    })

    // then
    expect(dataSource.opened).toBe(1)
    expect(seen).toBe(dataSource.tx)
  })

  it("commits the transaction when the unit of work completes", async () => {
    // given
    const dataSource = createMockDataSource()
    const runUoW = typeormUnitOfWork(dataSource, unitOfWork)

    // when
    await runUoW().execute(async () => {})

    // then
    expect(dataSource.committed).toBe(true)
    expect(dataSource.rolledBack).toBe(false)
  })

  it("rolls the transaction back when the unit of work fails", async () => {
    // given
    const dataSource = createMockDataSource()
    const runUoW = typeormUnitOfWork(dataSource, unitOfWork)

    // when
    await expect(
      runUoW().execute(async () => {
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    // then
    expect(dataSource.rolledBack).toBe(true)
    expect(dataSource.committed).toBe(false)
  })

  it("opens exactly one transaction per unit of work", async () => {
    // given
    const dataSource = createMockDataSource()
    const runUoW = typeormUnitOfWork(dataSource, unitOfWork)

    // when
    await runUoW().execute(async (uow) => {
      const a = await typeormTransaction(uow)
      const b = await typeormTransaction(uow)
      expect(a).toBe(b)
    })

    // then
    expect(dataSource.opened).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// The accessor pair
// ---------------------------------------------------------------------------

describe("typeormTransaction / activeTypeormTransaction", () => {
  it("typeormTransaction rejects a unit of work this adapter did not mint", async () => {
    expect(typeormTransaction(unitOfWork())).rejects.toThrow(/typeormUnitOfWork/)
  })

  it("activeTypeormTransaction never opens one", async () => {
    expect(activeTypeormTransaction(unitOfWork())).toBeUndefined()
    expect(activeTypeormTransaction(undefined)).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// typeormHandler — ONE wrapper over the handler FUNCTION, ctx gains manager()
// ---------------------------------------------------------------------------

/** A command handler that REQUIRES the typeorm context — what the wrapper takes. */
function handlerReading(read: (ctx: TypeormContext) => void) {
  return async (_message: unknown, ctx: TypeormContext): Promise<void> => {
    read(ctx)
  }
}

describe("typeormHandler", () => {
  it("gives the handler ctx the unit of work's transaction", async () => {
    // given
    const manager = createMockDataSource()
    let handle: unknown
    const handler = typeormHandler(
      handlerReading((ctx) => {
        handle = ctx.manager()
      }),
      manager,
    )

    // when
    const runUoW = typeormUnitOfWork(manager, unitOfWork)
    await runUoW().execute(async (uow) => {
      await handler({ payload: { id: "a" } } as never, { unitOfWork: uow } as never)
    })

    // then
    expect(handle).toBe(manager.tx)
  })

  it("falls back to the base handle outside one of its transactions", async () => {
    // given
    const manager = createMockDataSource()
    let handle: unknown
    const handler = typeormHandler(
      handlerReading((ctx) => {
        handle = ctx.manager()
      }),
      manager,
    )

    // when — a unit of work with no typeorm transaction
    await handler({ payload: { id: "a" } } as never, { unitOfWork: unitOfWork() } as never)

    // then
    expect(handle).toBe(manager)
    expect(manager.opened).toBe(0)
  })

  it("leaves the ENTRY to the host — the spread carries every other field", async () => {
    // The wrapper knows nothing about entries; wrapping is the host's own
    // `{ ...h, handler: … }`, which is exactly why nothing else can be lost.
    const manager = createMockDataSource()
    const entry: CommandHandlerDefinition<any, any, TypeormContext> = {
      kind: "command-handler",
      descriptor: {} as never,
      handler: async (_message, ctx: TypeormContext) => {
        ctx.manager()
      },
    }

    const wrapped = { ...entry, handler: typeormHandler(entry.handler, manager) }

    expect(wrapped.kind).toBe("command-handler")
    expect(wrapped.descriptor).toBe(entry.descriptor)
    expect(wrapped.handler).not.toBe(entry.handler)
    await wrapped.handler({} as never, { unitOfWork: unitOfWork() } as never)
  })
})
