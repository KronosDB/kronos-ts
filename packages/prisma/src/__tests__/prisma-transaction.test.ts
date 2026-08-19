import { describe, expect, it } from "bun:test"
import { type CommandHandlerDefinition, unitOfWork } from "@kronos-ts/core"
import {
  activePrismaTransaction,
  type PrismaClientLike,
  type PrismaContext,
  prismaHandler,
  prismaTransaction,
  prismaUnitOfWork,
} from "../prisma-transaction.js"

// ---------------------------------------------------------------------------
// Mock Prisma client — records what the callback-scoped transaction did.
// ---------------------------------------------------------------------------

interface MockPrisma extends PrismaClientLike {
  readonly committed: boolean
  readonly rolledBack: boolean
  readonly opened: number
  readonly tx: unknown
  readonly timeout: number | undefined
}

function createMockPrisma(): MockPrisma {
  const state = {
    committed: false,
    rolledBack: false,
    opened: 0,
    tx: undefined as unknown,
    timeout: undefined as number | undefined,
  }

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
    get timeout() {
      return state.timeout
    },

    async $transaction(fn, options) {
      state.opened += 1
      state.timeout = options?.timeout
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
// prismaUnitOfWork — begin / commit / rollback, driven by the UoW lifecycle
// ---------------------------------------------------------------------------

describe("prismaUnitOfWork", () => {
  it("opens a transaction before the action runs", async () => {
    // given
    const prisma = createMockPrisma()
    const runUoW = prismaUnitOfWork(prisma, unitOfWork)

    // when
    let seen: unknown
    await runUoW().execute(async (uow) => {
      seen = activePrismaTransaction(uow)
    })

    // then
    expect(prisma.opened).toBe(1)
    expect(seen).toBe(prisma.tx)
  })

  it("commits the transaction when the unit of work completes", async () => {
    // given
    const prisma = createMockPrisma()
    const runUoW = prismaUnitOfWork(prisma, unitOfWork)

    // when
    await runUoW().execute(async () => {})

    // then
    expect(prisma.committed).toBe(true)
    expect(prisma.rolledBack).toBe(false)
  })

  it("rolls the transaction back when the unit of work fails", async () => {
    // given
    const prisma = createMockPrisma()
    const runUoW = prismaUnitOfWork(prisma, unitOfWork)

    // when
    await expect(
      runUoW().execute(async () => {
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    // then
    expect(prisma.rolledBack).toBe(true)
    expect(prisma.committed).toBe(false)
  })

  it("hands the interactive-transaction timeout to $transaction", async () => {
    // given
    const prisma = createMockPrisma()
    const runUoW = prismaUnitOfWork(prisma, unitOfWork, { timeoutMs: 1234 })

    // when
    await runUoW().execute(async () => {})

    // then
    expect(prisma.timeout).toBe(1234)
  })

  it("opens exactly one transaction per unit of work", async () => {
    // given
    const prisma = createMockPrisma()
    const runUoW = prismaUnitOfWork(prisma, unitOfWork)

    // when
    await runUoW().execute(async (uow) => {
      const a = await prismaTransaction(uow)
      const b = await prismaTransaction(uow)
      expect(a).toBe(b)
    })

    // then
    expect(prisma.opened).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// The accessor pair
// ---------------------------------------------------------------------------

describe("prismaTransaction / activePrismaTransaction", () => {
  it("prismaTransaction rejects a unit of work this adapter did not mint", async () => {
    expect(prismaTransaction(unitOfWork())).rejects.toThrow(/prismaUnitOfWork/)
  })

  it("activePrismaTransaction never opens one", async () => {
    expect(activePrismaTransaction(unitOfWork())).toBeUndefined()
    expect(activePrismaTransaction(undefined)).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// prismaHandler — ONE wrapper over the handler FUNCTION, ctx gains prisma()
// ---------------------------------------------------------------------------

/** A command handler that REQUIRES the prisma context — what the wrapper takes. */
function handlerReading(read: (ctx: PrismaContext) => void) {
  return async (_message: unknown, ctx: PrismaContext): Promise<void> => {
    read(ctx)
  }
}

describe("prismaHandler", () => {
  it("gives the handler ctx the unit of work's transaction", async () => {
    // given
    const prisma = createMockPrisma()
    let handle: unknown
    const handler = prismaHandler(
      handlerReading((ctx) => {
        handle = ctx.prisma()
      }),
      prisma,
    )

    // when
    const runUoW = prismaUnitOfWork(prisma, unitOfWork)
    await runUoW().execute(async (uow) => {
      await handler({ payload: { id: "a" } } as never, { unitOfWork: uow } as never)
    })

    // then
    expect(handle).toBe(prisma.tx)
  })

  it("falls back to the base handle outside one of its transactions", async () => {
    // given
    const prisma = createMockPrisma()
    let handle: unknown
    const handler = prismaHandler(
      handlerReading((ctx) => {
        handle = ctx.prisma()
      }),
      prisma,
    )

    // when — a unit of work with no prisma transaction
    await handler({ payload: { id: "a" } } as never, { unitOfWork: unitOfWork() } as never)

    // then
    expect(handle).toBe(prisma)
    expect(prisma.opened).toBe(0)
  })

  it("leaves the ENTRY to the host — the spread carries every other field", async () => {
    // The wrapper knows nothing about entries; wrapping is the host's own
    // `{ ...h, handler: … }`, which is exactly why nothing else can be lost.
    const prisma = createMockPrisma()
    const entry: CommandHandlerDefinition<any, any, PrismaContext> = {
      kind: "command-handler",
      descriptor: {} as never,
      handler: async (_message, ctx: PrismaContext) => {
        ctx.prisma()
      },
    }

    const wrapped = { ...entry, handler: prismaHandler(entry.handler, prisma) }

    expect(wrapped.kind).toBe("command-handler")
    expect(wrapped.descriptor).toBe(entry.descriptor)
    expect(wrapped.handler).not.toBe(entry.handler)
    await wrapped.handler({} as never, { unitOfWork: unitOfWork() } as never)
  })
})
