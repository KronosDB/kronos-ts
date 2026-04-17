import { describe, expect, it } from "bun:test"
import { prismaTransactionManager, type PrismaClientLike } from "../prisma-transaction-manager.js"

// ---------------------------------------------------------------------------
// Mock Prisma client
// ---------------------------------------------------------------------------

function createMockPrismaClient(): PrismaClientLike & { committed: boolean; rolledBack: boolean } {
  const state = { committed: false, rolledBack: false }

  return {
    get committed() { return state.committed },
    get rolledBack() { return state.rolledBack },

    async $transaction(fn, options) {
      try {
        const result = await fn({ __mock: true } as any)
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
// Tests
// ---------------------------------------------------------------------------

describe("prismaTransactionManager", () => {
  it("begin returns a transaction client", async () => {
    // given
    const prisma = createMockPrismaClient()
    const txManager = prismaTransactionManager(prisma)

    // when
    const tx = await txManager.begin()

    // then
    expect(tx).toBeDefined()
    // Clean up
    await txManager.commit(tx)
  })

  it("commit completes the transaction successfully", async () => {
    // given
    const prisma = createMockPrismaClient()
    const txManager = prismaTransactionManager(prisma)

    // when
    const tx = await txManager.begin()
    await txManager.commit(tx)

    // then
    expect(prisma.committed).toBe(true)
    expect(prisma.rolledBack).toBe(false)
  })

  it("rollback aborts the transaction", async () => {
    // given
    const prisma = createMockPrismaClient()
    const txManager = prismaTransactionManager(prisma)

    // when
    const tx = await txManager.begin()
    await txManager.rollback(tx)

    // then
    expect(prisma.rolledBack).toBe(true)
    expect(prisma.committed).toBe(false)
  })
})
