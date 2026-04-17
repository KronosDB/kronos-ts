import { describe, expect, it } from "bun:test"
import { knexTransactionManager, type KnexInstanceLike } from "../knex-transaction-manager.js"

// ---------------------------------------------------------------------------
// Mock Knex instance
// ---------------------------------------------------------------------------

function createMockKnex(): KnexInstanceLike & { committed: boolean; rolledBack: boolean } {
  const state = { committed: false, rolledBack: false }

  return {
    get committed() { return state.committed },
    get rolledBack() { return state.rolledBack },

    async transaction<T>(fn: (trx: any) => Promise<T>): Promise<T> {
      try {
        const result = await fn({ __mock: true })
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

describe("knexTransactionManager", () => {
  it("begin returns a transaction client", async () => {
    // given
    const knex = createMockKnex()
    const txManager = knexTransactionManager(knex)

    // when
    const tx = await txManager.begin()

    // then
    expect(tx).toBeDefined()
    // Clean up
    await txManager.commit(tx)
  })

  it("commit completes the transaction successfully", async () => {
    // given
    const knex = createMockKnex()
    const txManager = knexTransactionManager(knex)

    // when
    const tx = await txManager.begin()
    await txManager.commit(tx)

    // then
    expect(knex.committed).toBe(true)
    expect(knex.rolledBack).toBe(false)
  })

  it("rollback aborts the transaction", async () => {
    // given
    const knex = createMockKnex()
    const txManager = knexTransactionManager(knex)

    // when
    const tx = await txManager.begin()
    await txManager.rollback(tx)

    // then
    expect(knex.rolledBack).toBe(true)
    expect(knex.committed).toBe(false)
  })
})
