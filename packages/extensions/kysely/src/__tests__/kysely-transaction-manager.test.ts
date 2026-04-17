import { describe, expect, it } from "bun:test"
import { kyselyTransactionManager, type KyselyDatabaseLike } from "../kysely-transaction-manager.js"

// ---------------------------------------------------------------------------
// Mock Kysely database
// ---------------------------------------------------------------------------

function createMockKyselyDb(): KyselyDatabaseLike & { committed: boolean; rolledBack: boolean } {
  const state = { committed: false, rolledBack: false }

  return {
    get committed() { return state.committed },
    get rolledBack() { return state.rolledBack },

    transaction() {
      return {
        async execute<T>(fn: (trx: any) => Promise<T>): Promise<T> {
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
    },
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("kyselyTransactionManager", () => {
  it("begin returns a transaction client", async () => {
    // given
    const db = createMockKyselyDb()
    const txManager = kyselyTransactionManager(db)

    // when
    const tx = await txManager.begin()

    // then
    expect(tx).toBeDefined()
    // Clean up
    await txManager.commit(tx)
  })

  it("commit completes the transaction successfully", async () => {
    // given
    const db = createMockKyselyDb()
    const txManager = kyselyTransactionManager(db)

    // when
    const tx = await txManager.begin()
    await txManager.commit(tx)

    // then
    expect(db.committed).toBe(true)
    expect(db.rolledBack).toBe(false)
  })

  it("rollback aborts the transaction", async () => {
    // given
    const db = createMockKyselyDb()
    const txManager = kyselyTransactionManager(db)

    // when
    const tx = await txManager.begin()
    await txManager.rollback(tx)

    // then
    expect(db.rolledBack).toBe(true)
    expect(db.committed).toBe(false)
  })
})
