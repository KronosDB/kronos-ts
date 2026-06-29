import { describe, expect, it } from "bun:test"
import { drizzleTransactionManager, type DrizzleDatabaseLike } from "../drizzle-transaction-manager.js"

function createMockDrizzleDb(): DrizzleDatabaseLike & { committed: boolean; rolledBack: boolean } {
  const state = { committed: false, rolledBack: false }
  return {
    get committed() { return state.committed },
    get rolledBack() { return state.rolledBack },
    async transaction(fn) {
      try {
        const result = await fn({ __mock: true })
        state.committed = true
        return result
      } catch {
        state.rolledBack = true
        throw new Error("rolled back")
      }
    },
  }
}

describe("drizzleTransactionManager", () => {
  it("begin returns a transaction", async () => {
    const db = createMockDrizzleDb()
    const txManager = drizzleTransactionManager(db)
    const tx = await txManager.begin()
    expect(tx).toBeDefined()
    await txManager.commit(tx)
  })

  it("commit completes successfully", async () => {
    const db = createMockDrizzleDb()
    const txManager = drizzleTransactionManager(db)
    const tx = await txManager.begin()
    await txManager.commit(tx)
    expect(db.committed).toBe(true)
  })

  it("rollback aborts the transaction", async () => {
    const db = createMockDrizzleDb()
    const txManager = drizzleTransactionManager(db)
    const tx = await txManager.begin()
    await txManager.rollback(tx)
    expect(db.rolledBack).toBe(true)
  })

  it("runs onBeginTransaction with the tx before begin() resolves", async () => {
    const db = createMockDrizzleDb()
    const seen: unknown[] = []
    const txManager = drizzleTransactionManager(db, {
      onBeginTransaction: async (tx) => {
        seen.push(tx)
      },
    })
    const tx = await txManager.begin()
    expect(seen).toEqual([tx])
    await txManager.commit(tx)
  })

  it("rejects begin() (does not hang) when onBeginTransaction throws", async () => {
    const db = createMockDrizzleDb()
    const txManager = drizzleTransactionManager(db, {
      onBeginTransaction: async () => {
        throw new Error("SET LOCAL failed")
      },
    })
    await expect(txManager.begin()).rejects.toThrow(/SET LOCAL failed/)
  })
})
