import { describe, expect, it } from "bun:test"
import { typeormTransactionManager, type TypeOrmDataSourceLike } from "../typeorm-transaction-manager.js"

// ---------------------------------------------------------------------------
// Mock TypeORM data source
// ---------------------------------------------------------------------------

function createMockDataSource(): TypeOrmDataSourceLike & { committed: boolean; rolledBack: boolean } {
  const state = { committed: false, rolledBack: false }

  return {
    get committed() { return state.committed },
    get rolledBack() { return state.rolledBack },

    async transaction<T>(fn: (entityManager: any) => Promise<T>): Promise<T> {
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

describe("typeormTransactionManager", () => {
  it("begin returns a transaction client", async () => {
    // given
    const dataSource = createMockDataSource()
    const txManager = typeormTransactionManager(dataSource)

    // when
    const tx = await txManager.begin()

    // then
    expect(tx).toBeDefined()
    // Clean up
    await txManager.commit(tx)
  })

  it("commit completes the transaction successfully", async () => {
    // given
    const dataSource = createMockDataSource()
    const txManager = typeormTransactionManager(dataSource)

    // when
    const tx = await txManager.begin()
    await txManager.commit(tx)

    // then
    expect(dataSource.committed).toBe(true)
    expect(dataSource.rolledBack).toBe(false)
  })

  it("rollback aborts the transaction", async () => {
    // given
    const dataSource = createMockDataSource()
    const txManager = typeormTransactionManager(dataSource)

    // when
    const tx = await txManager.begin()
    await txManager.rollback(tx)

    // then
    expect(dataSource.rolledBack).toBe(true)
    expect(dataSource.committed).toBe(false)
  })
})
