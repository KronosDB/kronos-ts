import { describe, it, expect } from "bun:test"
import { IsolationLevel } from "../adapter.js"
import type {
  PostgresAdapter,
  PostgresAdapterTransaction,
  ListenSubscription,
} from "../adapter.js"
import { postgresTransactionManager } from "../postgres-transaction-manager.js"

/**
 * Fake adapter that records lifecycle markers in order. transaction(IL, fn)
 * records "begin", runs `fn(tx)`, then records "commit" on resolve or
 * "rollback" on reject. Mirrors the kysely-transaction-manager.test.ts shape.
 */
function createRecordingAdapter() {
  const log: string[] = []
  const adapter: PostgresAdapter = {
    async query() { return [] },
    async queryOne() { return null },
    async transaction<T>(
      isolationLevel: IsolationLevel,
      fn: (tx: PostgresAdapterTransaction) => Promise<T>,
    ): Promise<T> {
      log.push(`begin:${isolationLevel}`)
      try {
        const result = await fn({
          unwrap<T = unknown>(): T {
            return undefined as unknown as T
          },
          async query(sql: string) {
            log.push(`query:${sql}`)
            return []
          },
        })
        log.push("commit")
        return result
      } catch (err) {
        log.push("rollback")
        throw err
      }
    },
    async listen(): Promise<ListenSubscription> { return { async unlisten() {} } },
    async connect() {},
    async disconnect() {},
  }
  return { adapter, log }
}

describe("postgresTransactionManager", () => {
  it("begin() opens a pg tx and returns a queryable handle", async () => {
    const { adapter, log } = createRecordingAdapter()
    const tm = postgresTransactionManager(adapter)

    const tx = await tm.begin()
    expect(typeof tx.query).toBe("function")
    expect(log).toEqual(["begin:READ COMMITTED"])

    await tx.query("SELECT 1")
    expect(log).toEqual(["begin:READ COMMITTED", "query:SELECT 1"])

    await tm.commit(tx)
    expect(log).toEqual(["begin:READ COMMITTED", "query:SELECT 1", "commit"])
  })

  it("rollback() makes the adapter ROLLBACK without committing", async () => {
    const { adapter, log } = createRecordingAdapter()
    const tm = postgresTransactionManager(adapter)

    const tx = await tm.begin()
    await tx.query("INSERT ...")
    await tm.rollback(tx)

    expect(log).toEqual(["begin:READ COMMITTED", "query:INSERT ...", "rollback"])
  })

  it("honors a non-default isolation level", async () => {
    const { adapter, log } = createRecordingAdapter()
    const tm = postgresTransactionManager(adapter, IsolationLevel.SERIALIZABLE)

    const tx = await tm.begin()
    await tm.commit(tx)

    expect(log).toEqual(["begin:SERIALIZABLE", "commit"])
  })

  it("queries between begin and commit run on the same pinned connection", async () => {
    // The recording adapter only ever creates ONE inner tx handle per
    // adapter.transaction() call, so multiple queries inside one begin/commit
    // pair must all log against that single handle — covered by sequence below.
    const { adapter, log } = createRecordingAdapter()
    const tm = postgresTransactionManager(adapter)

    const tx = await tm.begin()
    await tx.query("A")
    await tx.query("B")
    await tx.query("C")
    await tm.commit(tx)

    expect(log).toEqual([
      "begin:READ COMMITTED",
      "query:A",
      "query:B",
      "query:C",
      "commit",
    ])
  })

  it("two sequential begin/commit pairs open two distinct txes", async () => {
    const { adapter, log } = createRecordingAdapter()
    const tm = postgresTransactionManager(adapter)

    const a = await tm.begin()
    await tm.commit(a)
    const b = await tm.begin()
    await tm.commit(b)

    expect(log).toEqual([
      "begin:READ COMMITTED", "commit",
      "begin:READ COMMITTED", "commit",
    ])
  })
})
