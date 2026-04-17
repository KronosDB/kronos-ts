import { describe, expect, it } from "bun:test"
import { getActiveTransaction, runInTransaction, type TransactionManager } from "../transaction.js"

describe("TransactionManager + AsyncLocalStorage", () => {
  it("getActiveTransaction returns undefined when no transaction is active", () => {
    expect(getActiveTransaction()).toBeUndefined()
  })

  it("runInTransaction makes the transaction available via getActiveTransaction", async () => {
    const txManager: TransactionManager<{ id: string }> = {
      begin: async () => ({ id: "tx-1" }),
      commit: async () => {},
      rollback: async () => {},
    }

    let capturedTx: unknown = undefined

    await runInTransaction(txManager, async (tx) => {
      capturedTx = getActiveTransaction()
      expect(tx.id).toBe("tx-1")
    })

    expect(capturedTx).toEqual({ id: "tx-1" })
  })

  it("transaction is not visible after runInTransaction completes", async () => {
    const txManager: TransactionManager<string> = {
      begin: async () => "tx-2",
      commit: async () => {},
      rollback: async () => {},
    }

    await runInTransaction(txManager, async () => {
      expect(getActiveTransaction()).toBe("tx-2")
    })

    expect(getActiveTransaction()).toBeUndefined()
  })

  it("commits on success", async () => {
    const log: string[] = []
    const txManager: TransactionManager<string> = {
      begin: async () => { log.push("begin"); return "tx" },
      commit: async () => { log.push("commit") },
      rollback: async () => { log.push("rollback") },
    }

    await runInTransaction(txManager, async () => {
      log.push("work")
    })

    expect(log).toEqual(["begin", "work", "commit"])
  })

  it("rolls back on error and rethrows", async () => {
    const log: string[] = []
    const txManager: TransactionManager<string> = {
      begin: async () => { log.push("begin"); return "tx" },
      commit: async () => { log.push("commit") },
      rollback: async () => { log.push("rollback") },
    }

    expect(
      runInTransaction(txManager, async () => {
        log.push("work")
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    await new Promise((r) => setTimeout(r, 10))
    expect(log).toEqual(["begin", "work", "rollback"])
  })

  it("transaction propagates across async boundaries", async () => {
    const txManager: TransactionManager<string> = {
      begin: async () => "tx-deep",
      commit: async () => {},
      rollback: async () => {},
    }

    async function deepFunction(): Promise<string | undefined> {
      // Simulate an async call several layers deep
      await new Promise((r) => setTimeout(r, 1))
      return getActiveTransaction<string>()
    }

    let result: string | undefined

    await runInTransaction(txManager, async () => {
      result = await deepFunction()
    })

    expect(result).toBe("tx-deep")
  })
})
