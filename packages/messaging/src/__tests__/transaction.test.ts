import { describe, expect, it } from "bun:test"
import {
  getActiveTransaction,
  transactionalUnitOfWorkFactory,
  type TransactionManager,
} from "../transaction.js"
import { defaultUnitOfWorkFactory } from "../unit-of-work.js"

describe("TransactionManager + transactionalUnitOfWorkFactory", () => {
  it("getActiveTransaction returns undefined when no transaction is active", () => {
    expect(getActiveTransaction()).toBeUndefined()
  })

  it("transactionalUnitOfWorkFactory makes the transaction available via getActiveTransaction inside the UoW, and invisible outside", async () => {
    const txManager: TransactionManager<{ id: string }> = {
      begin: async () => ({ id: "tx-1" }),
      commit: async () => {},
      rollback: async () => {},
    }
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    let inside: unknown = undefined
    await factory().executeWithResult(async () => {
      inside = getActiveTransaction()
    })

    expect(inside).toEqual({ id: "tx-1" })
    expect(getActiveTransaction()).toBeUndefined()
  })

  it("commits on success", async () => {
    const log: string[] = []
    const txManager: TransactionManager<string> = {
      begin: async () => {
        log.push("begin")
        return "tx"
      },
      commit: async () => {
        log.push("commit")
      },
      rollback: async () => {
        log.push("rollback")
      },
    }
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    await factory().executeWithResult(async () => {
      log.push("work")
    })

    expect(log).toEqual(["begin", "work", "commit"])
  })

  it("rolls back on error and rethrows", async () => {
    const log: string[] = []
    const txManager: TransactionManager<string> = {
      begin: async () => {
        log.push("begin")
        return "tx"
      },
      commit: async () => {
        log.push("commit")
      },
      rollback: async () => {
        log.push("rollback")
      },
    }
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    await expect(
      factory().executeWithResult(async () => {
        log.push("work")
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    expect(log).toEqual(["begin", "work", "rollback"])
  })

  it("transaction propagates across async boundaries inside the UoW", async () => {
    const txManager: TransactionManager<string> = {
      begin: async () => "tx-deep",
      commit: async () => {},
      rollback: async () => {},
    }
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    async function deepFunction(): Promise<string | undefined> {
      // Simulate an async call several layers deep
      await new Promise((r) => setTimeout(r, 1))
      return getActiveTransaction<string>()
    }

    let result: string | undefined

    await factory().executeWithResult(async () => {
      result = await deepFunction()
    })

    expect(result).toBe("tx-deep")
  })
})
