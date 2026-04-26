import { describe, expect, it } from "bun:test"
import { emptyMetadata } from "@kronos-ts/common"
import {
  getActiveTransaction,
  transactionalUnitOfWorkFactory,
  type TransactionManager,
} from "../transaction.js"
import { runInUoW } from "../unit-of-work.js"

/**
 * Plan 03-04 (CTX-04 / D-34): `transactionalUnitOfWorkFactory` now wraps
 * a `UoWRunner` (`runInUoW`) and returns a new `UoWRunner`. The old
 * `factory().executeWithResult(...)` call shape is gone — call the runner
 * directly with `(metadata, action)`.
 */
describe("TransactionManager + transactionalUnitOfWorkFactory", () => {
  it("getActiveTransaction returns undefined when no transaction is active (permissive ALS read)", () => {
    expect(getActiveTransaction()).toBeUndefined()
  })

  it("makes the transaction visible inside the UoW and invisible outside", async () => {
    const txManager: TransactionManager<{ id: string }> = {
      begin: async () => ({ id: "tx-1" }),
      commit: async () => {},
      rollback: async () => {},
    }
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    let inside: unknown
    await txRunner(emptyMetadata(), async () => {
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
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    await txRunner(emptyMetadata(), async () => {
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
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    await expect(
      txRunner(emptyMetadata(), async () => {
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
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    async function deepFunction(): Promise<string | undefined> {
      // Simulate an async call several layers deep
      await new Promise((r) => setTimeout(r, 1))
      return getActiveTransaction<string>()
    }

    let result: string | undefined

    await txRunner(emptyMetadata(), async () => {
      result = await deepFunction()
    })

    expect(result).toBe("tx-deep")
  })
})
