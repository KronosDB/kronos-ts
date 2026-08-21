import { describe, expect, it } from "bun:test"
import { unitOfWork, type UnitOfWork } from "@kronos-ts/core"
import {
  activeTransaction,
  adapterUnitOfWork,
  claimed,
  openTransaction,
  transactionRegistry,
  type TransactionHooks,
} from "../transaction-glue.js"

/**
 * The glue `postgresUnitOfWork` / `postgresTransaction` /
 * `activePostgresTransaction` are built from — private to this package, and no
 * longer borrowed from core.
 *
 * Postgres is the LAZY member of the persistence family, and this is where that
 * choice is pinned: the unit of work is CLAIMED at mint time, but nothing
 * begins until a writer asks, so this package's read paths pay no begin/commit
 * and claim no connection. Everything else it has to guarantee is the same
 * ordering every family member does — exactly ONE transaction per unit of work,
 * commit in the COMMIT phase, rollback on error but never after a successful
 * commit — over nothing but the PUBLIC phase API, which is precisely why this
 * glue could leave core.
 *
 * These tests stand in for the real driver: a fake begin/commit/rollback, a
 * private registry, and the same accessor pair the package exports.
 */
function fakeAdapter() {
  const log: string[] = []
  let counter = 0

  const hooks: TransactionHooks<{ id: string }> = {
    begin: async () => {
      const tx = { id: `tx-${++counter}` }
      log.push(`begin:${tx.id}`)
      return tx
    },
    commit: async (tx) => { log.push(`commit:${tx.id}`) },
    rollback: async (tx) => { log.push(`rollback:${tx.id}`) },
  }

  const registry = transactionRegistry<{ id: string }>()
  return {
    log,
    registry,
    /** Stands in for `postgresUnitOfWork(unitOfWork, pg)`. */
    factory: adapterUnitOfWork(registry, hooks, unitOfWork),
    /** Stands in for `postgresTransaction(uow)`. */
    transaction: (uow: UnitOfWork) => openTransaction(registry, uow, "postgresUnitOfWork"),
    /** Stands in for `activePostgresTransaction(uow)`. */
    active: (uow: UnitOfWork | undefined) => activeTransaction(registry, uow),
  }
}

describe("postgres's adapterUnitOfWork — lazy, its honest default", () => {
  it("begins nothing when no component asks", async () => {
    const a = fakeAdapter()
    await a.factory().execute(async () => { a.log.push("handler") })
    expect(a.log).toEqual(["handler"])
  })

  it("begins on the first request and still commits at COMMIT", async () => {
    const a = fakeAdapter()
    await a.factory().execute(async (uow) => {
      a.log.push("before")
      await a.transaction(uow)
      a.log.push("after")
    })
    expect(a.log).toEqual(["before", "begin:tx-1", "after", "commit:tx-1"])
  })

  it("the OBSERVING accessor does NOT provoke one — that is its whole contract", async () => {
    const a = fakeAdapter()
    await a.factory().execute(async (uow) => {
      expect(a.active(uow)).toBeUndefined()
    })
    expect(a.log).toEqual([])
  })

  it("rolls back a lazily-begun transaction on failure", async () => {
    const a = fakeAdapter()
    await expect(
      a.factory().execute(async (uow) => {
        await a.transaction(uow)
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")
    expect(a.log).toEqual(["begin:tx-1", "rollback:tx-1"])
  })
})

describe("claimed vs foreign — the discrimination lazy binding needs", () => {
  it("a bare unit of work is claimed by nobody and has no transaction", async () => {
    const a = fakeAdapter()
    await unitOfWork().execute(async (uow) => {
      expect(claimed(a.registry, uow)).toBe(false)
      expect(a.active(uow)).toBeUndefined()
      await expect(a.transaction(uow)).rejects.toThrow(/not minted by postgresUnitOfWork/)
    })
  })

  it("the handle itself exposes no transaction vocabulary at all", async () => {
    const a = fakeAdapter()
    await a.factory().execute(async (uow) => {
      // Claimed — but claiming leaves nothing on the handle to find.
      expect(claimed(a.registry, uow)).toBe(true)
      expect("transaction" in uow).toBe(false)
      expect("activeTransaction" in uow).toBe(false)
      expect("setTransaction" in uow).toBe(false)
      expect("setTransactionOpener" in uow).toBe(false)
    })
  })
})
