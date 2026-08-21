import { describe, expect, it } from "bun:test"
import { unitOfWork, type UnitOfWork } from "@kronos-ts/core"
import {
  activeTransaction,
  adapterUnitOfWork,
  openTransaction,
  transactionRegistry,
  type TransactionHooks,
} from "../transaction-glue.js"

/**
 * The glue `drizzleUnitOfWork` / `drizzleTransaction` /
 * `activeDrizzleTransaction` are built from — private to this package, and no
 * longer borrowed from core.
 *
 * What it has to guarantee is ordering: exactly ONE transaction per unit of
 * work, commit in the COMMIT phase, rollback on error but never after a
 * successful commit, and — this package being an EAGER one — the transaction
 * forced open at PRE_INVOCATION, before the action runs. It touches nothing on
 * the handle but the PUBLIC phase API, which is precisely why it could leave
 * core: the base `UnitOfWork` has no transaction concept to reach into, and
 * this glue never needed one.
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
    /** Stands in for `drizzleUnitOfWork(unitOfWork, db)`. */
    factory: adapterUnitOfWork(registry, hooks, unitOfWork),
    /** Stands in for `drizzleTransaction(uow)`. */
    transaction: (uow: UnitOfWork) => openTransaction(registry, uow, "drizzleUnitOfWork"),
    /** Stands in for `activeDrizzleTransaction(uow)`. */
    active: (uow: UnitOfWork | undefined) => activeTransaction(registry, uow),
  }
}

describe("drizzle's adapterUnitOfWork — eager, the only mode this package has", () => {
  it("begins before the action and commits after it", async () => {
    const a = fakeAdapter()
    await a.factory().execute(async () => { a.log.push("handler") })
    expect(a.log).toEqual(["begin:tx-1", "handler", "commit:tx-1"])
  })

  it("rolls back on failure, rethrows, and does not commit", async () => {
    const a = fakeAdapter()
    await expect(
      a.factory().execute(async () => {
        a.log.push("handler")
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")
    expect(a.log).toEqual(["begin:tx-1", "handler", "rollback:tx-1"])
  })

  it("gives each unit of work its own transaction", async () => {
    const a = fakeAdapter()
    await a.factory().execute(async () => {})
    await a.factory().execute(async () => {})
    expect(a.log).toEqual(["begin:tx-1", "commit:tx-1", "begin:tx-2", "commit:tx-2"])
  })

  it("opens exactly one transaction however many times it is asked for", async () => {
    const a = fakeAdapter()
    await a.factory().execute(async (uow) => {
      const first = await a.transaction(uow)
      const second = await a.transaction(uow)
      expect(second).toBe(first)
    })
    expect(a.log.filter((l) => l.startsWith("begin"))).toEqual(["begin:tx-1"])
  })

  it("the OBSERVING accessor sees the eagerly-begun transaction", async () => {
    const a = fakeAdapter()
    await a.factory().execute(async (uow) => {
      expect(a.active(uow)).toEqual({ id: "tx-1" })
    })
  })

  it("the transaction spans the handler AND the PREPARE_COMMIT hooks", async () => {
    const a = fakeAdapter()
    let seenAtPrepare: unknown
    await a.factory().execute(async (uow) => {
      uow.onPrepareCommit(() => { seenAtPrepare = a.active(uow) })
    })
    expect(seenAtPrepare).toEqual({ id: "tx-1" })
  })
})

describe("the registry is private to the package that owns it", () => {
  it("asking a foreign unit of work for a transaction is a wiring error, not `undefined`", async () => {
    const a = fakeAdapter()
    await unitOfWork().execute(async (uow) => {
      expect(a.active(uow)).toBeUndefined()
      await expect(a.transaction(uow)).rejects.toThrow(/not minted by drizzleUnitOfWork/)
    })
  })

  it("two registries over the same unit of work do not see each other's transactions", async () => {
    const a = fakeAdapter()
    const b = fakeAdapter()
    await a.factory().execute(async (uow) => {
      expect(a.active(uow)).toEqual({ id: "tx-1" })
      // `uow` was minted by a's factory, so b never claimed it.
      expect(b.active(uow)).toBeUndefined()
    })
    expect(b.log).toEqual([])
  })
})
