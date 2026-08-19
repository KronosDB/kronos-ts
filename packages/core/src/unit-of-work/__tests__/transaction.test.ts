import { describe, expect, it } from "bun:test"
import { unitOfWork, type UnitOfWork } from "../unit-of-work.js"
import { activeTransaction, adapterUnitOfWork, claimed, openTransaction, transactionRegistry, type TransactionHooks } from "../transaction.js"

/**
 * The glue every adapter package is built from — `@kronos-ts/core/transaction`,
 * deliberately absent from the barrel.
 *
 * What it has to guarantee is ordering that would otherwise be re-derived (and
 * diverged) six times: exactly ONE transaction per unit of work, commit in the
 * COMMIT phase, rollback on error but never after a successful commit. It
 * touches nothing on the handle but the public phase API — which is the point,
 * since the base `UnitOfWork` has no transaction concept for it to reach into.
 *
 * These tests stand in for an adapter: a fake driver, a private registry, and
 * the same accessor pair every real adapter exports.
 */
function fakeAdapter(options: { eager?: boolean } = {}) {
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
    /** Stands in for `drizzleUnitOfWork(db, unitOfWork)`. */
    factory: adapterUnitOfWork(registry, hooks, unitOfWork, options),
    /** Stands in for `drizzleTransaction(uow)`. */
    transaction: (uow: UnitOfWork) => openTransaction(registry, uow, "fakeUnitOfWork"),
    /** Stands in for `activeDrizzleTransaction(uow)`. */
    active: (uow: UnitOfWork | undefined) => activeTransaction(registry, uow),
  }
}

describe("adapterUnitOfWork — eager (the default)", () => {
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

describe("adapterUnitOfWork — lazy", () => {
  it("begins nothing when no component asks", async () => {
    const a = fakeAdapter({ eager: false })
    await a.factory().execute(async () => { a.log.push("handler") })
    expect(a.log).toEqual(["handler"])
  })

  it("begins on the first request and still commits at COMMIT", async () => {
    const a = fakeAdapter({ eager: false })
    await a.factory().execute(async (uow) => {
      a.log.push("before")
      await a.transaction(uow)
      a.log.push("after")
    })
    expect(a.log).toEqual(["before", "begin:tx-1", "after", "commit:tx-1"])
  })

  it("the OBSERVING accessor does NOT provoke one — that is its whole contract", async () => {
    const a = fakeAdapter({ eager: false })
    await a.factory().execute(async (uow) => {
      expect(a.active(uow)).toBeUndefined()
    })
    expect(a.log).toEqual([])
  })

  it("rolls back a lazily-begun transaction on failure", async () => {
    const a = fakeAdapter({ eager: false })
    await expect(
      a.factory().execute(async (uow) => {
        await a.transaction(uow)
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")
    expect(a.log).toEqual(["begin:tx-1", "rollback:tx-1"])
  })
})

describe("the registry is private to the adapter that owns it", () => {
  it("a bare unit of work is claimed by nobody and has no transaction", async () => {
    const a = fakeAdapter()
    await unitOfWork().execute(async (uow) => {
      expect(claimed(a.registry, uow)).toBe(false)
      expect(a.active(uow)).toBeUndefined()
    })
  })

  it("asking a foreign unit of work for a transaction is a wiring error, not `undefined`", async () => {
    const a = fakeAdapter()
    await unitOfWork().execute(async (uow) => {
      await expect(a.transaction(uow)).rejects.toThrow(/not minted by fakeUnitOfWork/)
    })
  })

  it("two adapters over the same unit of work do not see each other's transactions", async () => {
    const a = fakeAdapter()
    const b = fakeAdapter()
    await a.factory().execute(async (uow) => {
      expect(a.active(uow)).toEqual({ id: "tx-1" })
      // `uow` was minted by a's factory, so b never claimed it.
      expect(claimed(b.registry, uow)).toBe(false)
      expect(b.active(uow)).toBeUndefined()
    })
    expect(b.log).toEqual([])
  })

  it("the handle itself exposes no transaction vocabulary at all", async () => {
    await unitOfWork().execute(async (uow) => {
      expect("transaction" in uow).toBe(false)
      expect("activeTransaction" in uow).toBe(false)
      expect("setTransaction" in uow).toBe(false)
      expect("setTransactionOpener" in uow).toBe(false)
    })
  })
})
