import { describe, it, expect } from "bun:test"
import { Phase, unitOfWork } from "@kronos-ts/core"
import { IsolationLevel } from "../adapter.js"
import type { PostgresAdapter, PostgresAdapterTransaction, ListenSubscription } from "../adapter.js"
import {
  activePostgresTransaction,
  postgresTransaction,
  postgresUnitOfWork,
} from "../postgres-transaction.js"

/**
 * Fake adapter that records lifecycle markers in order. transaction(IL, fn)
 * records "begin", runs `fn(tx)`, then records "commit" on resolve or
 * "rollback" on reject.
 *
 * Note: the per-transaction safety timeouts (SET LOCAL ...) are armed by the
 * concrete adapters' transaction() (see session-timeouts.ts + its test), NOT by
 * the unit-of-work factory — so this recording double issues no SET LOCAL, and
 * the factory is purely a begin/commit/rollback bridge.
 */
function createRecordingAdapter() {
  const log: string[] = []
  const adapter: PostgresAdapter = {
    async query() {
      return []
    },
    async queryOne() {
      return null
    },
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
    async listen(): Promise<ListenSubscription> {
      return { async unlisten() {} }
    },
    async connect() {},
    async disconnect() {},
  }
  return { adapter, log }
}

describe("postgresUnitOfWork", () => {
  it("opens a pg tx on first request and returns a queryable handle, committing at COMMIT", async () => {
    const { adapter, log } = createRecordingAdapter()
    const make = postgresUnitOfWork(unitOfWork, adapter)

    await make().execute(async (uow) => {
      const tx = await postgresTransaction(uow)
      expect(typeof tx.query).toBe("function")
      expect(log).toEqual(["begin:READ COMMITTED"])
      await tx.query("SELECT 1")
    })

    expect(log).toEqual(["begin:READ COMMITTED", "query:SELECT 1", "commit"])
  })

  it("a failing action makes the adapter ROLLBACK without committing", async () => {
    const { adapter, log } = createRecordingAdapter()
    const make = postgresUnitOfWork(unitOfWork, adapter)

    await expect(
      make().execute(async (uow) => {
        const tx = await postgresTransaction(uow)
        await tx.query("INSERT ...")
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    expect(log).toEqual(["begin:READ COMMITTED", "query:INSERT ...", "rollback"])
  })

  it("honors a non-default isolation level", async () => {
    const { adapter, log } = createRecordingAdapter()
    const make = postgresUnitOfWork(unitOfWork, adapter, IsolationLevel.SERIALIZABLE)

    await make().execute(async (uow) => {
      await postgresTransaction(uow)
    })

    expect(log).toEqual(["begin:SERIALIZABLE", "commit"])
  })

  it("queries between open and commit run on the same pinned connection", async () => {
    // The recording adapter only ever creates ONE inner tx handle per
    // adapter.transaction() call, so multiple queries inside one unit of work
    // must all log against that single handle — covered by the sequence below.
    const { adapter, log } = createRecordingAdapter()
    const make = postgresUnitOfWork(unitOfWork, adapter)

    await make().execute(async (uow) => {
      const tx = await postgresTransaction(uow)
      await tx.query("A")
      // The second request must NOT begin a second transaction: one unit of
      // work is one transaction.
      const again = await postgresTransaction(uow)
      expect(again).toBe(tx)
      await again.query("B")
      await tx.query("C")
    })

    expect(log).toEqual(["begin:READ COMMITTED", "query:A", "query:B", "query:C", "commit"])
  })

  it("two sequential units of work open two distinct txes", async () => {
    const { adapter, log } = createRecordingAdapter()
    const make = postgresUnitOfWork(unitOfWork, adapter)

    await make().execute(async (uow) => {
      await postgresTransaction(uow)
    })
    await make().execute(async (uow) => {
      await postgresTransaction(uow)
    })

    expect(log).toEqual(["begin:READ COMMITTED", "commit", "begin:READ COMMITTED", "commit"])
  })

  it("rejects (does not hang) when the adapter fails to open the tx", async () => {
    // Adapter whose transaction() rejects before invoking fn — models a
    // BEGIN-time failure (e.g. arming SET LOCAL throws inside the adapter).
    // Before the fix, the tx capture never ran and the open hung forever;
    // now it must reject.
    const adapter: PostgresAdapter = {
      async query() {
        return []
      },
      async queryOne() {
        return null
      },
      async transaction<T>(): Promise<T> {
        throw new Error("boom: BEGIN failed")
      },
      async listen(): Promise<ListenSubscription> {
        return { async unlisten() {} }
      },
      async connect() {},
      async disconnect() {},
    }
    const make = postgresUnitOfWork(unitOfWork, adapter)
    const uow = make()

    await expect(
      uow.execute(async (u) => {
        await postgresTransaction(u)
      }),
    ).rejects.toThrow(/boom: BEGIN failed/)
  })

  it("is LAZY — a unit of work nobody writes through claims no connection", async () => {
    // postgres's honest default, and the reason it opts out of the eager glue:
    // a pure-read unit of work must not pay a begin/commit round trip.
    const { adapter, log } = createRecordingAdapter()
    const make = postgresUnitOfWork(unitOfWork, adapter)

    await make().execute(async () => "read-only")

    expect(log).toEqual([])
  })
})

describe("activePostgresTransaction", () => {
  it("observes the open transaction and NEVER opens one", async () => {
    const { adapter, log } = createRecordingAdapter()
    const make = postgresUnitOfWork(unitOfWork, adapter)

    await make().execute(async (uow) => {
      // Nothing has written yet — the observer must not provoke a BEGIN.
      expect(activePostgresTransaction(uow)).toBeUndefined()
      expect(log).toEqual([])

      const tx = await postgresTransaction(uow)
      expect(activePostgresTransaction(uow)).toBe(tx)
    })
  })

  it("answers undefined for a unit of work this family did not mint", () => {
    const uow = unitOfWork()
    expect(activePostgresTransaction(uow)).toBeUndefined()
    expect(activePostgresTransaction(undefined)).toBeUndefined()
  })
})

describe("postgresTransaction", () => {
  it("REJECTS on a unit of work this family did not mint", async () => {
    // Answering `undefined` would push a wiring mistake downstream as a silent
    // non-transactional write.
    await expect(postgresTransaction(unitOfWork())).rejects.toThrow(/postgresUnitOfWork/)
  })

  it("commits in the COMMIT phase, not when the action returns", async () => {
    const { adapter, log } = createRecordingAdapter()
    const make = postgresUnitOfWork(unitOfWork, adapter)
    const uow = make()
    uow.on(Phase.PREPARE_COMMIT, async () => {
      log.push("prepare-commit")
    })

    await uow.execute(async (u) => {
      await postgresTransaction(u)
    })

    expect(log).toEqual(["begin:READ COMMITTED", "prepare-commit", "commit"])
  })
})
