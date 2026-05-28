import { describe, expect, it } from "bun:test"
import { emptyMetadata } from "@kronos-ts/common"
import { runInUoW } from "../unit-of-work.js"
import { onPrepareCommit, getResource } from "../processing-state.js"
import {
  lazyTransactionalUnitOfWorkFactory,
  getActiveTransaction,
  getOrBeginActiveTransaction,
  TRANSACTION_KEY,
  type TransactionManager,
} from "../transaction.js"

function createRecordingTxManager() {
  const log: string[] = []
  let txCounter = 0

  const txManager: TransactionManager<{ id: string }> = {
    begin: async () => {
      const tx = { id: `tx-${++txCounter}` }
      log.push(`begin:${tx.id}`)
      return tx
    },
    commit: async (tx) => { log.push(`commit:${tx.id}`) },
    rollback: async (tx) => { log.push(`rollback:${tx.id}`) },
  }

  return { txManager, log }
}

describe("lazyTransactionalUnitOfWorkFactory", () => {
  it("does NOT begin a transaction when no component requests one", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = lazyTransactionalUnitOfWorkFactory(runInUoW, txManager)

    await txRunner(emptyMetadata(), async () => {
      log.push("handler")
    })

    expect(log).toEqual(["handler"])
  })

  it("begins on first getOrBeginActiveTransaction call and commits at end", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = lazyTransactionalUnitOfWorkFactory(runInUoW, txManager)

    await txRunner(emptyMetadata(), async () => {
      log.push("handler-before")
      const tx = await getOrBeginActiveTransaction<{ id: string }>()
      log.push(`got:${tx?.id}`)
    })

    expect(log).toEqual(["handler-before", "begin:tx-1", "got:tx-1", "commit:tx-1"])
  })

  it("returns the same tx across multiple calls within one UoW", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = lazyTransactionalUnitOfWorkFactory(runInUoW, txManager)

    await txRunner(emptyMetadata(), async () => {
      const a = await getOrBeginActiveTransaction<{ id: string }>()
      const b = await getOrBeginActiveTransaction<{ id: string }>()
      expect(a).toBe(b)
    })

    expect(log).toEqual(["begin:tx-1", "commit:tx-1"])
  })

  it("rolls back when the handler throws after the tx was begun", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = lazyTransactionalUnitOfWorkFactory(runInUoW, txManager)

    await expect(
      txRunner(emptyMetadata(), async () => {
        await getOrBeginActiveTransaction<{ id: string }>()
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    expect(log).toEqual(["begin:tx-1", "rollback:tx-1"])
  })

  it("does not call rollback when the handler throws WITHOUT touching the tx", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = lazyTransactionalUnitOfWorkFactory(runInUoW, txManager)

    await expect(
      txRunner(emptyMetadata(), async () => {
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    expect(log).toEqual([])
  })

  it("a tx begun during INVOCATION is still active in PREPARE_COMMIT hooks", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = lazyTransactionalUnitOfWorkFactory(runInUoW, txManager)

    await txRunner(emptyMetadata(), async () => {
      await getOrBeginActiveTransaction<{ id: string }>()
      onPrepareCommit(async () => {
        const tx = getActiveTransaction<{ id: string }>()
        log.push(`prepare-commit-saw:${tx?.id}`)
      })
    })

    expect(log).toEqual(["begin:tx-1", "prepare-commit-saw:tx-1", "commit:tx-1"])
  })

  it("PREPARE_COMMIT can be the first caller — tx opens lazily there and still commits", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = lazyTransactionalUnitOfWorkFactory(runInUoW, txManager)

    await txRunner(emptyMetadata(), async () => {
      onPrepareCommit(async () => {
        const tx = await getOrBeginActiveTransaction<{ id: string }>()
        log.push(`prepare-commit-opened:${tx?.id}`)
      })
    })

    expect(log).toEqual(["begin:tx-1", "prepare-commit-opened:tx-1", "commit:tx-1"])
  })

  it("getOrBeginActiveTransaction returns undefined outside a UoW", async () => {
    expect(await getOrBeginActiveTransaction()).toBeUndefined()
  })

  it("getOrBeginActiveTransaction returns undefined when no factory installed", async () => {
    // runInUoW without any wrapping lazy/eager tx runner
    await runInUoW(emptyMetadata(), async () => {
      expect(await getOrBeginActiveTransaction()).toBeUndefined()
      expect(getResource(TRANSACTION_KEY)).toBeUndefined()
    })
  })
})
