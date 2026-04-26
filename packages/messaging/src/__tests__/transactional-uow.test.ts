import { describe, expect, it } from "bun:test"
import { emptyMetadata } from "@kronos-ts/common"
import { runInUoW } from "../unit-of-work.js"
import { onPrepareCommit, getResource } from "../processing-state.js"
import {
  transactionalUnitOfWorkFactory,
  getActiveTransaction,
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

/**
 * Plan 03-04 (CTX-04 / D-34): `transactionalUnitOfWorkFactory` is now a
 * runner-wrapping function — it takes a `UoWRunner` (e.g. `runInUoW`) and
 * returns a new `UoWRunner` that begins/commits/rolls-back a transaction
 * around the wrapped action. The old `factory().executeWithResult(...)`
 * shape is gone.
 */
describe("transactionalUnitOfWorkFactory", () => {
  it("begins transaction before handler and commits after", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    await txRunner(emptyMetadata(), async () => {
      log.push("handler")
    })

    expect(log).toEqual(["begin:tx-1", "handler", "commit:tx-1"])
  })

  it("rolls back on handler failure", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    await expect(
      txRunner(emptyMetadata(), async () => {
        log.push("handler")
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    expect(log).toEqual(["begin:tx-1", "handler", "rollback:tx-1"])
  })

  it("makes transaction available via getActiveTransaction", async () => {
    const { txManager } = createRecordingTxManager()
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    let captured: unknown
    await txRunner(emptyMetadata(), async () => {
      captured = getActiveTransaction()
    })

    expect(captured).toEqual({ id: "tx-1" })
  })

  it("stores transaction in the active processing-state resources", async () => {
    const { txManager } = createRecordingTxManager()
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    let captured: unknown
    await txRunner(emptyMetadata(), async () => {
      captured = getResource(TRANSACTION_KEY)
    })

    expect(captured).toEqual({ id: "tx-1" })
  })

  it("transaction spans handler and PREPARE_COMMIT hooks", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    await txRunner(emptyMetadata(), async () => {
      log.push("handler")
      onPrepareCommit(() => { log.push("prepare-commit") })
    })

    expect(log).toEqual(["begin:tx-1", "handler", "prepare-commit", "commit:tx-1"])
  })

  it("each runner invocation gets its own transaction", async () => {
    const { txManager, log } = createRecordingTxManager()
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)

    await txRunner(emptyMetadata(), async () => { log.push("first") })
    await txRunner(emptyMetadata(), async () => { log.push("second") })

    expect(log).toEqual([
      "begin:tx-1", "first", "commit:tx-1",
      "begin:tx-2", "second", "commit:tx-2",
    ])
  })
})
