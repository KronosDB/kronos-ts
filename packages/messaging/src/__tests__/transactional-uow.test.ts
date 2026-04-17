import { describe, expect, it } from "bun:test"
import { resourceKey } from "@kronos-ts/common"
import { createUnitOfWork, defaultUnitOfWorkFactory } from "../unit-of-work.js"
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

describe("TransactionalUnitOfWorkFactory", () => {
  it("begins transaction before handler and commits after", async () => {
    const { txManager, log } = createRecordingTxManager()
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    const uow = factory()
    await uow.executeWithResult(async () => {
      log.push("handler")
    })

    expect(log).toEqual(["begin:tx-1", "handler", "commit:tx-1"])
  })

  it("rolls back on handler failure", async () => {
    const { txManager, log } = createRecordingTxManager()
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    const uow = factory()
    await expect(
      uow.executeWithResult(async () => {
        log.push("handler")
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    expect(log).toEqual(["begin:tx-1", "handler", "rollback:tx-1"])
  })

  it("makes transaction available via getActiveTransaction", async () => {
    const { txManager } = createRecordingTxManager()
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    let captured: unknown

    const uow = factory()
    await uow.executeWithResult(async () => {
      captured = getActiveTransaction()
    })

    expect(captured).toEqual({ id: "tx-1" })
  })

  it("stores transaction in ProcessingContext resources", async () => {
    const { txManager } = createRecordingTxManager()
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    let captured: unknown

    const uow = factory()
    await uow.executeWithResult(async (ctx) => {
      captured = ctx.get(TRANSACTION_KEY)
    })

    expect(captured).toEqual({ id: "tx-1" })
  })

  it("transaction spans handler and PREPARE_COMMIT hooks", async () => {
    const { txManager, log } = createRecordingTxManager()
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    const uow = factory()
    await uow.executeWithResult(async (ctx) => {
      log.push("handler")
      ctx.onPrepareCommit(() => { log.push("prepare-commit") })
    })

    // prepare-commit runs between handler and commit
    expect(log).toEqual(["begin:tx-1", "handler", "prepare-commit", "commit:tx-1"])
  })

  it("each UnitOfWork gets its own transaction", async () => {
    const { txManager, log } = createRecordingTxManager()
    const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)

    await factory().executeWithResult(async () => { log.push("first") })
    await factory().executeWithResult(async () => { log.push("second") })

    expect(log).toEqual([
      "begin:tx-1", "first", "commit:tx-1",
      "begin:tx-2", "second", "commit:tx-2",
    ])
  })
})
