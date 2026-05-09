/**
 * Plan 09-01 Task 1 — transactionManager typed slot.
 *
 * Asserts: (a) `kronos()` resolves a default no-op TransactionManager and emits
 * the SLT-04 startup warning; (b) `app.forceSet('transactionManager', ...)`
 * overrides the default and suppresses the warning; (c) the default
 * `noTransactionManager()` round-trips begin/commit/rollback without error.
 */
import { describe, it, expect } from "bun:test"
import { kronos } from "../kronos.js"
import { noTransactionManager, type TransactionManager } from "@kronos-ts/messaging"

describe("transactionManager slot — Plan 09-01 (D-84)", () => {
  it("default no-op transactionManager is registered and emits the SLT-04 startup warning", async () => {
    const messages: string[] = []
    const app = kronos({
      logger: { warn: (msg: string) => messages.push(msg) },
    })
    const running = await app.start()
    try {
      expect(messages).toContain(
        "[kronos] transactionManager: in-memory — pass-through, configure a transactional extension for production",
      )
    } finally {
      await running.stop()
    }
  })

  it("app.forceSet('transactionManager', ...) overrides the default and suppresses the warning", async () => {
    const messages: string[] = []
    const myTm: TransactionManager<{ id: string }> = {
      begin: async () => ({ id: "tx-1" }),
      commit: async (_tx) => {},
      rollback: async (_tx) => {},
    }
    const app = kronos({
      logger: { warn: (msg: string) => messages.push(msg) },
    }).forceSet("transactionManager", () => myTm as TransactionManager)

    const running = await app.start()
    try {
      const txWarning = messages.find((m) => m.includes("transactionManager"))
      expect(txWarning).toBeUndefined()
    } finally {
      await running.stop()
    }
  })

  it("default noTransactionManager() begin/commit/rollback round-trip without throwing", async () => {
    const tm = noTransactionManager()
    const tx = await tm.begin()
    await tm.commit(tx)
    await tm.rollback(tx)
    expect(true).toBe(true)
  })
})
