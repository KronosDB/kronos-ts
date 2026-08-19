import { describe, it, expect } from "bun:test"
import type { PostgresAdapterTransaction } from "../adapter.js"
import { applySessionTimeouts, resolveSessionTimeouts } from "../session-timeouts.js"

/** Records the SQL issued so we can assert exactly which SET LOCALs are armed. */
function recordingTx() {
  const queries: string[] = []
  const tx: PostgresAdapterTransaction = {
    unwrap<T = unknown>(): T {
      return undefined as unknown as T
    },
    async query(sql: string) {
      queries.push(sql)
      return []
    },
  }
  return { tx, queries }
}

describe("resolveSessionTimeouts", () => {
  it("defaults idle-in-transaction to 30s and statement timeout off", () => {
    expect(resolveSessionTimeouts()).toEqual({
      idleInTransactionTimeoutMs: 30_000,
      statementTimeoutMs: 0,
    })
  })

  it("honors explicit values", () => {
    expect(resolveSessionTimeouts({ idleInTransactionTimeoutMs: 10_000, statementTimeoutMs: 5_000 })).toEqual({
      idleInTransactionTimeoutMs: 10_000,
      statementTimeoutMs: 5_000,
    })
  })

  it("normalizes non-finite / negative timeouts to 0 (disabled)", () => {
    expect(resolveSessionTimeouts({ idleInTransactionTimeoutMs: -1, statementTimeoutMs: NaN })).toEqual({
      idleInTransactionTimeoutMs: 0,
      statementTimeoutMs: 0,
    })
  })

  it("floors fractional milliseconds", () => {
    expect(resolveSessionTimeouts({ idleInTransactionTimeoutMs: 1234.9 }).idleInTransactionTimeoutMs).toBe(1234)
  })
})

describe("applySessionTimeouts", () => {
  it("arms idle-in-transaction by default (30s) and no statement timeout", async () => {
    const { tx, queries } = recordingTx()
    await applySessionTimeouts(tx, resolveSessionTimeouts())
    expect(queries).toEqual(["SET LOCAL idle_in_transaction_session_timeout = 30000"])
  })

  it("arms both timeouts when configured", async () => {
    const { tx, queries } = recordingTx()
    await applySessionTimeouts(tx, resolveSessionTimeouts({ idleInTransactionTimeoutMs: 10_000, statementTimeoutMs: 5_000 }))
    expect(queries).toEqual([
      "SET LOCAL idle_in_transaction_session_timeout = 10000",
      "SET LOCAL statement_timeout = 5000",
    ])
  })

  it("emits no SET LOCAL when both timeouts are disabled", async () => {
    const { tx, queries } = recordingTx()
    await applySessionTimeouts(tx, resolveSessionTimeouts({ idleInTransactionTimeoutMs: 0, statementTimeoutMs: 0 }))
    expect(queries).toEqual([])
  })
})
