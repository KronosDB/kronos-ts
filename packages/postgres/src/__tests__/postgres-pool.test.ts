import { describe, expect, it } from "bun:test"
import type { PostgresAdapter } from "../adapter.js"
import { postgresPool, type PostgresAdapterPoolOptions } from "../postgres-pool.js"

const adapter = {} as PostgresAdapter

/** What a caller the compiler did not check can still pass. */
const unchecked = (options: object) => options as PostgresAdapterPoolOptions

describe("postgresPool over an adapter", () => {
  it("refuses idleInTransactionTimeoutMs, naming where it goes", () => {
    expect(() => postgresPool(adapter, unchecked({ idleInTransactionTimeoutMs: 5_000 }))).toThrow(
      /idleInTransactionTimeoutMs cannot be set on a pool built from an adapter\. Set it on the adapter/,
    )
  })

  it("refuses statementTimeoutMs, and zero is a setting too", () => {
    expect(() => postgresPool(adapter, unchecked({ statementTimeoutMs: 0 }))).toThrow(
      /statementTimeoutMs cannot be set on a pool built from an adapter/,
    )
  })

  it("names both when both are given", () => {
    expect(() =>
      postgresPool(adapter, unchecked({ idleInTransactionTimeoutMs: 5_000, statementTimeoutMs: 1_000 })),
    ).toThrow(/idleInTransactionTimeoutMs and statementTimeoutMs cannot be set .* Set them on the adapter/)
  })

  it("takes the options that are the pool's own", () => {
    const pool = postgresPool(adapter, { bootstrap: false, resilience: { maxAttempts: 1 } })
    expect(pool.tables.events).toBe("kronos_events")
  })
})

describe("postgresPool over a connection string", () => {
  it("takes the session timeouts, which configure the adapter it loads", () => {
    expect(() =>
      postgresPool("postgresql://localhost/db", { idleInTransactionTimeoutMs: 5_000, statementTimeoutMs: 1_000 }),
    ).not.toThrow()
  })
})
