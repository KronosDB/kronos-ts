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

describe("postgresPool start()", () => {
  /** An adapter whose `connect` fails with each of `failures` in turn, then succeeds. */
  function connecting(failures: object[]) {
    let calls = 0
    const adapter = {
      async connect() {
        calls++
        const failure = failures[calls - 1]
        if (failure !== undefined) throw Object.assign(new Error("connect failed"), failure)
      },
    } as unknown as PostgresAdapter
    const quick = { initialDelayMs: 1, maxDelayMs: 1, log: () => {} }
    return { pool: postgresPool(adapter, { bootstrap: false, resilience: quick }), calls: () => calls }
  }

  it("connects again when the server could not be reached", async () => {
    const { pool, calls } = connecting([{ code: "ECONNREFUSED" }, { code: "57P03" }])
    await pool.start()
    expect(calls()).toBe(3)
  })

  it("fails at once on an error another attempt cannot fix", async () => {
    const { pool, calls } = connecting([{ code: "28P01" }, { code: "28P01" }])
    await expect(pool.start()).rejects.toThrow("connect failed")
    expect(calls()).toBe(1)
  })

  it("leaves the choice to the host's own isRetryable", async () => {
    let calls = 0
    const adapter = {
      async connect() {
        if (++calls < 3) throw Object.assign(new Error("connect failed"), { code: "28P01" })
      },
    } as unknown as PostgresAdapter
    const pool = postgresPool(adapter, {
      bootstrap: false,
      resilience: { initialDelayMs: 1, maxDelayMs: 1, log: () => {}, isRetryable: () => true },
    })
    await pool.start()
    expect(calls).toBe(3)
  })
})
