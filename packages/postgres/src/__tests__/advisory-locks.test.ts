import { describe, it, expect } from "bun:test"
import {
  hashLockKey,
  leafKey,
  typeIntentKey,
  globalIntentKey,
  acquireWriteLocks,
  acquireReadLocks,
} from "../advisory-locks.js"
import type { PostgresAdapterTransaction } from "../adapter.js"

function makeMockTx(): { tx: PostgresAdapterTransaction; calls: Array<{ sql: string; params?: unknown[] }> } {
  const calls: Array<{ sql: string; params?: unknown[] }> = []
  return {
    calls,
    tx: {
      unwrap<T = unknown>(): T {
        return undefined as unknown as T
      },
      async query(sql: string, params?: unknown[]) {
        calls.push({ sql, params })
        return []
      },
    },
  }
}

describe("hashLockKey (FNV-1a 64-bit)", () => {
  it("returns offset basis for the empty string", () => {
    // FNV-1a 64-bit offset basis: 0xcbf29ce484222325
    // Signed BIGINT interpretation is negative (high bit set):
    // 0xcbf29ce484222325 - 2^64 = -3750763034362895579n
    expect(hashLockKey("")).toBe(-3750763034362895579n)
  })

  it("returns the canonical vector for \"a\"", () => {
    // 0xaf63dc4c8601ec8c - 2^64 = -5808556873153909620n (high bit set → negative)
    expect(hashLockKey("a")).toBe(-5808556873153909620n)
  })

  it("returns the canonical vector for \"foobar\"", () => {
    // 0x85944171f73967e8 - 2^64 = -8821353812377114648n
    // (plan spec had a computation error; verified: 0x85944171f73967e8 - 2^64 = -8821353812377114648)
    expect(hashLockKey("foobar")).toBe(-8821353812377114648n)
  })

  it("returns values inside the signed 64-bit BIGINT range", () => {
    const MIN = -(1n << 63n)
    const MAX = (1n << 63n) - 1n
    for (const input of ["", "a", "abc", "hello world", "Order:placed", "x".repeat(1024)]) {
      const v = hashLockKey(input)
      expect(v).toBeGreaterThanOrEqual(MIN)
      expect(v).toBeLessThanOrEqual(MAX)
    }
  })
})

describe("leafKey / typeIntentKey / globalIntentKey", () => {
  it("leafKey discriminates by event type", () => {
    const a = leafKey("OrderPlaced", "order:123")
    const b = leafKey("OrderShipped", "order:123")
    expect(a).not.toBe(b)
  })

  it("leafKey discriminates by tag", () => {
    const a = leafKey("OrderPlaced", "order:123")
    const b = leafKey("OrderPlaced", "order:456")
    expect(a).not.toBe(b)
  })

  it("leafKey and typeIntentKey produce different keys for the same type (keyspace separation)", () => {
    const leaf = leafKey("OrderPlaced", "order:123")
    const intent = typeIntentKey("OrderPlaced")
    expect(leaf).not.toBe(intent)
  })

  it("globalIntentKey is constant", () => {
    expect(globalIntentKey()).toBe(globalIntentKey())
  })

  it("globalIntentKey differs from any typeIntentKey", () => {
    const g = globalIntentKey()
    const t = typeIntentKey("AnyType")
    expect(g).not.toBe(t)
  })

  it("U+001F delimiter prevents tuple collision: leafKey(\"Order\", \"Placed:x\") != leafKey(\"OrderPlaced\", \"x\")", () => {
    // Without a delimiter, the serialisations would both be "OrderPlaced:x".
    // With U+001F, they differ → distinct keys.
    expect(leafKey("Order", "Placed:x")).not.toBe(leafKey("OrderPlaced", "x"))
  })

  it("uses ASCII Unit Separator (U+001F) — empty separator would cause hash collisions across boundary-shifted inputs", () => {
    // Regression: with UNIT_SEPARATOR = "" the pairs ("ab","c") and ("a","bc")
    // both serialise to "Lab c" / "La bc" — identical key strings → identical
    // hashes → false leaf-key collisions → real DCB races on disjoint entities.
    expect(leafKey("ab", "c")).not.toBe(leafKey("a", "bc"))
    expect(typeIntentKey("ab")).not.toBe(typeIntentKey("a"))
  })
})

describe("acquireWriteLocks", () => {
  it("takes X on leaf, S on type-intent, S on global-intent (writer pattern)", async () => {
    const m = makeMockTx()
    await acquireWriteLocks(m.tx, [{ type: "OrderPlaced", tag: "order:123" }])
    const sqls = m.calls.map((c) => c.sql)
    // X-lock for the leaf
    expect(sqls.some((s) => s.includes("pg_advisory_xact_lock(") && !s.includes("shared"))).toBe(true)
    // Shared-lock for type intent + global intent
    expect(sqls.filter((s) => s.includes("pg_advisory_xact_lock_shared(")).length).toBeGreaterThanOrEqual(2)
  })

  it("acquires one leaf lock per (type, tag) pair", async () => {
    const m = makeMockTx()
    await acquireWriteLocks(m.tx, [
      { type: "OrderPlaced", tag: "order:1" },
      { type: "OrderPlaced", tag: "order:2" },
      { type: "OrderShipped", tag: "order:1" },
    ])
    const xLocks = m.calls.filter((c) => c.sql.includes("pg_advisory_xact_lock(") && !c.sql.includes("shared"))
    // 3 unique leaves
    expect(xLocks.length).toBe(3)
  })
})

describe("acquireReadLocks", () => {
  it("takes S on leaf, X on type-intent, X on global-intent (reader pattern — Query.all)", async () => {
    const m = makeMockTx()
    await acquireReadLocks(m.tx, [{ type: "OrderPlaced", tag: "order:123" }])
    const sqls = m.calls.map((c) => c.sql)
    // S on leaf
    expect(sqls.some((s) => s.includes("pg_advisory_xact_lock_shared("))).toBe(true)
    // X on intents (asymmetric inverse of write)
    expect(sqls.filter((s) => s.includes("pg_advisory_xact_lock(") && !s.includes("shared")).length).toBeGreaterThanOrEqual(2)
  })
})
