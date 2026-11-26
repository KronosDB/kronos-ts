import { describe, it, expect } from "bun:test"
import {
  hashLockKey,
  tagKey,
  typeKey,
  acquireLocks,
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

describe("tagKey and typeKey", () => {
  it("give different tags different locks", () => {
    expect(tagKey("order:123")).not.toBe(tagKey("order:456"))
  })

  it("give different types different locks", () => {
    expect(typeKey("shop.OrderPlaced")).not.toBe(typeKey("shop.OrderShipped"))
  })

  it("give a tag and a type with the same text different locks", () => {
    expect(tagKey("shop.OrderPlaced")).not.toBe(typeKey("shop.OrderPlaced"))
  })

  it("give one tag the same lock every time", () => {
    expect(tagKey("order:123")).toBe(tagKey("order:123"))
    // The lock an append has always taken for this tag.
    expect(tagKey("order:123")).toBe(hashLockKey("L\u001f\u001forder:123"))
  })
})

describe("acquireLocks", () => {
  it("takes each lock exclusively, once, in ascending order of key", async () => {
    const m = makeMockTx()
    const keys = [typeKey("shop.OrderPlaced"), tagKey("order:2"), tagKey("order:1"), tagKey("order:2")]
    await acquireLocks(m.tx, keys)

    expect(m.calls.every((c) => c.sql === "SELECT pg_advisory_xact_lock($1)")).toBe(true)
    const taken = m.calls.map((c) => c.params![0] as bigint)
    expect(taken).toHaveLength(3)
    expect(new Set(taken)).toEqual(new Set(keys))
    expect(taken.every((key, i) => i === 0 || taken[i - 1]! < key)).toBe(true)
  })

  it("takes nothing when there is nothing to lock", async () => {
    const m = makeMockTx()
    await acquireLocks(m.tx, [])
    expect(m.calls).toHaveLength(0)
  })
})
