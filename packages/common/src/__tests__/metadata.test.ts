import { describe, expect, it } from "bun:test"
import {
  emptyMetadata,
  metadataWith,
  mergeMetadata,
  metadataAnd,
  metadataAndIfNotPresent,
  metadataWithoutKeys,
  metadataSubset,
  metadataContains,
} from "../metadata.js"
import { tag } from "../tag.js"

describe("Metadata", () => {
  describe("emptyMetadata", () => {
    it("creates an empty object", () => {
      const meta = emptyMetadata()

      expect(meta).toEqual({})
      expect(Object.keys(meta)).toHaveLength(0)
    })
  })

  describe("metadataWith", () => {
    it("creates metadata with a single entry", () => {
      const meta = metadataWith("correlationId", "abc-123")

      expect(meta).toEqual({ correlationId: "abc-123" })
    })

    it("supports any value type", () => {
      expect(metadataWith("count", 42)).toEqual({ count: 42 })
      expect(metadataWith("active", true)).toEqual({ active: true })
      expect(metadataWith("data", null)).toEqual({ data: null })
    })
  })

  describe("mergeMetadata", () => {
    it("merges two metadata objects with override precedence", () => {
      const base = { correlationId: "abc", traceId: "123" }
      const override = { correlationId: "def", extra: "value" }

      const result = mergeMetadata(base, override)

      expect(result).toEqual({
        correlationId: "def",
        traceId: "123",
        extra: "value",
      })
    })

    it("returns base keys when override is empty", () => {
      const base = { key: "value" }

      expect(mergeMetadata(base, {})).toEqual({ key: "value" })
    })

    it("returns override keys when base is empty", () => {
      const override = { key: "value" }

      expect(mergeMetadata({}, override)).toEqual({ key: "value" })
    })

    it("does not mutate original objects", () => {
      const base = { a: 1 }
      const override = { b: 2 }

      mergeMetadata(base, override)

      expect(base).toEqual({ a: 1 })
      expect(override).toEqual({ b: 2 })
    })
  })

  describe("metadataAnd", () => {
    it("adds a new entry to existing metadata", () => {
      const meta = metadataWith("a", 1)

      const result = metadataAnd(meta, "b", 2)

      expect(result).toEqual({ a: 1, b: 2 })
    })

    it("replaces an existing entry", () => {
      const meta = metadataWith("key", "old")

      const result = metadataAnd(meta, "key", "new")

      expect(result).toEqual({ key: "new" })
    })

    it("does not mutate the original metadata", () => {
      const meta = metadataWith("a", 1)

      metadataAnd(meta, "b", 2)

      expect(meta).toEqual({ a: 1 })
    })
  })

  describe("metadataAndIfNotPresent", () => {
    it("adds entry when key does not exist", () => {
      const meta = metadataWith("a", 1)

      const result = metadataAndIfNotPresent(meta, "b", () => 2)

      expect(result).toEqual({ a: 1, b: 2 })
    })

    it("does not overwrite when key already exists", () => {
      const meta = metadataWith("a", "original")

      const result = metadataAndIfNotPresent(meta, "a", () => "replacement")

      expect(result).toEqual({ a: "original" })
    })

    it("supplier is NOT called when key exists", () => {
      const meta = metadataWith("a", 1)
      let supplierCalled = false

      metadataAndIfNotPresent(meta, "a", () => {
        supplierCalled = true
        return 99
      })

      expect(supplierCalled).toBe(false)
    })

    it("returns the same reference when key exists", () => {
      const meta = metadataWith("a", 1)

      const result = metadataAndIfNotPresent(meta, "a", () => 2)

      expect(result).toBe(meta) // same reference, not a copy
    })
  })

  describe("metadataWithoutKeys", () => {
    it("removes specified keys", () => {
      const meta = { a: 1, b: 2, c: 3 }

      const result = metadataWithoutKeys(meta, "a", "c")

      expect(result).toEqual({ b: 2 })
    })

    it("ignores keys that do not exist", () => {
      const meta = { a: 1 }

      const result = metadataWithoutKeys(meta, "nonexistent")

      expect(result).toEqual({ a: 1 })
    })

    it("does not mutate the original", () => {
      const meta = { a: 1, b: 2 }

      metadataWithoutKeys(meta, "a")

      expect(meta).toEqual({ a: 1, b: 2 })
    })

    it("returns empty object when all keys removed", () => {
      const meta = { a: 1 }

      expect(metadataWithoutKeys(meta, "a")).toEqual({})
    })
  })

  describe("metadataSubset", () => {
    it("returns only the specified keys", () => {
      const meta = { a: 1, b: 2, c: 3 }

      const result = metadataSubset(meta, "a", "c")

      expect(result).toEqual({ a: 1, c: 3 })
    })

    it("ignores keys that do not exist in source", () => {
      const meta = { a: 1 }

      const result = metadataSubset(meta, "a", "missing")

      expect(result).toEqual({ a: 1 })
    })

    it("returns empty object when no keys match", () => {
      const meta = { a: 1 }

      expect(metadataSubset(meta, "nonexistent")).toEqual({})
    })
  })

  describe("metadataContains", () => {
    it("returns true when key exists", () => {
      const meta = metadataWith("traceId", "abc")

      expect(metadataContains(meta, "traceId")).toBe(true)
    })

    it("returns false when key does not exist", () => {
      const meta = emptyMetadata()

      expect(metadataContains(meta, "traceId")).toBe(false)
    })

    it("returns true even when value is null or undefined", () => {
      const meta = metadataWith("key", null)

      expect(metadataContains(meta, "key")).toBe(true)
    })
  })
})

describe("Tag", () => {
  it("creates a tag with key and value", () => {
    const t = tag("courseId", "cs-101")

    expect(t.key).toBe("courseId")
    expect(t.value).toBe("cs-101")
  })

  it("throws when key is empty", () => {
    expect(() => tag("", "value")).toThrow("Tag key must not be empty")
  })

  it("throws when value is empty", () => {
    expect(() => tag("key", "")).toThrow("Tag value must not be empty")
  })
})
