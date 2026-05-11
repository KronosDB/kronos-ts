import { describe, it, expect } from "bun:test"
import { buildCriteriaWhere } from "../criteria-sql.js"

describe("buildCriteriaWhere", () => {
  it("any-tag: produces cardinality(tags) > 0 with no params", () => {
    const result = buildCriteriaWhere({ kind: "any-tag" }, 1)
    expect(result.where).toBe("cardinality(tags) > 0")
    expect(result.params).toEqual([])
    expect(result.nextParamIndex).toBe(1)
  })

  it("tags (non-empty): emits @> with U+001F-encoded key:value strings", () => {
    const result = buildCriteriaWhere(
      { kind: "tags", tags: [{ key: "order", value: "123" }, { key: "customer", value: "abc" }] },
      1,
    )
    expect(result.where).toBe("tags @> $1::text[]")
    // U+001F (unit separator) used as delimiter between key and value
    expect(result.params).toEqual([["order123", "customerabc"]])
    expect(result.nextParamIndex).toBe(2)
  })

  it("tags (empty): trivially-true predicate", () => {
    const result = buildCriteriaWhere({ kind: "tags", tags: [] }, 1)
    expect(result.where).toBe("true")
    expect(result.params).toEqual([])
  })

  it("NEGATIVE: never emits overlap operator && for tags (must be @> contains-all)", () => {
    const result = buildCriteriaWhere(
      { kind: "tags", tags: [{ key: "k", value: "v" }] },
      1,
    )
    expect(result.where).not.toContain("&&")
    expect(result.where).toContain("@>")
  })

  it("type-restricted: AND-combines inner predicate with type = ANY()", () => {
    const result = buildCriteriaWhere(
      {
        kind: "type-restricted",
        types: ["OrderPlaced", "OrderShipped"],
        inner: { kind: "any-tag" },
      },
      1,
    )
    expect(result.where).toBe("(cardinality(tags) > 0) AND type = ANY($1::text[])")
    expect(result.params).toEqual([["OrderPlaced", "OrderShipped"]])
    expect(result.nextParamIndex).toBe(2)
  })

  it("either: OR-combines branches and renumbers params monotonically across branches", () => {
    const result = buildCriteriaWhere(
      {
        kind: "either",
        criteria: [
          { kind: "tags", tags: [{ key: "a", value: "1" }] },
          { kind: "type-restricted", types: ["T"], inner: { kind: "any-tag" } },
        ],
      },
      1,
    )
    // First branch uses $1, second branch's renumbered param starts at $2
    expect(result.where).toBe("(tags @> $1::text[]) OR ((cardinality(tags) > 0) AND type = ANY($2::text[]))")
    expect(result.params).toEqual([["a1"], ["T"]])
    expect(result.nextParamIndex).toBe(3)
  })

  it("starts numbering at the provided offset (so caller can chain in a larger query)", () => {
    const result = buildCriteriaWhere({ kind: "tags", tags: [{ key: "k", value: "v" }] }, 5)
    expect(result.where).toBe("tags @> $5::text[]")
    expect(result.nextParamIndex).toBe(6)
  })
})
