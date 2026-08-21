/**
 * The fitness test, on its own. It is a pure function of two values, so it is
 * judged as one — no store, no log, no fold.
 */
import { describe, expect, it } from "bun:test"
import { matchesInitialStructure } from "../structural-fitness.js"

describe("matchesInitialStructure — the hazard it exists for", () => {
  it("catches a field the specimen has and the candidate lacks", () => {
    // The real failure mode: new code reads `capacity`, old entries have none,
    // and the fold silently computes on `undefined` from there on.
    expect(matchesInitialStructure({ name: "", capacity: 0 }, { name: "CS" })).toBe(false)
  })

  it("catches a type that changed under a key", () => {
    expect(matchesInitialStructure({ capacity: 0 }, { capacity: "30" })).toBe(false)
  })

  it("TOLERATES an extra key — a removed field's leftovers are harmless", () => {
    expect(matchesInitialStructure({ name: "" }, { name: "CS", retired: true })).toBe(true)
  })

  it("accepts a candidate of exactly the specimen's shape", () => {
    expect(matchesInitialStructure({ name: "", capacity: 0 }, { name: "CS", capacity: 30 })).toBe(true)
  })
})

describe("matchesInitialStructure — recursion", () => {
  it("recurses into nested objects", () => {
    const specimen = { meta: { owner: "", limits: { max: 0 } } }
    expect(matchesInitialStructure(specimen, { meta: { owner: "a", limits: { max: 1 } } })).toBe(true)
    expect(matchesInitialStructure(specimen, { meta: { owner: "a", limits: {} } })).toBe(false)
    expect(matchesInitialStructure(specimen, { meta: { owner: "a" } })).toBe(false)
  })

  it("rejects a nested object that came back a primitive", () => {
    expect(matchesInitialStructure({ meta: { owner: "" } }, { meta: "owner" })).toBe(false)
  })

  it("rejects null where the specimen has an object", () => {
    expect(matchesInitialStructure({ meta: { owner: "" } }, { meta: null })).toBe(false)
  })
})

describe("matchesInitialStructure — arrays", () => {
  it("checks every element against the specimen array's FIRST element", () => {
    const specimen = { enrolled: [{ studentId: "", credits: 0 }] }
    expect(matchesInitialStructure(specimen, { enrolled: [{ studentId: "s", credits: 3 }] })).toBe(true)
    expect(matchesInitialStructure(specimen, { enrolled: [{ studentId: "s" }] })).toBe(false)
  })

  it("catches ONE bad element among good ones", () => {
    const specimen = { enrolled: [{ studentId: "" }] }
    const candidate = { enrolled: [{ studentId: "a" }, { studentId: 7 }] }
    expect(matchesInitialStructure(specimen, candidate)).toBe(false)
  })

  it("an EMPTY specimen array teaches nothing, so anything array-shaped passes", () => {
    expect(matchesInitialStructure({ enrolled: [] }, { enrolled: [{ anything: 1 }] })).toBe(true)
    expect(matchesInitialStructure({ enrolled: [] }, { enrolled: [] })).toBe(true)
  })

  it("still demands an ARRAY, even when it teaches nothing about elements", () => {
    expect(matchesInitialStructure({ enrolled: [] }, { enrolled: {} })).toBe(false)
    expect(matchesInitialStructure({ enrolled: [] }, { enrolled: 0 })).toBe(false)
  })

  it("rejects an object where the specimen has an array", () => {
    expect(matchesInitialStructure([1], { 0: 1 })).toBe(false)
  })
})

describe("matchesInitialStructure — what a specimen declines to say", () => {
  it("a null leaf declares no type, so anything satisfies it", () => {
    expect(matchesInitialStructure({ closedAt: null }, { closedAt: 1717171717 })).toBe(true)
    expect(matchesInitialStructure({ closedAt: null }, { closedAt: null })).toBe(true)
  })

  it("an undefined leaf does not even demand the key — it cannot survive JSON", () => {
    expect(matchesInitialStructure({ closedAt: undefined }, {})).toBe(true)
  })

  it("but a null leaf DOES demand the key, because null survives JSON", () => {
    expect(matchesInitialStructure({ closedAt: null }, {})).toBe(false)
  })
})

describe("matchesInitialStructure — the limits, stated out loud", () => {
  it("CANNOT see a semantic change that keeps the structure", () => {
    // Cents became dollars. `number` before, `number` after. A hand-written
    // schema would have missed it for exactly the same reason.
    expect(matchesInitialStructure({ balance: 0 }, { balance: 1999 })).toBe(true)
  })

  it("does see a primitive KIND change, which is the drift worth catching", () => {
    expect(matchesInitialStructure({ id: 0n }, { id: "1" })).toBe(false)
    expect(matchesInitialStructure({ ok: false }, { ok: "false" })).toBe(false)
  })

  it("never throws — 'not fit' is an answer, not an error", () => {
    expect(matchesInitialStructure({ a: 1 }, null)).toBe(false)
    expect(matchesInitialStructure({ a: 1 }, undefined)).toBe(false)
    expect(matchesInitialStructure(undefined, undefined)).toBe(true)
  })
})
