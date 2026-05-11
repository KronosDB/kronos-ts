import { describe, it, expect } from "bun:test"
import {
  AppendConditionError,
  KRONOS_DCB_VIOLATION_SQLSTATE,
  isDcbViolation,
} from "../errors.js"

describe("AppendConditionError", () => {
  it("preserves name and message", () => {
    const e = new AppendConditionError("boom")
    expect(e.name).toBe("AppendConditionError")
    expect(e.message).toBe("boom")
  })

  it("is an Error", () => {
    expect(new AppendConditionError("x")).toBeInstanceOf(Error)
  })

  it("fromConflictCount produces a descriptive message", () => {
    const e = AppendConditionError.fromConflictCount(3, 42n)
    expect(e.message).toContain("3 conflicting")
    expect(e.message).toContain("position 42")
  })
})

describe("KRONOS_DCB_VIOLATION_SQLSTATE", () => {
  it("is KR001 (user-defined range, distinct from P0001 generic RAISE)", () => {
    expect(KRONOS_DCB_VIOLATION_SQLSTATE).toBe("KR001")
  })
})

describe("isDcbViolation", () => {
  it("returns true when error.code matches the SQLSTATE", () => {
    expect(isDcbViolation({ code: "KR001" })).toBe(true)
  })
  it("returns false for unrelated codes", () => {
    expect(isDcbViolation({ code: "23505" })).toBe(false) // unique-violation
    expect(isDcbViolation({ code: "P0001" })).toBe(false) // generic RAISE
    expect(isDcbViolation({})).toBe(false)
    expect(isDcbViolation(null)).toBe(false)
    expect(isDcbViolation(new Error("plain"))).toBe(false)
  })
})
