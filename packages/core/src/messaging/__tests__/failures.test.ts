import { describe, expect, test } from "bun:test"
import { AppendConditionError, isConflict } from "../failures.js"

describe("isConflict", () => {
  test("recognises core's AppendConditionError and its subclasses", () => {
    class StoreConflict extends AppendConditionError {
      readonly position = 42n
    }
    expect(isConflict(new AppendConditionError("lost the race"))).toBe(true)
    expect(isConflict(new StoreConflict("lost the race"))).toBe(true)
  })

  test("recognises a conflict built by another copy of core", () => {
    // What a second installed copy of core throws: same shape, a different class.
    class AppendConditionError extends Error {
      readonly kind = "conflict"
    }
    const foreign = new AppendConditionError("lost the race")
    expect(foreign instanceof Error).toBe(true)
    expect(isConflict(foreign)).toBe(true)
  })

  test("refuses everything else", () => {
    expect(isConflict(new Error("append condition violated"))).toBe(false)
    expect(isConflict(Object.assign(new Error("x"), { code: "KR001" }))).toBe(false)
    expect(isConflict(undefined)).toBe(false)
    expect(isConflict(null)).toBe(false)
    expect(isConflict("conflict")).toBe(false)
  })

  test("does not look through a cause", () => {
    expect(isConflict(new Error("order closed", { cause: new AppendConditionError("lost the race") }))).toBe(false)
  })

  test("keeps the name and the cause", () => {
    const cause = new Error("driver")
    const error = new AppendConditionError("lost the race", { cause })
    expect(error.name).toBe("AppendConditionError")
    expect(error.cause).toBe(cause)
    expect(error.kind).toBe("conflict")
  })
})
