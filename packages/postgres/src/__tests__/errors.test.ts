import { describe, it, expect } from "bun:test"
import { AppendConditionError as CoreAppendConditionError, isConflict, isUnavailable } from "@kronos-ts/core"
import {
  AppendConditionError,
  KRONOS_DCB_VIOLATION_SQLSTATE,
  isDcbViolation,
  markUnavailable,
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

describe("the conflict contract", () => {
  it("is core's conflict, by class and by guard", () => {
    const e = AppendConditionError.fromConflictCount(1, 7n)
    expect(e).toBeInstanceOf(CoreAppendConditionError)
    expect(isConflict(e)).toBe(true)
  })
})

describe("markUnavailable", () => {
  const coded = (code: string, message = "driver") => Object.assign(new Error(message), { code })

  it("marks lost or refused connections, from the socket layer and each driver", () => {
    for (const code of ["ECONNREFUSED", "ECONNRESET", "CONNECTION_CLOSED", "CONNECT_TIMEOUT", "ERR_POSTGRES_CONNECTION_CLOSED", "ERR_POSTGRES_CONNECTION_REFUSED"]) {
      expect(isUnavailable(markUnavailable(coded(code)))).toBe(true)
    }
    expect(isUnavailable(markUnavailable(new Error("timeout exceeded when trying to connect")))).toBe(true)
    expect(isUnavailable(markUnavailable(new Error("Connection terminated unexpectedly")))).toBe(true)
    expect(isUnavailable(markUnavailable(new AggregateError([coded("ECONNREFUSED")], "connect")))).toBe(true)
  })

  it("marks the SQLSTATEs of a server that cannot serve now, connection exceptions as a class", () => {
    for (const code of ["57P01", "57P03", "53300", "57014", "55P03", "25P03", "08006", "08001"]) {
      expect(isUnavailable(markUnavailable(coded(code)))).toBe(true)
    }
  })

  it("leaves everything else alone: bad credentials, schema errors, conflicts", () => {
    for (const code of ["28P01", "3D000", "42P01", "23505", "KR001", "ENOTFOUND"]) {
      expect(isUnavailable(markUnavailable(coded(code)))).toBe(false)
    }
    // A SQLSTATE-bearing error is never judged by its text.
    expect(isUnavailable(markUnavailable(coded("42601", "Connection terminated in a comment")))).toBe(false)
    expect(isUnavailable(markUnavailable(new Error("syntax error")))).toBe(false)
    const conflict = AppendConditionError.fromConflictCount(1, 1n)
    expect(markUnavailable(conflict)).toBe(conflict)
    expect(isConflict(conflict)).toBe(true)
  })

  it("marks in place, keeping the SQLSTATE on .code", () => {
    const error = coded("57P01")
    expect(markUnavailable(error)).toBe(error)
    expect(error.code).toBe("57P01")
  })
})
