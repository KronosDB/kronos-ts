import { describe, expect, it } from "bun:test"
import { AppendConditionError, isUnavailable, UnavailableError } from "@kronos-ts/core"
import { handlingFailureCode, KronosDbErrorCode, mapErrorCode, requestError } from "../errors.js"
import { ErrorCode } from "../protocol/kronosdb-protocol.js"
import { ShutdownInProgressError } from "../shutdown-latch.js"

describe("KronosDB unavailable", () => {
  it("gives the codes that say to retry later core's unavailable kind, and execution errors none", () => {
    for (const code of [
      KronosDbErrorCode.CONNECTION_FAILED,
      KronosDbErrorCode.NO_EVENT_STORE_LEADER,
      KronosDbErrorCode.COMMAND_DISPATCH_ERROR,
      KronosDbErrorCode.QUERY_DISPATCH_ERROR,
      KronosDbErrorCode.COMMAND_TIMEOUT,
      KronosDbErrorCode.CONNECTION_TO_HANDLER_LOST,
    ]) {
      expect(isUnavailable(mapErrorCode(code, "m"))).toBe(true)
    }
    expect(isUnavailable(mapErrorCode(KronosDbErrorCode.COMMAND_EXECUTION_ERROR, "m"))).toBe(false)
    expect(isUnavailable(mapErrorCode(KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED, "m"))).toBe(false)
  })

  it("gives the native unavailable status the kind, and corruption or bad credentials none", () => {
    expect(isUnavailable(requestError(ErrorCode.unavailable, "failing over"))).toBe(true)
    expect(isUnavailable(requestError(ErrorCode.corrupted, "bad bytes"))).toBe(false)
    expect(isUnavailable(requestError(ErrorCode.unauthenticated, "who"))).toBe(false)
  })

  it("answers a handler's unavailable failure with CONNECTION_FAILED, and a conflict with its own code", () => {
    const fallback = KronosDbErrorCode.COMMAND_EXECUTION_ERROR
    expect(handlingFailureCode(new UnavailableError("store down"), fallback)).toBe(KronosDbErrorCode.CONNECTION_FAILED)
    expect(handlingFailureCode(new AppendConditionError("lost"), fallback)).toBe(KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED)
  })

  it("refuses work during shutdown as unavailable", () => {
    expect(isUnavailable(new ShutdownInProgressError())).toBe(true)
  })
})
