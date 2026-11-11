import { describe, expect, it } from "bun:test"
import { AppendConditionError, isConflict, isNoHandler, NoHandlerError } from "@kronos-ts/core"
import { handlingFailureCode, KronosDbErrorCode, mapErrorCode, NoHandlerForCommandError, requestError } from "../errors.js"
import { ErrorCode } from "../protocol/kronosdb-protocol.js"

describe("KronosDB failure codes", () => {
  it("rebuilds the server's no-handler answers with core's no-handler kind", () => {
    expect(isNoHandler(mapErrorCode(KronosDbErrorCode.NO_HANDLER_FOR_COMMAND, "none"))).toBe(true)
    expect(isNoHandler(mapErrorCode(KronosDbErrorCode.NO_HANDLER_FOR_QUERY, "none"))).toBe(true)
    expect(isNoHandler(mapErrorCode(KronosDbErrorCode.COMMAND_EXECUTION_ERROR, "failed"))).toBe(false)
  })

  it("rebuilds the server's no_handler refusal with core's no-handler kind, and no other refusal", () => {
    const refusal = requestError(ErrorCode.no_handler, "no handler available for command 'x'")
    expect(isNoHandler(refusal)).toBe(true)
    expect(refusal.transient).toBe(false)
    expect(isNoHandler(requestError(ErrorCode.unavailable, "failing over"))).toBe(false)
    expect(isNoHandler(requestError(99, "unknown"))).toBe(false)
  })

  it("rebuilds a conflict code as a conflict", () => {
    expect(isConflict(mapErrorCode(KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED, "lost"))).toBe(true)
  })

  it("answers a handler's conflict with its code, and anything else, a nested no-handler too, with the fallback", () => {
    const fallback = KronosDbErrorCode.COMMAND_EXECUTION_ERROR
    expect(handlingFailureCode(new AppendConditionError("lost"), fallback)).toBe(KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED)
    expect(handlingFailureCode(new NoHandlerError("nested"), fallback)).toBe(fallback)
    expect(handlingFailureCode(new NoHandlerForCommandError("nested"), fallback)).toBe(fallback)
    expect(handlingFailureCode(new Error("failed"), fallback)).toBe(fallback)
  })
})
