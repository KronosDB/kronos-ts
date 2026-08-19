import { describe, expect, it } from "bun:test"
import {
  shutdownLatch,
  ShutdownInProgressError,
} from "../shutdown-latch.js"
import {
  AxonServerError,
  AxonServerErrorCode,
  NoHandlerForCommandError,
  NoHandlerForQueryError,
  CommandExecutionError,
  QueryExecutionError,
  CommandDispatchError,
  QueryDispatchError,
  ConcurrencyError,
  ConnectionFailedError,
  AuthenticationError,
  mapErrorCode,
  isTransientError,
} from "../errors.js"
import {
  messageSizeValidator,
  MessageSizeExceededError,
} from "../message-size.js"

describe("ShutdownLatch", () => {
  it("starts with zero active count and not shutting down", () => {
    const latch = shutdownLatch()

    expect(latch.activeCount).toBe(0)
    expect(latch.shuttingDown).toBe(false)
  })

  it("tracks registered activities", () => {
    const latch = shutdownLatch()

    const h1 = latch.registerActivity()
    expect(latch.activeCount).toBe(1)

    const h2 = latch.registerActivity()
    expect(latch.activeCount).toBe(2)

    h1.end()
    expect(latch.activeCount).toBe(1)

    h2.end()
    expect(latch.activeCount).toBe(0)
  })

  it("end() is idempotent - calling twice does not double-decrement", () => {
    const latch = shutdownLatch()
    const handle = latch.registerActivity()

    handle.end()
    handle.end()

    expect(latch.activeCount).toBe(0)
  })

  it("initiateShutdown resolves immediately when no active activities", async () => {
    const latch = shutdownLatch()

    await latch.initiateShutdown()

    expect(latch.shuttingDown).toBe(true)
  })

  it("initiateShutdown drains pending activities before resolving", async () => {
    const latch = shutdownLatch()
    const h1 = latch.registerActivity()
    const h2 = latch.registerActivity()

    let drained = false
    const shutdownPromise = latch.initiateShutdown().then(() => {
      drained = true
    })

    expect(latch.shuttingDown).toBe(true)
    expect(drained).toBe(false)

    h1.end()
    // still one active
    expect(drained).toBe(false)

    h2.end()
    await shutdownPromise

    expect(drained).toBe(true)
    expect(latch.activeCount).toBe(0)
  })

  it("rejects new activities after shutdown is initiated", () => {
    const latch = shutdownLatch()
    latch.initiateShutdown()

    expect(() => latch.registerActivity()).toThrow(ShutdownInProgressError)
  })

  it("ShutdownInProgressError has correct name and message", () => {
    const error = new ShutdownInProgressError()

    expect(error.name).toBe("ShutdownInProgressError")
    expect(error.message).toBe("Shutdown in progress")
    expect(error).toBeInstanceOf(Error)
  })
})

describe("Error mapping", () => {
  describe("mapErrorCode", () => {
    it("maps NO_HANDLER_FOR_COMMAND to NoHandlerForCommandError", () => {
      const err = mapErrorCode(AxonServerErrorCode.NO_HANDLER_FOR_COMMAND, "no handler")

      expect(err).toBeInstanceOf(NoHandlerForCommandError)
      expect(err.errorCode).toBe(AxonServerErrorCode.NO_HANDLER_FOR_COMMAND)
      expect(err.transient).toBe(false)
    })

    it("maps NO_HANDLER_FOR_QUERY to NoHandlerForQueryError", () => {
      const err = mapErrorCode(AxonServerErrorCode.NO_HANDLER_FOR_QUERY, "no handler")

      expect(err).toBeInstanceOf(NoHandlerForQueryError)
      expect(err.transient).toBe(false)
    })

    it("maps COMMAND_EXECUTION_ERROR to transient CommandExecutionError", () => {
      const err = mapErrorCode(AxonServerErrorCode.COMMAND_EXECUTION_ERROR, "failed")

      expect(err).toBeInstanceOf(CommandExecutionError)
      expect(err.transient).toBe(true)
    })

    it("maps COMMAND_EXECUTION_NON_TRANSIENT_ERROR to non-transient CommandExecutionError", () => {
      const err = mapErrorCode(AxonServerErrorCode.COMMAND_EXECUTION_NON_TRANSIENT_ERROR, "failed")

      expect(err).toBeInstanceOf(CommandExecutionError)
      expect(err.transient).toBe(false)
    })

    it("maps QUERY_EXECUTION_ERROR to transient QueryExecutionError", () => {
      const err = mapErrorCode(AxonServerErrorCode.QUERY_EXECUTION_ERROR, "failed")

      expect(err).toBeInstanceOf(QueryExecutionError)
      expect(err.transient).toBe(true)
    })

    it("maps QUERY_EXECUTION_NON_TRANSIENT_ERROR to non-transient QueryExecutionError", () => {
      const err = mapErrorCode(AxonServerErrorCode.QUERY_EXECUTION_NON_TRANSIENT_ERROR, "failed")

      expect(err).toBeInstanceOf(QueryExecutionError)
      expect(err.transient).toBe(false)
    })

    it("maps COMMAND_DISPATCH_ERROR to CommandDispatchError", () => {
      const err = mapErrorCode(AxonServerErrorCode.COMMAND_DISPATCH_ERROR, "dispatch failed")

      expect(err).toBeInstanceOf(CommandDispatchError)
      expect(err.transient).toBe(true)
    })

    it("maps QUERY_DISPATCH_ERROR to QueryDispatchError", () => {
      const err = mapErrorCode(AxonServerErrorCode.QUERY_DISPATCH_ERROR, "dispatch failed")

      expect(err).toBeInstanceOf(QueryDispatchError)
      expect(err.transient).toBe(true)
    })

    it("maps CONCURRENCY_EXCEPTION to ConcurrencyError", () => {
      const err = mapErrorCode(AxonServerErrorCode.CONCURRENCY_EXCEPTION, "conflict")

      expect(err).toBeInstanceOf(ConcurrencyError)
      expect(err.transient).toBe(true)
    })

    it("maps COMMAND_CONCURRENCY_ERROR to ConcurrencyError", () => {
      const err = mapErrorCode(AxonServerErrorCode.COMMAND_CONCURRENCY_ERROR, "conflict")

      expect(err).toBeInstanceOf(ConcurrencyError)
      expect(err.transient).toBe(true)
    })

    it("maps CONNECTION_FAILED to ConnectionFailedError", () => {
      const err = mapErrorCode(AxonServerErrorCode.CONNECTION_FAILED, "timeout")

      expect(err).toBeInstanceOf(ConnectionFailedError)
      expect(err.transient).toBe(true)
    })

    it("maps GRPC_MESSAGE_TOO_LARGE to ConnectionFailedError", () => {
      const err = mapErrorCode(AxonServerErrorCode.GRPC_MESSAGE_TOO_LARGE, "too large")

      expect(err).toBeInstanceOf(ConnectionFailedError)
      expect(err.transient).toBe(true)
    })

    it("maps AUTHENTICATION_TOKEN_MISSING to AuthenticationError", () => {
      const err = mapErrorCode(AxonServerErrorCode.AUTHENTICATION_TOKEN_MISSING, "no token")

      expect(err).toBeInstanceOf(AuthenticationError)
      expect(err.transient).toBe(false)
    })

    it("maps AUTHENTICATION_INVALID_TOKEN to AuthenticationError", () => {
      const err = mapErrorCode(AxonServerErrorCode.AUTHENTICATION_INVALID_TOKEN, "bad token")

      expect(err).toBeInstanceOf(AuthenticationError)
      expect(err.transient).toBe(false)
    })

    it("maps unknown codes to base AxonServerError", () => {
      const err = mapErrorCode("AXONIQ-9999", "unknown")

      expect(err).toBeInstanceOf(AxonServerError)
      expect(err.errorCode).toBe("AXONIQ-9999")
    })

    it("marks unknown transient codes as transient", () => {
      // NO_EVENT_STORE_MASTER_AVAILABLE is in TRANSIENT_CODES but has no specific case
      const err = mapErrorCode(AxonServerErrorCode.NO_EVENT_STORE_MASTER_AVAILABLE, "no master")

      expect(err.transient).toBe(true)
    })
  })

  describe("isTransientError", () => {
    it("returns true for transient AxonServerError", () => {
      const err = new CommandDispatchError("dispatch failed")

      expect(isTransientError(err)).toBe(true)
    })

    it("returns false for non-transient AxonServerError", () => {
      const err = new NoHandlerForCommandError("no handler")

      expect(isTransientError(err)).toBe(false)
    })

    it("returns false for plain Error", () => {
      expect(isTransientError(new Error("something"))).toBe(false)
    })

    it("returns false for non-error values", () => {
      expect(isTransientError(null)).toBe(false)
      expect(isTransientError(undefined)).toBe(false)
      expect(isTransientError("string error")).toBe(false)
    })
  })

  describe("error hierarchy", () => {
    it("all typed errors extend AxonServerError", () => {
      expect(new NoHandlerForCommandError("msg")).toBeInstanceOf(AxonServerError)
      expect(new CommandExecutionError("msg")).toBeInstanceOf(AxonServerError)
      expect(new QueryExecutionError("msg")).toBeInstanceOf(AxonServerError)
      expect(new ConcurrencyError("msg")).toBeInstanceOf(AxonServerError)
      expect(new ConnectionFailedError("msg")).toBeInstanceOf(AxonServerError)
      expect(new AuthenticationError("msg", "AXONIQ-1000")).toBeInstanceOf(AxonServerError)
    })

    it("all typed errors extend Error", () => {
      expect(new AxonServerError("msg", "code", false)).toBeInstanceOf(Error)
    })
  })
})

describe("MessageSizeValidator", () => {
  it("accepts messages under the limit", () => {
    const validator = messageSizeValidator({ maxMessageSize: 100 })
    const data = new Uint8Array(50)

    // should not throw
    expect(() => validator.validate(data)).not.toThrow()
  })

  it("throws when message exceeds the limit", () => {
    const validator = messageSizeValidator({ maxMessageSize: 100 })
    const data = new Uint8Array(101)

    expect(() => validator.validate(data)).toThrow(MessageSizeExceededError)
  })

  it("MessageSizeExceededError carries size details", () => {
    const validator = messageSizeValidator({ maxMessageSize: 100 })
    const data = new Uint8Array(200)

    try {
      validator.validate(data)
      expect.unreachable("should have thrown")
    } catch (e) {
      expect(e).toBeInstanceOf(MessageSizeExceededError)
      const err = e as MessageSizeExceededError
      expect(err.actualSize).toBe(200)
      expect(err.maxSize).toBe(100)
      expect(err.message).toContain("200")
      expect(err.message).toContain("100")
    }
  })

  it("warns when message exceeds warning threshold but not max", () => {
    // Use custom threshold of 0.5 (50%)
    const validator = messageSizeValidator({
      maxMessageSize: 100,
      warningThreshold: 0.5,
    })
    const data = new Uint8Array(60) // 60% > 50% threshold

    // Should not throw (under max), but internally warns
    expect(() => validator.validate(data)).not.toThrow()
  })

  it("does not warn when under the warning threshold", () => {
    const validator = messageSizeValidator({
      maxMessageSize: 100,
      warningThreshold: 0.75,
    })
    const data = new Uint8Array(50) // 50% < 75% threshold

    expect(() => validator.validate(data)).not.toThrow()
  })

  it("uses default max size of 4MB", () => {
    const validator = messageSizeValidator()

    expect(validator.maxSize).toBe(4 * 1024 * 1024)
  })

  it("estimateSize returns byte length of JSON-serialized payload", () => {
    const validator = messageSizeValidator()

    const size = validator.estimateSize({ key: "value" })

    // JSON.stringify({ key: "value" }) = '{"key":"value"}' = 15 bytes
    expect(size).toBe(15)
  })

  it("estimateSize handles nested objects", () => {
    const validator = messageSizeValidator()

    const size = validator.estimateSize({ a: { b: "c" } })

    expect(size).toBeGreaterThan(0)
  })

  it("validate includes context in warning message", () => {
    // This tests that context parameter is accepted without error
    const validator = messageSizeValidator({ maxMessageSize: 100 })
    const data = new Uint8Array(10)

    expect(() => validator.validate(data, "command-dispatch")).not.toThrow()
  })

  it("exactly at max size does not throw", () => {
    const validator = messageSizeValidator({ maxMessageSize: 100 })
    const data = new Uint8Array(100)

    expect(() => validator.validate(data)).not.toThrow()
  })

  it("one byte over max size throws", () => {
    const validator = messageSizeValidator({ maxMessageSize: 100 })
    const data = new Uint8Array(101)

    expect(() => validator.validate(data)).toThrow(MessageSizeExceededError)
  })
})
