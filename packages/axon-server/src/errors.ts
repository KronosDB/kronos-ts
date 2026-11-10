import { AppendConditionError, isConflict, isUnavailable } from "@kronos-ts/core"

/**
 * Axon Server error codes — mapped from the Java framework's ErrorCode enum.
 *
 * Error codes follow the AXONIQ-XXXX pattern where the first digit
 * indicates the category:
 * - 1xxx: Authentication/instruction errors
 * - 2xxx: Event publishing errors
 * - 3xxx: Communication errors
 * - 4xxx: Command errors
 * - 5xxx: Query errors
 * - 9xxx: Internal/storage errors
 */
export const AxonServerErrorCode = {
  // Authentication & instructions
  AUTHENTICATION_TOKEN_MISSING: "AXONIQ-1000",
  AUTHENTICATION_INVALID_TOKEN: "AXONIQ-1001",
  UNSUPPORTED_INSTRUCTION: "AXONIQ-1002",
  INSTRUCTION_ACK_ERROR: "AXONIQ-1003",
  INSTRUCTION_EXECUTION_ERROR: "AXONIQ-1004",

  // Event publishing
  EVENT_PAYLOAD_TOO_LARGE: "AXONIQ-2001",
  NO_EVENT_STORE_MASTER_AVAILABLE: "AXONIQ-2100",
  CONCURRENCY_EXCEPTION: "AXONIQ-2000",

  // Communication
  CONNECTION_FAILED: "AXONIQ-3001",
  GRPC_MESSAGE_TOO_LARGE: "AXONIQ-3002",

  // Commands
  NO_HANDLER_FOR_COMMAND: "AXONIQ-4000",
  COMMAND_EXECUTION_ERROR: "AXONIQ-4002",
  COMMAND_DISPATCH_ERROR: "AXONIQ-4003",
  COMMAND_CONCURRENCY_ERROR: "AXONIQ-4004",
  COMMAND_EXECUTION_NON_TRANSIENT_ERROR: "AXONIQ-4005",

  // Queries
  NO_HANDLER_FOR_QUERY: "AXONIQ-5000",
  QUERY_EXECUTION_ERROR: "AXONIQ-5001",
  QUERY_DISPATCH_ERROR: "AXONIQ-5002",
  QUERY_EXECUTION_NON_TRANSIENT_ERROR: "AXONIQ-5003",

  // Internal/storage
  DATAFILE_READ_ERROR: "AXONIQ-9000",
  INDEX_READ_ERROR: "AXONIQ-9001",
  DATAFILE_WRITE_ERROR: "AXONIQ-9100",
  INDEX_WRITE_ERROR: "AXONIQ-9101",
  DIRECTORY_CREATION_FAILED: "AXONIQ-9102",
  VALIDATION_FAILED: "AXONIQ-9200",
  TRANSACTION_ROLLED_BACK: "AXONIQ-9900",

  // Default
  OTHER: "AXONIQ-0001",
} as const

export type AxonServerErrorCodeValue = typeof AxonServerErrorCode[keyof typeof AxonServerErrorCode]

// ---------------------------------------------------------------------------
// Exception hierarchy
// ---------------------------------------------------------------------------

/**
 * The codes that mean the server or a handler could not serve the request
 * now, and the same request later may succeed: core's `unavailable` kind.
 */
const UNAVAILABLE_CODES = new Set<string>([
  AxonServerErrorCode.CONNECTION_FAILED,
  AxonServerErrorCode.NO_EVENT_STORE_MASTER_AVAILABLE,
  AxonServerErrorCode.COMMAND_DISPATCH_ERROR,
  AxonServerErrorCode.QUERY_DISPATCH_ERROR,
])

/**
 * Base error for all Axon Server errors. Carries the error code
 * and whether the error is transient (retryable).
 */
export class AxonServerError extends Error {
  readonly errorCode: string
  readonly transient: boolean
  /** Core's `unavailable` kind, when the code says to retry later, so `isUnavailable` recognises it. */
  readonly kind?: "unavailable"

  constructor(message: string, errorCode: string, transient: boolean, options?: ErrorOptions) {
    super(message, options)
    this.name = "AxonServerError"
    this.errorCode = errorCode
    this.transient = transient
    if (UNAVAILABLE_CODES.has(errorCode)) this.kind = "unavailable"
  }
}

/** No handler registered for the dispatched command. */
export class NoHandlerForCommandError extends AxonServerError {
  constructor(message: string) {
    super(message, AxonServerErrorCode.NO_HANDLER_FOR_COMMAND, false)
    this.name = "NoHandlerForCommandError"
  }
}

/** No handler registered for the dispatched query. */
export class NoHandlerForQueryError extends AxonServerError {
  constructor(message: string) {
    super(message, AxonServerErrorCode.NO_HANDLER_FOR_QUERY, false)
    this.name = "NoHandlerForQueryError"
  }
}

/** Command handler execution failed (transient — may succeed on retry). */
export class CommandExecutionError extends AxonServerError {
  constructor(message: string, transient: boolean = true) {
    super(
      message,
      transient
        ? AxonServerErrorCode.COMMAND_EXECUTION_ERROR
        : AxonServerErrorCode.COMMAND_EXECUTION_NON_TRANSIENT_ERROR,
      transient,
    )
    this.name = "CommandExecutionError"
  }
}

/** Query handler execution failed (transient — may succeed on retry). */
export class QueryExecutionError extends AxonServerError {
  constructor(message: string, transient: boolean = true) {
    super(
      message,
      transient
        ? AxonServerErrorCode.QUERY_EXECUTION_ERROR
        : AxonServerErrorCode.QUERY_EXECUTION_NON_TRANSIENT_ERROR,
      transient,
    )
    this.name = "QueryExecutionError"
  }
}

/** Command dispatch failed (infrastructure error). */
export class CommandDispatchError extends AxonServerError {
  constructor(message: string) {
    super(message, AxonServerErrorCode.COMMAND_DISPATCH_ERROR, true)
    this.name = "CommandDispatchError"
  }
}

/** Query dispatch failed (infrastructure error). */
export class QueryDispatchError extends AxonServerError {
  constructor(message: string) {
    super(message, AxonServerErrorCode.QUERY_DISPATCH_ERROR, true)
    this.name = "QueryDispatchError"
  }
}

/**
 * Axon Server refused an append whose condition was violated: core's
 * {@link AppendConditionError}, so `isConflict` recognises it, with Axon
 * Server's code. Ask with `isConflict` from `@kronos-ts/core` rather than this class.
 *
 * It is not an `AxonServerError`: it carries the same `errorCode` and
 * `transient` fields, and `isTransientError` answers for it.
 */
export class ConcurrencyError extends AppendConditionError {
  readonly errorCode: string
  readonly transient: boolean = true

  constructor(message: string, errorCode: string = AxonServerErrorCode.CONCURRENCY_EXCEPTION, options?: ErrorOptions) {
    super(message, options)
    this.name = "ConcurrencyError"
    this.errorCode = errorCode
  }
}

/** Connection to Axon Server failed. */
export class ConnectionFailedError extends AxonServerError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, AxonServerErrorCode.CONNECTION_FAILED, true, options)
    this.name = "ConnectionFailedError"
  }
}

/** Authentication/authorization failed. */
export class AuthenticationError extends AxonServerError {
  constructor(message: string, code: string) {
    super(message, code, false)
    this.name = "AuthenticationError"
  }
}

// ---------------------------------------------------------------------------
// Error code → exception mapping
// ---------------------------------------------------------------------------

const TRANSIENT_CODES = new Set<string>([
  AxonServerErrorCode.COMMAND_EXECUTION_ERROR,
  AxonServerErrorCode.QUERY_EXECUTION_ERROR,
  AxonServerErrorCode.COMMAND_DISPATCH_ERROR,
  AxonServerErrorCode.QUERY_DISPATCH_ERROR,
  AxonServerErrorCode.CONNECTION_FAILED,
  AxonServerErrorCode.CONCURRENCY_EXCEPTION,
  AxonServerErrorCode.COMMAND_CONCURRENCY_ERROR,
  AxonServerErrorCode.NO_EVENT_STORE_MASTER_AVAILABLE,
])

/**
 * Convert an Axon Server error code + message into a typed exception.
 */
export function mapErrorCode(errorCode: string, message: string): AxonServerError | ConcurrencyError {
  switch (errorCode) {
    case AxonServerErrorCode.NO_HANDLER_FOR_COMMAND:
      return new NoHandlerForCommandError(message)
    case AxonServerErrorCode.NO_HANDLER_FOR_QUERY:
      return new NoHandlerForQueryError(message)
    case AxonServerErrorCode.COMMAND_EXECUTION_ERROR:
      return new CommandExecutionError(message, true)
    case AxonServerErrorCode.COMMAND_EXECUTION_NON_TRANSIENT_ERROR:
      return new CommandExecutionError(message, false)
    case AxonServerErrorCode.QUERY_EXECUTION_ERROR:
      return new QueryExecutionError(message, true)
    case AxonServerErrorCode.QUERY_EXECUTION_NON_TRANSIENT_ERROR:
      return new QueryExecutionError(message, false)
    case AxonServerErrorCode.COMMAND_DISPATCH_ERROR:
      return new CommandDispatchError(message)
    case AxonServerErrorCode.QUERY_DISPATCH_ERROR:
      return new QueryDispatchError(message)
    case AxonServerErrorCode.CONCURRENCY_EXCEPTION:
    case AxonServerErrorCode.COMMAND_CONCURRENCY_ERROR:
      return new ConcurrencyError(message, errorCode)
    case AxonServerErrorCode.CONNECTION_FAILED:
      return new ConnectionFailedError(message)
    case AxonServerErrorCode.GRPC_MESSAGE_TOO_LARGE:
      // Too large is too large on every retry: not a connection failure.
      return new AxonServerError(message, errorCode, false)
    case AxonServerErrorCode.AUTHENTICATION_TOKEN_MISSING:
    case AxonServerErrorCode.AUTHENTICATION_INVALID_TOKEN:
      return new AuthenticationError(message, errorCode)
    default:
      return new AxonServerError(
        message,
        errorCode,
        TRANSIENT_CODES.has(errorCode),
      )
  }
}

/**
 * Whether the error's own `transient` flag is set. That flag is the protocol's
 * word and does not mean "retry later": a handler's business failure is
 * transient too.
 *
 * @deprecated Use `isUnavailable` (retry later) and `isConflict` (load again)
 * from `@kronos-ts/core`.
 */
export function isTransientError(error: unknown): boolean {
  if (error instanceof AxonServerError || error instanceof ConcurrencyError) return error.transient
  return false
}

/**
 * The code a command handler's failure goes back to its caller with: its
 * contract kind's code when it has one, so the caller's `mapErrorCode`
 * rebuilds the same kind, and the execution error otherwise.
 */
export function commandFailureCode(error: unknown): string {
  if (isConflict(error)) return AxonServerErrorCode.COMMAND_CONCURRENCY_ERROR
  if (isUnavailable(error)) return AxonServerErrorCode.CONNECTION_FAILED
  return AxonServerErrorCode.COMMAND_EXECUTION_ERROR
}

/**
 * The code a query handler's failure goes back with, as {@link commandFailureCode}.
 * Axon Server has no query concurrency code, so a conflict is the event store's.
 */
export function queryFailureCode(error: unknown): string {
  if (isConflict(error)) return AxonServerErrorCode.CONCURRENCY_EXCEPTION
  if (isUnavailable(error)) return AxonServerErrorCode.CONNECTION_FAILED
  return AxonServerErrorCode.QUERY_EXECUTION_ERROR
}

/** gRPC statuses that mean the server could not answer now: UNAVAILABLE and DEADLINE_EXCEEDED. */
const UNAVAILABLE_GRPC_STATUSES = new Set([14, 4])

/**
 * The error a failed gRPC call is reported as: a `ConnectionFailedError`, with
 * the client's error as its cause, when the server could not answer now, and
 * the error itself otherwise.
 */
export function fromGrpc(error: unknown): unknown {
  const code = (error as { code?: unknown } | null)?.code
  if (error instanceof Error && error.name === "ClientError" && typeof code === "number" && UNAVAILABLE_GRPC_STATUSES.has(code)) {
    return new ConnectionFailedError(error.message, { cause: error })
  }
  return error
}
