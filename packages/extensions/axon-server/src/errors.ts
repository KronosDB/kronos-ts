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
 * Base error for all Axon Server errors. Carries the error code
 * and whether the error is transient (retryable).
 */
export class AxonServerError extends Error {
  readonly errorCode: string
  readonly transient: boolean

  constructor(message: string, errorCode: string, transient: boolean) {
    super(message)
    this.name = "AxonServerError"
    this.errorCode = errorCode
    this.transient = transient
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

/** Optimistic concurrency violation (transient — retry with fresh state). */
export class ConcurrencyError extends AxonServerError {
  constructor(message: string) {
    super(message, AxonServerErrorCode.CONCURRENCY_EXCEPTION, true)
    this.name = "ConcurrencyError"
  }
}

/** Connection to Axon Server failed. */
export class ConnectionFailedError extends AxonServerError {
  constructor(message: string) {
    super(message, AxonServerErrorCode.CONNECTION_FAILED, true)
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
  AxonServerErrorCode.GRPC_MESSAGE_TOO_LARGE,
  AxonServerErrorCode.CONCURRENCY_EXCEPTION,
  AxonServerErrorCode.COMMAND_CONCURRENCY_ERROR,
  AxonServerErrorCode.NO_EVENT_STORE_MASTER_AVAILABLE,
])

/**
 * Convert an Axon Server error code + message into a typed exception.
 */
export function mapErrorCode(errorCode: string, message: string): AxonServerError {
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
      return new ConcurrencyError(message)
    case AxonServerErrorCode.CONNECTION_FAILED:
    case AxonServerErrorCode.GRPC_MESSAGE_TOO_LARGE:
      return new ConnectionFailedError(message)
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
 * Check if an error is transient (retryable).
 */
export function isTransientError(error: unknown): boolean {
  if (error instanceof AxonServerError) return error.transient
  return false
}
