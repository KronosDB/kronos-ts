/**
 * KronosDB error codes.
 *
 * Error codes follow the KRONOS-XXXX pattern where the first digit
 * indicates the category:
 * - 1xxx: Authentication/instruction errors
 * - 2xxx: Event store errors
 * - 3xxx: Communication errors
 * - 4xxx: Command errors
 * - 5xxx: Query errors
 * - 9xxx: Internal/storage errors
 */
export const KronosDbErrorCode = {
  // Authentication & instructions
  AUTHENTICATION_TOKEN_MISSING: "KRONOS-1000",
  AUTHENTICATION_INVALID_TOKEN: "KRONOS-1001",

  // Event store
  CONSISTENCY_CONDITION_VIOLATED: "KRONOS-2000",
  EVENT_PAYLOAD_TOO_LARGE: "KRONOS-2001",
  NO_EVENT_STORE_LEADER: "KRONOS-2100",

  // Communication
  CONNECTION_FAILED: "KRONOS-3001",
  GRPC_MESSAGE_TOO_LARGE: "KRONOS-3002",

  // Commands
  NO_HANDLER_FOR_COMMAND: "KRONOS-4000",
  COMMAND_EXECUTION_ERROR: "KRONOS-4002",
  COMMAND_DISPATCH_ERROR: "KRONOS-4003",

  // Queries
  NO_HANDLER_FOR_QUERY: "KRONOS-5000",
  QUERY_EXECUTION_ERROR: "KRONOS-5001",
  QUERY_DISPATCH_ERROR: "KRONOS-5002",

  // Internal/storage
  DATAFILE_READ_ERROR: "KRONOS-9000",
  DATAFILE_WRITE_ERROR: "KRONOS-9100",

  // Default
  OTHER: "KRONOS-0001",
} as const

export type KronosDbErrorCodeValue = typeof KronosDbErrorCode[keyof typeof KronosDbErrorCode]

/**
 * Base error for all KronosDB errors.
 */
export class KronosDbError extends Error {
  readonly errorCode: string
  readonly transient: boolean

  constructor(message: string, errorCode: string, transient: boolean) {
    super(message)
    this.name = "KronosDbError"
    this.errorCode = errorCode
    this.transient = transient
  }
}

export class NoHandlerForCommandError extends KronosDbError {
  constructor(message: string) {
    super(message, KronosDbErrorCode.NO_HANDLER_FOR_COMMAND, false)
    this.name = "NoHandlerForCommandError"
  }
}

export class NoHandlerForQueryError extends KronosDbError {
  constructor(message: string) {
    super(message, KronosDbErrorCode.NO_HANDLER_FOR_QUERY, false)
    this.name = "NoHandlerForQueryError"
  }
}

export class CommandExecutionError extends KronosDbError {
  constructor(message: string) {
    super(message, KronosDbErrorCode.COMMAND_EXECUTION_ERROR, true)
    this.name = "CommandExecutionError"
  }
}

export class QueryExecutionError extends KronosDbError {
  constructor(message: string) {
    super(message, KronosDbErrorCode.QUERY_EXECUTION_ERROR, true)
    this.name = "QueryExecutionError"
  }
}

export class CommandDispatchError extends KronosDbError {
  constructor(message: string) {
    super(message, KronosDbErrorCode.COMMAND_DISPATCH_ERROR, true)
    this.name = "CommandDispatchError"
  }
}

export class QueryDispatchError extends KronosDbError {
  constructor(message: string) {
    super(message, KronosDbErrorCode.QUERY_DISPATCH_ERROR, true)
    this.name = "QueryDispatchError"
  }
}

export class ConcurrencyError extends KronosDbError {
  constructor(message: string) {
    super(message, KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED, true)
    this.name = "ConcurrencyError"
  }
}

export class ConnectionFailedError extends KronosDbError {
  constructor(message: string) {
    super(message, KronosDbErrorCode.CONNECTION_FAILED, true)
    this.name = "ConnectionFailedError"
  }
}

export class AuthenticationError extends KronosDbError {
  constructor(message: string, code: string) {
    super(message, code, false)
    this.name = "AuthenticationError"
  }
}

const TRANSIENT_CODES = new Set<string>([
  KronosDbErrorCode.COMMAND_EXECUTION_ERROR,
  KronosDbErrorCode.QUERY_EXECUTION_ERROR,
  KronosDbErrorCode.COMMAND_DISPATCH_ERROR,
  KronosDbErrorCode.QUERY_DISPATCH_ERROR,
  KronosDbErrorCode.CONNECTION_FAILED,
  KronosDbErrorCode.GRPC_MESSAGE_TOO_LARGE,
  KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED,
  KronosDbErrorCode.NO_EVENT_STORE_LEADER,
])

/**
 * Convert a KronosDB error code + message into a typed exception.
 */
export function mapErrorCode(errorCode: string, message: string): KronosDbError {
  switch (errorCode) {
    case KronosDbErrorCode.NO_HANDLER_FOR_COMMAND:
      return new NoHandlerForCommandError(message)
    case KronosDbErrorCode.NO_HANDLER_FOR_QUERY:
      return new NoHandlerForQueryError(message)
    case KronosDbErrorCode.COMMAND_EXECUTION_ERROR:
      return new CommandExecutionError(message)
    case KronosDbErrorCode.QUERY_EXECUTION_ERROR:
      return new QueryExecutionError(message)
    case KronosDbErrorCode.COMMAND_DISPATCH_ERROR:
      return new CommandDispatchError(message)
    case KronosDbErrorCode.QUERY_DISPATCH_ERROR:
      return new QueryDispatchError(message)
    case KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED:
      return new ConcurrencyError(message)
    case KronosDbErrorCode.CONNECTION_FAILED:
    case KronosDbErrorCode.GRPC_MESSAGE_TOO_LARGE:
      return new ConnectionFailedError(message)
    case KronosDbErrorCode.AUTHENTICATION_TOKEN_MISSING:
    case KronosDbErrorCode.AUTHENTICATION_INVALID_TOKEN:
      return new AuthenticationError(message, errorCode)
    default:
      return new KronosDbError(
        message,
        errorCode,
        TRANSIENT_CODES.has(errorCode),
      )
  }
}

export function isTransientError(error: unknown): boolean {
  if (error instanceof KronosDbError) return error.transient
  return false
}
