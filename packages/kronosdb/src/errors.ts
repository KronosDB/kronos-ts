import { AppendConditionError, isConflict, isUnavailable } from "@kronos-ts/core"
import { ErrorCode as NativeErrorCode, ErrorCodeNames as NativeErrorCodeNames } from "./protocol/kronosdb-protocol.js"

/**
 * KronosDB error codes.
 *
 * Error codes follow the KRONOS-XXXX pattern where the first digit
 * indicates the category (the two the server itself emits on a dispatch,
 * 4005 and 4006, are spelled KRONOSDB-XXXX):
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

  // Commands
  NO_HANDLER_FOR_COMMAND: "KRONOS-4000",
  COMMAND_EXECUTION_ERROR: "KRONOS-4002",
  COMMAND_DISPATCH_ERROR: "KRONOS-4003",
  /** The server's: no handler answered a command in time. The command may still have run. */
  COMMAND_TIMEOUT: "KRONOSDB-4005",
  /** The server's: the connection of the handler a command was routed to was lost. The command may have run. */
  CONNECTION_TO_HANDLER_LOST: "KRONOSDB-4006",

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
 * The codes that mean the server or a handler could not serve the request
 * now, and the same request later may succeed: core's `unavailable` kind.
 */
const UNAVAILABLE_CODES = new Set<string>([
  KronosDbErrorCode.CONNECTION_FAILED,
  KronosDbErrorCode.NO_EVENT_STORE_LEADER,
  KronosDbErrorCode.COMMAND_DISPATCH_ERROR,
  KronosDbErrorCode.QUERY_DISPATCH_ERROR,
  KronosDbErrorCode.COMMAND_TIMEOUT,
  KronosDbErrorCode.CONNECTION_TO_HANDLER_LOST,
])

/**
 * Base error for all KronosDB errors. One whose code says to retry later
 * carries core's `unavailable` kind, so `isUnavailable` recognises it.
 */
export class KronosDbError extends Error {
  readonly errorCode: string
  readonly transient: boolean
  /** Core's contract kind: `unavailable` when the code says to retry later, `no-handler` when nothing handles the name. */
  readonly kind?: "unavailable" | "no-handler"

  constructor(message: string, errorCode: string, transient: boolean, options?: ErrorOptions) {
    super(message, options)
    this.name = "KronosDbError"
    this.errorCode = errorCode
    this.transient = transient
    if (UNAVAILABLE_CODES.has(errorCode)) this.kind = "unavailable"
  }
}

export class NoHandlerForCommandError extends KronosDbError {
  /** Core's `no-handler` kind, so `isNoHandler` recognises it. */
  readonly kind: "no-handler" = "no-handler"

  constructor(message: string) {
    super(message, KronosDbErrorCode.NO_HANDLER_FOR_COMMAND, false)
    this.name = "NoHandlerForCommandError"
  }
}

export class NoHandlerForQueryError extends KronosDbError {
  /** Core's `no-handler` kind, so `isNoHandler` recognises it. */
  readonly kind: "no-handler" = "no-handler"

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

/**
 * KronosDB refused an append whose condition was violated: core's
 * {@link AppendConditionError}, so `isConflict` recognises it, with KronosDB's
 * code. Ask with `isConflict` from `@kronos-ts/core` rather than this class.
 *
 * It is not a `KronosDbError`: it carries the same `errorCode` and
 * `transient` fields, and `isTransientError` answers for it.
 */
export class ConcurrencyError extends AppendConditionError {
  readonly errorCode: string = KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED
  readonly transient: boolean = true

  constructor(message: string) {
    super(message)
    this.name = "ConcurrencyError"
  }
}

export class ConnectionFailedError extends KronosDbError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, KronosDbErrorCode.CONNECTION_FAILED, true, options)
    this.name = "ConnectionFailedError"
  }
}

export class AuthenticationError extends KronosDbError {
  constructor(message: string, code: string) {
    super(message, code, false)
    this.name = "AuthenticationError"
  }
}

/**
 * A request the native protocol answered with RequestFailed.
 *
 * `status` is the protocol's own word for what went wrong (`unavailable`,
 * `rejected`, `not_found`, …) so a caller can branch on it without knowing the
 * numeric code; `errorCode` places the same failure in the KRONOS-XXXX
 * catalogue the rest of this package speaks.
 */
export class KronosDbRequestError extends KronosDbError {
  readonly status: string

  constructor(status: string, message: string, errorCode: string, transient: boolean) {
    super(message, errorCode, transient)
    this.name = "KronosDbRequestError"
    this.status = status
    // `no_handler` is core's no-handler kind; `unavailable` already has its kind from the code.
    if (status === "no_handler") (this as { kind?: string }).kind = "no-handler"
  }
}

/**
 * The error a native RequestFailed becomes. Codes this client does not know
 * are `internal`, as the protocol requires.
 */
export function requestError(code: number, message: string): KronosDbError {
  const status = NativeErrorCodeNames[code] ?? "internal"
  switch (code) {
    case NativeErrorCode.unavailable:
      // Not ready, failing over, unreachable: the one status that is safe to retry as-is.
      return new KronosDbRequestError(status, message, KronosDbErrorCode.CONNECTION_FAILED, true)
    case NativeErrorCode.no_handler:
      // Nothing handles the dispatched name. Not a retry until something subscribes.
      return new KronosDbRequestError(status, message, KronosDbErrorCode.OTHER, false)
    case NativeErrorCode.unauthenticated:
      return new AuthenticationError(message, KronosDbErrorCode.AUTHENTICATION_INVALID_TOKEN)
    case NativeErrorCode.corrupted:
      return new KronosDbRequestError(status, message, KronosDbErrorCode.DATAFILE_READ_ERROR, false)
    default:
      return new KronosDbRequestError(status, message, KronosDbErrorCode.OTHER, false)
  }
}

const TRANSIENT_CODES = new Set<string>([
  KronosDbErrorCode.COMMAND_EXECUTION_ERROR,
  KronosDbErrorCode.QUERY_EXECUTION_ERROR,
  KronosDbErrorCode.COMMAND_DISPATCH_ERROR,
  KronosDbErrorCode.QUERY_DISPATCH_ERROR,
  KronosDbErrorCode.CONNECTION_FAILED,
  KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED,
  KronosDbErrorCode.NO_EVENT_STORE_LEADER,
])

/**
 * Convert a KronosDB error code + message into a typed exception.
 */
export function mapErrorCode(errorCode: string, message: string): KronosDbError | ConcurrencyError {
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

/**
 * Whether the error's own `transient` flag is set. That flag is the protocol's
 * word and does not mean "retry later": a handler's business failure is
 * transient too.
 *
 * @deprecated Use `isUnavailable` (retry later) and `isConflict` (load again)
 * from `@kronos-ts/core`.
 */
export function isTransientError(error: unknown): boolean {
  if (error instanceof KronosDbError || error instanceof ConcurrencyError) return error.transient
  return false
}

/**
 * The code a handling failure goes back to its caller with: its contract
 * kind's code when it has one, so the caller's `mapErrorCode` rebuilds the
 * same kind, and `fallback` otherwise.
 */
export function handlingFailureCode(error: unknown, fallback: string): string {
  if (isConflict(error)) return KronosDbErrorCode.CONSISTENCY_CONDITION_VIOLATED
  if (isUnavailable(error)) return KronosDbErrorCode.CONNECTION_FAILED
  return fallback
}
