import { AppendConditionError as CoreAppendConditionError } from "@kronos-ts/core"

/**
 * SQLSTATE-style code tagged onto the error when a DCB append condition is
 * violated. The conflict check in `checkAndInsert` detects the violation and
 * attaches this code (per D-12.12: a dedicated code, never error-text parsing).
 *
 * `KR001` lives in the Postgres user-defined SQLSTATE range (KX–ZZ).
 * It is intentionally distinct from:
 *   - `P0001` — generic RAISE (would over-match any unhandled plpgsql exception)
 *   - `23505` — unique_violation (used by primary key conflicts)
 * Adapter layers translate `err.code === KR001` into AppendConditionError.
 */
export const KRONOS_DCB_VIOLATION_SQLSTATE = "KR001"

/**
 * Core's {@link CoreAppendConditionError}, with the conflict count in its
 * message. Throw and catch core's class, and ask with core's `isConflict`:
 * this subclass stays for one release and then goes.
 *
 * @deprecated Use `AppendConditionError` and `isConflict` from `@kronos-ts/core`.
 */
export class AppendConditionError extends CoreAppendConditionError {
  static fromConflictCount(count: number, afterPosition: bigint, options?: ErrorOptions): AppendConditionError {
    return new AppendConditionError(
      `Append condition violated: ${count} conflicting event(s) ` +
        `found after position ${afterPosition}`,
      options,
    )
  }
}

/**
 * Adapter-agnostic check: pg and postgres.js both surface SQLSTATE on
 * thrown errors as `.code` (string). Bun.sql follows the same convention.
 * This helper keeps the SQLSTATE constant centralised.
 */
export function isDcbViolation(err: unknown): boolean {
  return (
    typeof err === "object" &&
    err !== null &&
    "code" in err &&
    (err as { code: unknown }).code === KRONOS_DCB_VIOLATION_SQLSTATE
  )
}

/**
 * SQLSTATEs that mean the server could not serve the statement now: it is
 * shutting down or not yet accepting connections (57P01–57P03), out of
 * connections (53300), or a timeout this package's session settings armed
 * fired (statement 57014, lock 55P03, idle in transaction 25P03). Class 08,
 * connection exceptions, counts as a whole.
 */
const UNAVAILABLE_SQLSTATES = new Set(["57P01", "57P02", "57P03", "53300", "57014", "55P03", "25P03"])

/**
 * Codes the drivers and the socket layer put on a connection that could not be
 * made or was lost: the operating system's, postgres.js's and Bun.SQL's.
 */
const UNAVAILABLE_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ECONNABORTED",
  "ETIMEDOUT",
  "EPIPE",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "EAI_AGAIN",
  "CONNECT_TIMEOUT",
  "CONNECTION_CLOSED",
  "CONNECTION_ENDED",
  "CONNECTION_DESTROYED",
  "ERR_POSTGRES_CONNECTION_CLOSED",
  "ERR_POSTGRES_CONNECTION_REFUSED",
  "ERR_POSTGRES_CONNECTION_TIMEOUT",
  "ERR_POSTGRES_IDLE_TIMEOUT",
  "ERR_POSTGRES_LIFETIME_TIMEOUT",
])

/** What `pg` says, with no code, when it could not get or keep a connection. */
const UNAVAILABLE_PG_MESSAGES = [
  /timeout exceeded when trying to connect/i,
  /Connection terminated/i,
  /Client has encountered a connection error/i,
]

function connectionLost(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false
  const code = (error as { code?: unknown }).code
  if (typeof code === "string" && (UNAVAILABLE_CODES.has(code) || UNAVAILABLE_SQLSTATES.has(code) || /^08[0-9A-Z]{3}$/.test(code))) {
    return true
  }
  if (error instanceof AggregateError && error.errors.some(connectionLost)) return true
  return error instanceof Error && typeof code !== "string" && UNAVAILABLE_PG_MESSAGES.some((m) => m.test(error.message))
}

/**
 * Gives a driver error that means "the server could not serve this now" core's
 * `unavailable` kind, so `isUnavailable` recognises it, and returns it. The
 * error is marked IN PLACE, not wrapped, so its SQLSTATE stays on `.code`
 * for `isDcbViolation` and every other reader, and an error that already
 * carries a kind keeps it.
 */
export function markUnavailable(error: unknown): unknown {
  if (connectionLost(error) && (error as { kind?: unknown }).kind === undefined) {
    ;(error as { kind?: string }).kind = "unavailable"
  }
  return error
}
