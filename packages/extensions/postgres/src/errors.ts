/**
 * SQLSTATE used by the schema-bootstrap stored procedure when a DCB
 * append condition is violated. Per D-12.12: dedicated SQLSTATE via
 * `RAISE ... USING ERRCODE`, never error-text parsing.
 *
 * `KR001` lives in the Postgres user-defined SQLSTATE range (KX–ZZ).
 * It is intentionally distinct from:
 *   - `P0001` — generic RAISE (would over-match any unhandled plpgsql exception)
 *   - `23505` — unique_violation (used by primary key conflicts)
 * Adapter layers translate `err.code === KR001` into AppendConditionError.
 */
export const KRONOS_DCB_VIOLATION_SQLSTATE = "KR001"

/**
 * Thrown when an append condition is violated — optimistic concurrency
 * failure. Structurally mirrors `@kronos-ts/eventsourcing`'s
 * AppendConditionError so callers that catch either get equivalent
 * behaviour, but we ship our own class so that the SQLSTATE-catch
 * boundary lives inside this package's import graph.
 */
export class AppendConditionError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "AppendConditionError"
  }

  static fromConflictCount(count: number, afterPosition: bigint): AppendConditionError {
    return new AppendConditionError(
      `Append condition violated: ${count} conflicting event(s) ` +
        `found after position ${afterPosition}`,
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
