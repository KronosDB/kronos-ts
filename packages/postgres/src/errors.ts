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
