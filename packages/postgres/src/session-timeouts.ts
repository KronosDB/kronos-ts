/**
 * Per-transaction safety timeouts, armed via `SET LOCAL` by each adapter's
 * `transaction()` at BEGIN. Living on the adapter (not the transaction
 * manager) means EVERY postgres transaction is bounded — UoW-scoped commits,
 * the event store's own-tx append/publish, and the scheduler worker tick alike
 * — and each adapter instance carries its own settings, so two adapters
 * pointed at two different databases stay fully decoupled.
 *
 * A non-postgres adapter (e.g. a future sqlite one) arms its own
 * dialect-appropriate settings, or none.
 */

import type { PostgresAdapterTransaction } from "./adapter.js"

export type SessionTimeoutOptions = {
  /**
   * `idle_in_transaction_session_timeout` (ms) applied via `SET LOCAL` on every
   * transaction. A transaction that begins but stalls before commit/rollback
   * would otherwise hold its connection — and pin `pg_snapshot_xmin`, which
   * gates the gap-free tailing query in the event store — open indefinitely,
   * stalling all streaming processors until the process restarts. This bounds
   * that window: postgres aborts the idle transaction and the connection (and
   * xmin) is freed. Default 30000 (30s). Set 0 to disable (postgres default).
   */
  readonly idleInTransactionTimeoutMs?: number
  /**
   * `statement_timeout` (ms) applied via `SET LOCAL` on every transaction.
   * Bounds a single hung statement inside the tx. Default 0 (disabled) — large
   * appends / replays can legitimately run long, so opt in per deployment.
   */
  readonly statementTimeoutMs?: number
}

export type ResolvedSessionTimeouts = {
  readonly idleInTransactionTimeoutMs: number
  readonly statementTimeoutMs: number
}

const DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS = 30_000

/** Resolve config to concrete, normalized timeouts (applying defaults). */
export function resolveSessionTimeouts(opts?: SessionTimeoutOptions): ResolvedSessionTimeouts {
  return {
    idleInTransactionTimeoutMs: normalizeTimeoutMs(
      opts?.idleInTransactionTimeoutMs ?? DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS,
    ),
    statementTimeoutMs: normalizeTimeoutMs(opts?.statementTimeoutMs ?? 0),
  }
}

/**
 * Arm the resolved timeouts on a freshly-opened transaction. No-op for any
 * timeout resolved to 0.
 *
 * GUCs cannot be parameterized ($1) — the value is a config-supplied integer,
 * normalized to a non-negative whole number, so inlining is injection-safe.
 * `SET LOCAL` auto-resets at COMMIT/ROLLBACK, so it never leaks onto pooled
 * connections.
 */
export async function applySessionTimeouts(
  tx: PostgresAdapterTransaction,
  timeouts: ResolvedSessionTimeouts,
): Promise<void> {
  if (timeouts.idleInTransactionTimeoutMs > 0) {
    await tx.query(`SET LOCAL idle_in_transaction_session_timeout = ${timeouts.idleInTransactionTimeoutMs}`)
  }
  if (timeouts.statementTimeoutMs > 0) {
    await tx.query(`SET LOCAL statement_timeout = ${timeouts.statementTimeoutMs}`)
  }
}

/** Coerce a config timeout to a non-negative whole number of milliseconds.
 *  Non-finite or negative values disable the timeout (treated as 0). */
function normalizeTimeoutMs(value: number): number {
  if (!Number.isFinite(value) || value <= 0) return 0
  return Math.floor(value)
}
