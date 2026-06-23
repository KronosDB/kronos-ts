/**
 * postgresTransactionManager — bridges the framework's TransactionManager
 * lifecycle to the postgres adapter's callback-shaped `adapter.transaction`.
 *
 * adapter.transaction(IL, fn) opens a pg tx, runs `fn(tx)`, and COMMITs on
 * fn-resolve / ROLLBACKs on fn-reject. The framework needs a tx whose
 * commit/rollback is callable LATER (at the UoW's COMMIT or onError phase),
 * not when fn returns. We bridge by parking `fn` on a deferred completion
 * promise — `commit()` resolves it (→ adapter commits), `rollback()`
 * rejects it (→ adapter rolls back).
 *
 * Same shape as kysely/prisma transaction managers in this repo.
 */

import { getOrBeginActiveTransaction, type TransactionManager } from "@kronos-ts/messaging"
import type { PostgresAdapter, PostgresAdapterTransaction } from "./adapter.js"
import { IsolationLevel } from "./adapter.js"

/**
 * Module-private symbol attaching commit/rollback control to a tx handle.
 * Consumers via `getActiveTransaction<PostgresAdapterTransaction>()` see
 * only `{ query }` — they cannot read this without importing the symbol.
 */
const TX_CONTROL = Symbol("kronos.postgresTxControl")

interface TxControl {
  readonly resolveCommit: () => void
  readonly rejectRollback: (err: unknown) => void
  readonly txPromise: Promise<void>
}

interface ManagedPostgresTransaction extends PostgresAdapterTransaction {
  [TX_CONTROL]: TxControl
}

/** Marker error: signals an intentional rollback so the .catch can suppress it. */
const ROLLBACK_MARKER = "__kronos_postgres_tx_rollback__"

/** Tuning for the safety timeouts applied to every UoW-scoped transaction. */
export interface PostgresTransactionManagerOptions {
  /**
   * `idle_in_transaction_session_timeout` (ms) applied via `SET LOCAL` on every
   * transaction. A UoW that begins a tx but stalls before commit/rollback would
   * otherwise hold its connection — and pin `pg_snapshot_xmin`, which gates the
   * gap-free tailing query in the event store — open indefinitely, stalling all
   * streaming processors until the process restarts. This bounds that window:
   * postgres aborts the idle transaction and the connection (and xmin) is freed.
   * Default 30000 (30s). Set 0 to disable (postgres default — no timeout).
   */
  readonly idleInTransactionTimeoutMs?: number
  /**
   * `statement_timeout` (ms) applied via `SET LOCAL` on every transaction.
   * Bounds a single hung statement inside the tx. Default 0 (disabled) — large
   * appends / replays can legitimately run long, so opt in per deployment.
   */
  readonly statementTimeoutMs?: number
}

const DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS = 30_000

export function postgresTransactionManager(
  adapter: PostgresAdapter,
  isolationLevel: IsolationLevel = IsolationLevel.READ_COMMITTED,
  options: PostgresTransactionManagerOptions = {},
): TransactionManager<PostgresAdapterTransaction> {
  const idleTimeoutMs = normalizeTimeoutMs(
    options.idleInTransactionTimeoutMs ?? DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS,
  )
  const statementTimeoutMs = normalizeTimeoutMs(options.statementTimeoutMs ?? 0)

  // GUCs cannot be parameterized ($1) — the value is a config-supplied integer,
  // normalized to a non-negative whole number, so inlining is injection-safe.
  // SET LOCAL auto-resets at COMMIT/ROLLBACK, so it never leaks onto pooled
  // connections.
  async function applyTimeouts(tx: PostgresAdapterTransaction): Promise<void> {
    if (idleTimeoutMs > 0) {
      await tx.query(`SET LOCAL idle_in_transaction_session_timeout = ${idleTimeoutMs}`)
    }
    if (statementTimeoutMs > 0) {
      await tx.query(`SET LOCAL statement_timeout = ${statementTimeoutMs}`)
    }
  }

  return {
    async begin(): Promise<PostgresAdapterTransaction> {
      let captureTx!: (tx: PostgresAdapterTransaction) => void
      const txReady = new Promise<PostgresAdapterTransaction>((res) => {
        captureTx = res
      })

      let resolveCommit!: () => void
      let rejectRollback!: (err: unknown) => void
      const completion = new Promise<void>((res, rej) => {
        resolveCommit = res
        rejectRollback = rej
      })

      const txPromise = adapter
        .transaction(isolationLevel, async (tx) => {
          // Arm the per-transaction safety timeouts before handing the tx to
          // the UoW, so even the very first awaited statement is bounded.
          await applyTimeouts(tx)
          captureTx(tx)
          await completion
        })
        .then(
          () => undefined,
          (err) => {
            // Suppress the marker — rollback is an expected outcome.
            if (err instanceof Error && err.message === ROLLBACK_MARKER) return
            throw err
          },
        )

      // If the transaction callback fails before it hands back the tx — BEGIN
      // itself failing, or arming the safety timeouts throwing — `captureTx`
      // never runs and `txReady` would never resolve. Race it against
      // `txPromise` so an early failure rejects begin() instead of hanging it
      // forever. In the happy path `txPromise` stays pending (parked on
      // `completion` until commit/rollback), so `txReady` always wins.
      const tx = (await Promise.race([txReady, txPromise])) as
        | ManagedPostgresTransaction
        | undefined
      if (tx === undefined) {
        // txPromise settled first by resolving — the tx ended before begin()
        // returned, so the handle is unusable. Surface rather than return it.
        throw new Error(
          "postgresTransactionManager: transaction ended before begin() completed",
        )
      }
      tx[TX_CONTROL] = { resolveCommit, rejectRollback, txPromise }
      return tx
    },

    async commit(tx: PostgresAdapterTransaction): Promise<void> {
      const ctrl = (tx as ManagedPostgresTransaction)[TX_CONTROL]
      ctrl.resolveCommit()
      await ctrl.txPromise
    },

    async rollback(tx: PostgresAdapterTransaction): Promise<void> {
      const ctrl = (tx as ManagedPostgresTransaction)[TX_CONTROL]
      ctrl.rejectRollback(new Error(ROLLBACK_MARKER))
      try {
        await ctrl.txPromise
      } catch (err) {
        // A real follow-up error during ROLLBACK execution. Don't throw
        // from rollback() — the UoW is already in an error path and a
        // cascading throw masks the original failure.
        console.warn("postgresTransactionManager: rollback path threw:", err)
      }
    },
  }
}

/** Coerce a config timeout to a non-negative whole number of milliseconds.
 *  Non-finite or negative values disable the timeout (treated as 0). */
function normalizeTimeoutMs(value: number): number {
  if (!Number.isFinite(value) || value <= 0) return 0
  return Math.floor(value)
}

/**
 * Run `fn` inside a postgres tx, joining a UoW-scoped tx if one is active
 * (or installed lazily), otherwise opening an ad-hoc tx via `adapter.transaction`.
 *
 * This is what every postgres-extension write path should funnel through —
 * event store appends, snapshot writes, scheduler inserts — so that all
 * writes inside a single UoW land in one pg tx and commit/roll back atomically.
 * Calls from outside any UoW (e.g., the scheduler worker loop, projection
 * queries, lifecycle bootstraps) get their own short-lived tx.
 *
 * Returns whatever `fn` returns. Tx commit/rollback happens when the
 * surrounding UoW's COMMIT/onError fires (joined path) or when `fn` resolves/
 * rejects (ad-hoc path).
 */
export async function withSharedOrOwnTx<R>(
  adapter: PostgresAdapter,
  fn: (tx: PostgresAdapterTransaction) => Promise<R>,
  isolationLevel: IsolationLevel = IsolationLevel.READ_COMMITTED,
): Promise<R> {
  const shared = await getOrBeginActiveTransaction<PostgresAdapterTransaction>()
  if (shared !== undefined) return fn(shared)
  return adapter.transaction(isolationLevel, fn)
}

