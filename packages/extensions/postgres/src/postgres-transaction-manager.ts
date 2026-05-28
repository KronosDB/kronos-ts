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

export function postgresTransactionManager(
  adapter: PostgresAdapter,
  isolationLevel: IsolationLevel = IsolationLevel.READ_COMMITTED,
): TransactionManager<PostgresAdapterTransaction> {
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

      const tx = (await txReady) as ManagedPostgresTransaction
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

