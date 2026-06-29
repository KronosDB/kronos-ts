import type { TransactionManager } from "@kronos-ts/messaging"

/**
 * A Drizzle database instance that supports transactions.
 * Works with any Drizzle driver (postgres-js, node-postgres, better-sqlite3, etc.).
 */
export interface DrizzleDatabaseLike {
  transaction<T>(fn: (tx: any) => Promise<T>): Promise<T>
}

/**
 * The Drizzle transaction object — has the same query API as the database.
 */
export type DrizzleTransaction = any

export interface DrizzleTransactionManagerOptions {
  /**
   * Runs once on every transaction, right after it opens and before the UoW
   * gets the handle. This is where a Postgres-backed deployment arms session
   * GUCs — chiefly `idle_in_transaction_session_timeout` — so a stalled UoW
   * (e.g. a hung dead-letter drain whose replay never returns) is aborted by
   * the database instead of pinning a connection indefinitely. The Postgres
   * adapter does this for its own transactions; this hook is the equivalent
   * seam for the (DB-agnostic) Drizzle/Knex path. No-op by default.
   *
   * Example (postgres-js driver):
   * ```ts
   * drizzleTransactionManager(db, {
   *   onBeginTransaction: (tx) => tx.execute(sql.raw(
   *     "SET LOCAL idle_in_transaction_session_timeout = 30000")),
   * })
   * ```
   */
  readonly onBeginTransaction?: (tx: DrizzleTransaction) => Promise<void>
}

/**
 * Creates a TransactionManager for Drizzle ORM.
 *
 * Drizzle's `db.transaction()` uses a callback pattern. This manager
 * bridges it to the framework's `begin/commit/rollback` lifecycle
 * using deferred promises — the same pattern as the Prisma extension.
 *
 * The `tx` is made available via `getActiveTransaction()` so projections
 * and token stores participate in the same transaction.
 *
 * ```typescript
 * import { drizzle } from "drizzle-orm/postgres-js"
 * import { drizzleTransactionManager } from "@kronos-ts/drizzle"
 *
 * const db = drizzle(sql)
 *
 * // transactionManager wiring to a kronos() App is pending a typed
 * // `transactionManager` slot (Phase 9). For now, construct the manager
 * // and pass it directly into the unitOfWorkFactory composition:
 * const txManager = drizzleTransactionManager(db)
 * ```
 */
export function drizzleTransactionManager(
  db: DrizzleDatabaseLike,
  options: DrizzleTransactionManagerOptions = {},
): TransactionManager<DrizzleTransaction> {
  const { onBeginTransaction } = options
  return {
    async begin(): Promise<DrizzleTransaction> {
      let resolveTx!: (tx: DrizzleTransaction) => void
      let rejectTx!: (error: unknown) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<DrizzleTransaction>((resolve, reject) => {
        resolveTx = resolve
        rejectTx = reject
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = db.transaction(async (tx) => {
        // Arm session settings (e.g. idle-in-transaction timeout) before the
        // UoW gets the handle, so begin() only resolves once the tx is bounded.
        if (onBeginTransaction) {
          try {
            await onBeginTransaction(tx)
          } catch (err) {
            // Surface as a begin() rejection rather than hanging on txReady.
            rejectTx(err)
            throw err
          }
        }
        resolveTx(tx)
        await completionSignal
      })
      // If db.transaction() rejects before onBeginTransaction runs (e.g. the
      // pool can't hand out a connection), make begin() reject instead of
      // hanging on txReady forever.
      txPromise.catch(rejectTx)

      const tx = await txReady
      ;(tx as any).__kronos_commit = resolveCompletion
      ;(tx as any).__kronos_rollback = rejectCompletion
      ;(tx as any).__kronos_txPromise = txPromise

      return tx
    },

    async commit(tx: DrizzleTransaction): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: DrizzleTransaction): Promise<void> {
      const rollback = (tx as any).__kronos_rollback as (error: unknown) => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      rollback(new Error("Transaction rolled back"))
      try { await txPromise } catch { /* expected */ }
    },
  }
}
