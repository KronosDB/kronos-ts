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
): TransactionManager<DrizzleTransaction> {
  return {
    async begin(): Promise<DrizzleTransaction> {
      let resolveTx!: (tx: DrizzleTransaction) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<DrizzleTransaction>((resolve) => {
        resolveTx = resolve
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = db.transaction(async (tx) => {
        resolveTx(tx)
        await completionSignal
      })

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
