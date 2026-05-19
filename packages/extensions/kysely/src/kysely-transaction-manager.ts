import type { TransactionManager } from "@kronos-ts/messaging"

/**
 * A Kysely database instance that supports transactions.
 */
export interface KyselyDatabaseLike {
  transaction(): { execute<T>(fn: (trx: any) => Promise<T>): Promise<T> }
}

/**
 * The Kysely transaction object.
 */
export type KyselyTransaction = any

/**
 * Creates a TransactionManager for Kysely.
 *
 * Bridges Kysely's `db.transaction().execute(fn)` callback pattern
 * to the framework's `begin/commit/rollback` lifecycle.
 *
 * ```typescript
 * import { Kysely } from "kysely"
 * import { kyselyTransactionManager } from "@kronos-ts/kysely"
 *
 * // transactionManager wiring to a kronos() App is pending a typed
 * // `transactionManager` slot (Phase 9). For now, construct the manager
 * // and pass it directly into the unitOfWorkFactory composition:
 * const txManager = kyselyTransactionManager(db)
 * ```
 */
export function kyselyTransactionManager(
  db: KyselyDatabaseLike,
): TransactionManager<KyselyTransaction> {
  return {
    async begin(): Promise<KyselyTransaction> {
      let resolveTx!: (tx: KyselyTransaction) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<KyselyTransaction>((resolve) => {
        resolveTx = resolve
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = db.transaction().execute(async (trx) => {
        resolveTx(trx)
        await completionSignal
      })

      const tx = await txReady
      ;(tx as any).__kronos_commit = resolveCompletion
      ;(tx as any).__kronos_rollback = rejectCompletion
      ;(tx as any).__kronos_txPromise = txPromise

      return tx
    },

    async commit(tx: KyselyTransaction): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: KyselyTransaction): Promise<void> {
      const rollback = (tx as any).__kronos_rollback as (error: unknown) => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      rollback(new Error("Transaction rolled back"))
      try { await txPromise } catch { /* expected */ }
    },
  }
}
