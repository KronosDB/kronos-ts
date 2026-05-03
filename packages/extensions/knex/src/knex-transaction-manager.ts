import type { TransactionManager } from "@kronos-ts/messaging"

/**
 * A Knex instance that supports transactions.
 */
export interface KnexInstanceLike {
  transaction<T>(fn: (trx: any) => Promise<T>): Promise<T>
}

/**
 * The Knex transaction object.
 */
export type KnexTransaction = any

/**
 * Creates a TransactionManager for Knex.
 *
 * Bridges Knex's `knex.transaction(fn)` callback to the framework's
 * `begin/commit/rollback` lifecycle.
 *
 * ```typescript
 * import Knex from "knex"
 * import { knexTransactionManager } from "@kronos-ts/extensions/knex"
 *
 * const knex = Knex({ client: "pg", connection: "..." })
 *
 * // transactionManager wiring to a kronos() App is pending a typed
 * // `transactionManager` slot (Phase 9). For now, construct the manager
 * // and pass it directly into the unitOfWorkFactory composition:
 * const txManager = knexTransactionManager(knex)
 * ```
 */
export function knexTransactionManager(
  knex: KnexInstanceLike,
): TransactionManager<KnexTransaction> {
  return {
    async begin(): Promise<KnexTransaction> {
      let resolveTx!: (tx: KnexTransaction) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<KnexTransaction>((resolve) => {
        resolveTx = resolve
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = knex.transaction(async (trx) => {
        resolveTx(trx)
        await completionSignal
      })

      const tx = await txReady
      ;(tx as any).__kronos_commit = resolveCompletion
      ;(tx as any).__kronos_rollback = rejectCompletion
      ;(tx as any).__kronos_txPromise = txPromise

      return tx
    },

    async commit(tx: KnexTransaction): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: KnexTransaction): Promise<void> {
      const rollback = (tx as any).__kronos_rollback as (error: unknown) => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      rollback(new Error("Transaction rolled back"))
      try { await txPromise } catch { /* expected */ }
    },
  }
}
