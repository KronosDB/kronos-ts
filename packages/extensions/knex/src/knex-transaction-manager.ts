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

export interface KnexTransactionManagerOptions {
  /**
   * Runs once on every transaction, right after it opens and before the UoW
   * gets the handle. This is where a Postgres-backed deployment arms session
   * GUCs — chiefly `idle_in_transaction_session_timeout` — so a stalled UoW
   * (e.g. a hung dead-letter drain whose replay never returns) is aborted by
   * the database instead of pinning a connection indefinitely. The Postgres
   * adapter does this for its own transactions; this hook is the equivalent
   * seam for the (DB-agnostic) Knex/Drizzle path. No-op by default.
   *
   * Example (pg client):
   * ```ts
   * knexTransactionManager(knex, {
   *   onBeginTransaction: (trx) => trx.raw(
   *     "SET LOCAL idle_in_transaction_session_timeout = 30000"),
   * })
   * ```
   */
  readonly onBeginTransaction?: (tx: KnexTransaction) => Promise<void>
}

/**
 * Creates a TransactionManager for Knex.
 *
 * Bridges Knex's `knex.transaction(fn)` callback to the framework's
 * `begin/commit/rollback` lifecycle.
 *
 * ```typescript
 * import Knex from "knex"
 * import { knexTransactionManager } from "@kronos-ts/knex"
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
  options: KnexTransactionManagerOptions = {},
): TransactionManager<KnexTransaction> {
  const { onBeginTransaction } = options
  return {
    async begin(): Promise<KnexTransaction> {
      let resolveTx!: (tx: KnexTransaction) => void
      let rejectTx!: (error: unknown) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<KnexTransaction>((resolve, reject) => {
        resolveTx = resolve
        rejectTx = reject
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = knex.transaction(async (trx) => {
        // Arm session settings (e.g. idle-in-transaction timeout) before the
        // UoW gets the handle, so begin() only resolves once the tx is bounded.
        if (onBeginTransaction) {
          try {
            await onBeginTransaction(trx)
          } catch (err) {
            rejectTx(err)
            throw err
          }
        }
        resolveTx(trx)
        await completionSignal
      })
      // If knex.transaction() rejects before onBeginTransaction runs (e.g. the
      // pool can't hand out a connection), make begin() reject instead of
      // hanging on txReady forever.
      txPromise.catch(rejectTx)

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
