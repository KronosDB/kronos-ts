import type { TransactionManager } from "@kronos-ts/messaging"

/**
 * TypeORM DataSource with transaction support.
 */
export interface TypeOrmDataSourceLike {
  transaction<T>(fn: (entityManager: any) => Promise<T>): Promise<T>
}

/**
 * The TypeORM EntityManager inside a transaction.
 */
export type TypeOrmTransaction = any

/**
 * Creates a TransactionManager for TypeORM.
 *
 * Bridges TypeORM's `dataSource.transaction(fn)` callback to the
 * framework's `begin/commit/rollback` lifecycle.
 *
 * ```typescript
 * import { typeormTransactionManager } from "@kronos-ts/extensions/typeorm"
 *
 * configurer.componentRegistry(cr => {
 *   cr.register(ComponentKeys.TRANSACTION_MANAGER,
 *     () => typeormTransactionManager(dataSource))
 * })
 * ```
 */
export function typeormTransactionManager(
  dataSource: TypeOrmDataSourceLike,
): TransactionManager<TypeOrmTransaction> {
  return {
    async begin(): Promise<TypeOrmTransaction> {
      let resolveTx!: (tx: TypeOrmTransaction) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<TypeOrmTransaction>((resolve) => {
        resolveTx = resolve
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = dataSource.transaction(async (entityManager) => {
        resolveTx(entityManager)
        await completionSignal
      })

      const tx = await txReady
      ;(tx as any).__kronos_commit = resolveCompletion
      ;(tx as any).__kronos_rollback = rejectCompletion
      ;(tx as any).__kronos_txPromise = txPromise

      return tx
    },

    async commit(tx: TypeOrmTransaction): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: TypeOrmTransaction): Promise<void> {
      const rollback = (tx as any).__kronos_rollback as (error: unknown) => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      rollback(new Error("Transaction rolled back"))
      try { await txPromise } catch { /* expected */ }
    },
  }
}
