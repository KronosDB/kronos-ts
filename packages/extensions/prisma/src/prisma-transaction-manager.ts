import type { TransactionManager } from "@kronos-ts/messaging"

/**
 * The Prisma transaction client — the `tx` parameter inside `$transaction()`.
 * This is a generic type since the actual PrismaClient type depends on the
 * user's schema. Any Prisma client with `$transaction` works.
 */
export type PrismaTransactionClient = {
  // Marker type — the actual tx client has all model methods
  [key: string]: any
}

/**
 * A Prisma client that supports interactive transactions.
 */
export interface PrismaClientLike {
  $transaction<T>(fn: (tx: PrismaTransactionClient) => Promise<T>, options?: { timeout?: number }): Promise<T>
}

/**
 * Creates a TransactionManager for Prisma's interactive transactions.
 *
 * Prisma's `$transaction()` uses a callback pattern — the transaction
 * client (`tx`) is only available inside the callback. This manager
 * bridges that to the framework's `begin/commit/rollback` lifecycle
 * using deferred promises.
 *
 * The `tx` client is made available via `getActiveTransaction()` so
 * projections and token stores can participate in the same transaction.
 *
 * ```typescript
 * import { PrismaClient } from "@prisma/client"
 * import { prismaTransactionManager } from "@kronos-ts/extensions-prisma"
 *
 * const prisma = new PrismaClient()
 *
 * EventSourcingConfigurer.create()
 *   .componentRegistry(cr => {
 *     cr.register(ComponentKeys.TRANSACTION_MANAGER,
 *       () => prismaTransactionManager(prisma))
 *   })
 * ```
 */
export function prismaTransactionManager(
  prisma: PrismaClientLike,
  options?: { timeoutMs?: number },
): TransactionManager<PrismaTransactionClient> {
  return {
    async begin(): Promise<PrismaTransactionClient> {
      // Create deferred promises to coordinate the transaction lifecycle.
      // The $transaction callback receives the tx client and waits for
      // either commit (resolve) or rollback (reject).
      let resolveTx!: (tx: PrismaTransactionClient) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<PrismaTransactionClient>((resolve) => {
        resolveTx = resolve
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      // Start the transaction in the background — it will wait for
      // the completion signal before committing or rolling back.
      const txPromise = prisma.$transaction(async (tx) => {
        resolveTx(tx)
        await completionSignal
      }, { timeout: options?.timeoutMs })

      // Capture the completion handlers on the tx client so commit/rollback
      // can signal from outside the callback.
      const tx = await txReady
      ;(tx as any).__kronos_commit = resolveCompletion
      ;(tx as any).__kronos_rollback = rejectCompletion
      ;(tx as any).__kronos_txPromise = txPromise

      return tx
    },

    async commit(tx: PrismaTransactionClient): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: PrismaTransactionClient): Promise<void> {
      const rollback = (tx as any).__kronos_rollback as (error: unknown) => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      rollback(new Error("Transaction rolled back"))
      // Swallow the expected rejection from the $transaction promise
      try { await txPromise } catch { /* expected */ }
    },
  }
}
