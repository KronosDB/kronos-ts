import { AsyncLocalStorage } from "node:async_hooks"
import { resourceKey, type ResourceKey } from "@kronos-ts/common"
import { Phase, type ProcessingContext } from "./processing-context.js"
import type { UnitOfWork, UnitOfWorkFactory } from "./unit-of-work.js"

/**
 * Manages transaction lifecycle. Users provide an implementation
 * for their specific database/ORM.
 *
 * The framework calls begin/commit/rollback — users never touch these directly.
 */
export interface TransactionManager<T = unknown> {
  begin(): Promise<T>
  commit(tx: T): Promise<void>
  rollback(tx: T): Promise<void>
}

/**
 * A no-op transaction manager for when no database transactions are needed.
 */
export function noTransactionManager(): TransactionManager<void> {
  return {
    begin: async () => {},
    commit: async () => {},
    rollback: async () => {},
  }
}

// AsyncLocalStorage for transparent transaction propagation
const transactionStorage = new AsyncLocalStorage<unknown>()

/** Resource key for storing the active transaction in a ProcessingContext. */
export const TRANSACTION_KEY: ResourceKey<unknown> = resourceKey("transaction")

/**
 * Get the active transaction from AsyncLocalStorage.
 * Returns undefined if no transaction is active.
 *
 * ORM integrations use this to participate in the framework's transaction:
 * ```
 * const db = drizzle(pool, {
 *   transaction: () => getActiveTransaction(),
 * })
 * ```
 */
export function getActiveTransaction<T = unknown>(): T | undefined {
  return transactionStorage.getStore() as T | undefined
}

/**
 * Run a function within a transaction context.
 * The transaction is available via getActiveTransaction() and
 * on the ProcessingContext for explicit access.
 */
export async function runInTransaction<T, R>(
  txManager: TransactionManager<T>,
  fn: (tx: T) => Promise<R>,
): Promise<R> {
  const tx = await txManager.begin()
  try {
    const result = await transactionStorage.run(tx, () => fn(tx))
    await txManager.commit(tx)
    return result
  } catch (err) {
    await txManager.rollback(tx)
    throw err
  }
}

/**
 * Creates a UnitOfWorkFactory that wraps each UnitOfWork with transaction
 * management. The transaction is:
 * - Started before phase execution
 * - Available via `getActiveTransaction()` and `ctx.get(TRANSACTION_KEY)` throughout
 * - Committed in the COMMIT phase
 * - Rolled back on error
 *
 * All phases (PRE_INVOCATION through AFTER_COMMIT) execute within the
 * transaction's AsyncLocalStorage context, so ORMs pick it up transparently.
 *
 * ```
 * const txUowFactory = transactionalUnitOfWorkFactory(
 *   defaultUnitOfWorkFactory(),
 *   myTransactionManager,
 * )
 * ```
 */
export function transactionalUnitOfWorkFactory<T>(
  delegate: UnitOfWorkFactory,
  txManager: TransactionManager<T>,
): UnitOfWorkFactory {
  return (metadata) => {
    const inner = delegate(metadata)

    // Proxy that wraps executeWithResult in a transaction context
    const wrapper: UnitOfWork = {
      on: (phase, action) => inner.on(phase, action),
      onError: (handler) => inner.onError(handler),
      whenComplete: (handler) => inner.whenComplete(handler),

      async executeWithResult<R>(action: (ctx: ProcessingContext) => Promise<R>): Promise<R> {
        const tx = await txManager.begin()

        // Register commit/rollback on the inner UnitOfWork
        inner.on(Phase.COMMIT, async () => {
          await txManager.commit(tx)
        })
        inner.onError(async () => {
          await txManager.rollback(tx)
        })

        // Execute within AsyncLocalStorage context so getActiveTransaction() works
        return transactionStorage.run(tx, () =>
          inner.executeWithResult(async (ctx) => {
            // Store transaction in ProcessingContext for explicit access
            ctx.set(TRANSACTION_KEY as ResourceKey<T>, tx)
            return action(ctx)
          }),
        )
      },
    }

    return wrapper
  }
}
