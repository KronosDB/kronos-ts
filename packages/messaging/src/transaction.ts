import { resourceKey, type ResourceKey } from "@kronos-ts/common"
import { processingStateStorage, setResource } from "./processing-state.js"
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

/** Resource key for storing the active transaction in the active UnitOfWork's ALS-backed resources. */
export const TRANSACTION_KEY: ResourceKey<unknown> = resourceKey("transaction")

/**
 * Get the active transaction from the active UnitOfWork's ALS-backed resources.
 * Returns undefined if no UnitOfWork is active or no transaction has been stored.
 *
 * ORM integrations use this to participate in the framework's transaction:
 * ```
 * const db = drizzle(pool, {
 *   transaction: () => getActiveTransaction(),
 * })
 * ```
 *
 * Permissive undefined-return preserved (D-12 / D-23): callers outside a UoW
 * get `undefined`, NOT a NoActiveUnitOfWork throw — this is an ORM escape hatch,
 * not a framework-internal accessor.
 */
export function getActiveTransaction<T = unknown>(): T | undefined {
  const state = processingStateStorage.getStore()
  if (!state) return undefined
  return state.resources.get(TRANSACTION_KEY.symbol) as T | undefined
}

/**
 * Creates a UnitOfWorkFactory that wraps each UnitOfWork with transaction
 * management. The transaction is:
 * - Started before phase execution
 * - Available via `getActiveTransaction()` and `ctx.get(TRANSACTION_KEY)` throughout
 * - Committed in the COMMIT phase
 * - Rolled back on error
 *
 * The transaction is stored as a resource on the active UoW's ALS state,
 * so all phases (PRE_INVOCATION through AFTER_COMMIT) and any code calling
 * `getActiveTransaction()` inside the UoW see it.
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

        // Inner UoW's executeWithResult enters processingStateStorage.run (Phase 1 wiring),
        // so setResource inside the action callback writes to the active ALS state.
        // D-22: no outer ALS wrap around the inner UoW — a single ALS boundary.
        return inner.executeWithResult(async (ctx) => {
          setResource(TRANSACTION_KEY, tx)
          return action(ctx)
        })
      },
    }

    return wrapper
  }
}
