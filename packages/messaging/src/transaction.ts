import { resourceKey, type ResourceKey } from "@kronos-ts/common"
import {
  processingStateStorage,
  setResource,
  on,
  onError,
  Phase,
} from "./processing-state.js"
import type { UoWRunner } from "./unit-of-work.js"

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
 * Wraps a delegate runner with transaction management. The transaction is:
 * - Started before the delegate's action runs
 * - Available via `getActiveTransaction()` throughout
 * - Committed in the COMMIT phase
 * - Rolled back on error
 *
 * The transaction is stored as a resource on the active UoW's ALS state,
 * so all phases (PRE_INVOCATION through AFTER_COMMIT) and any code calling
 * `getActiveTransaction()` inside the UoW see it.
 *
 * ```
 * const txRunner = transactionalUnitOfWorkFactory(runInUoW, myTransactionManager)
 * await txRunner(metadata, async () => { ... })
 * ```
 *
 * Plan 03-04 (CTX-04 / D-34): rewritten as a composable runner wrapper.
 * Previously took/returned `UnitOfWorkFactory`; the UoW interface and
 * factory are gone, so this now takes/returns `UoWRunner`. The name is
 * preserved despite the shape change — public docs and extension code
 * import `transactionalUnitOfWorkFactory` by name. Phase 9 (Extension
 * Migration) can rename if the kronos() app API warrants.
 */
export function transactionalUnitOfWorkFactory<T>(
  delegate: UoWRunner,
  txManager: TransactionManager<T>,
): UoWRunner {
  return async (metadata, action) => {
    const tx = await txManager.begin()
    return delegate(metadata, async () => {
      setResource(TRANSACTION_KEY, tx)
      on(Phase.COMMIT, async () => {
        await txManager.commit(tx)
      })
      onError(async () => {
        await txManager.rollback(tx)
      })
      return action()
    })
  }
}
