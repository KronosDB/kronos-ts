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
 * Resource key holding a deferred-begin factory installed by
 * {@link lazyTransactionalUnitOfWorkFactory}. The factory begins the tx on
 * first call, registers commit/rollback hooks on the UoW, caches the
 * resulting tx in {@link TRANSACTION_KEY}, and returns it. Subsequent calls
 * return the cached tx without a re-begin.
 *
 * NOT exported from the package barrel — components reach the lazily-begun
 * tx through {@link getOrBeginActiveTransaction}.
 */
const LAZY_TX_FACTORY_KEY: ResourceKey<() => Promise<unknown>> = resourceKey("lazyTxFactory")

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

/**
 * Lazy variant of {@link transactionalUnitOfWorkFactory}.
 *
 * Unlike the eager factory, no transaction is begun on UoW entry. Instead,
 * a factory is installed in the UoW that opens the tx on the first call to
 * {@link getOrBeginActiveTransaction}. Pure-read UoWs that never request a
 * tx pay zero begin/commit cost and never claim a connection from the pool.
 *
 * On first request: the tx is begun, stored in {@link TRANSACTION_KEY},
 * and commit/rollback hooks are registered. Subsequent requests within
 * the same UoW return the cached tx — there is exactly one tx per UoW.
 *
 * Components that may write to the underlying store (event stores,
 * schedulers, ORM integrations) reach the tx via
 * {@link getOrBeginActiveTransaction}; read-only paths use
 * {@link getActiveTransaction} so they observe an existing tx but do not
 * provoke one to open.
 */
export function lazyTransactionalUnitOfWorkFactory<T>(
  delegate: UoWRunner,
  txManager: TransactionManager<T>,
): UoWRunner {
  return async (metadata, action) => {
    return delegate(metadata, async () => {
      let tx: T | undefined
      let committed = false

      const factory = async (): Promise<T> => {
        if (tx !== undefined) return tx
        tx = await txManager.begin()
        setResource(TRANSACTION_KEY, tx)
        on(Phase.COMMIT, async () => {
          if (tx === undefined) return
          await txManager.commit(tx)
          committed = true
        })
        onError(async () => {
          if (tx === undefined || committed) return
          await txManager.rollback(tx)
        })
        return tx
      }

      setResource(LAZY_TX_FACTORY_KEY, factory as () => Promise<unknown>)
      return action()
    })
  }
}

/**
 * Return the active UoW transaction, opening it if a lazy factory is
 * installed and no tx has been begun yet. Returns the cached tx on
 * subsequent calls within the same UoW.
 *
 * Returns `undefined` when no UoW is active OR when an active UoW has
 * neither an existing tx nor a lazy factory installed (e.g., the app
 * doesn't compose a TransactionManager). Callers that need a tx must
 * decide what to do with `undefined` — typically fall back to opening
 * an ad-hoc tx on their own driver.
 */
export async function getOrBeginActiveTransaction<T = unknown>(): Promise<T | undefined> {
  const state = processingStateStorage.getStore()
  if (!state) return undefined
  const existing = state.resources.get(TRANSACTION_KEY.symbol) as T | undefined
  if (existing !== undefined) return existing
  const factory = state.resources.get(LAZY_TX_FACTORY_KEY.symbol) as
    | (() => Promise<T>)
    | undefined
  if (factory === undefined) return undefined
  return await factory()
}
