import { resourceKey, type ResourceKey } from "@kronos-ts/common"
import { processingStateStorage, setResource, on, onError } from "./processing-state.js"
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
      // Forwarders preserve the UnitOfWork interface — buffered pre-registration
      // is no longer used by transactionalUnitOfWorkFactory itself, but external
      // callers may still pre-register hooks before executeWithResult.
      // Plan 04 deletes the UnitOfWork interface and these forwarders disappear.
      on: (phase, action) => inner.on(phase, action),
      onError: (handler) => inner.onError(handler),
      whenComplete: (handler) => inner.whenComplete(handler),

      async executeWithResult<R>(action: (ctx: ProcessingContext) => Promise<R>): Promise<R> {
        const tx = await txManager.begin()

        // Inner UoW's executeWithResult enters processingStateStorage.run (Phase 1 wiring),
        // so setResource AND module-level lifecycle registrations inside the action
        // callback write to the active ALS state. D-22: a single ALS boundary.
        //
        // Phase 3 / Plan 02 (CTX-03): commit / rollback are registered via the
        // module-level on(Phase.COMMIT, ...) and onError(...) accessors INSIDE the
        // action body — no more inner.on / inner.onError buffered pre-registration.
        // INVOCATION runs first; on entry we register commit (which fires later
        // when the COMMIT phase runs) and onError (a late-bound error handler the
        // ctx fires from runErrorHandlers if any phase throws).
        return inner.executeWithResult(async (ctx) => {
          setResource(TRANSACTION_KEY, tx)
          on(Phase.COMMIT, async () => {
            await txManager.commit(tx)
          })
          onError(async () => {
            await txManager.rollback(tx)
          })
          return action(ctx)
        })
      },
    }

    return wrapper
  }
}
