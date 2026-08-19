import { Phase, type UnitOfWork } from "./unit-of-work.js"

/**
 * INTERNAL — the three calls an adapter's transaction plumbing makes.
 *
 * Deliberately not a named concept a host ever implements. A host never plugs a
 * part in here: each adapter ships a FINISHED factory (`drizzleUnitOfWork(db,
 * make)`) and a finished accessor pair, because the adapter already knows what
 * a transaction is on its own driver.
 */
export interface TransactionHooks<T> {
  begin(): Promise<T>
  commit(tx: T): Promise<void>
  rollback(tx: T): Promise<void>
}

/**
 * One unit of work's transaction, in flight or settled — plus the hooks that
 * opened it. The hooks are recorded per unit of work because they are per
 * DATABASE: two `drizzleUnitOfWork(dbA, …)` / `drizzleUnitOfWork(dbB, …)`
 * factories share this module's registry, and the free accessors must reach the
 * right driver for whichever unit of work they are handed.
 */
interface TransactionSlot<T> {
  readonly hooks: TransactionHooks<T>
  opening?: Promise<T>
  tx?: T
}

/**
 * An adapter's private table of transactions, keyed by unit of work.
 *
 * The base {@link UnitOfWork} has no transaction concept — no `transaction()`,
 * no `activeTransaction()`, nothing to assert a type against. THIS is where a
 * transaction actually lives, and the only things that can read it are the
 * accessors the owning adapter exports. Create one per adapter MODULE (not per
 * factory), so the adapter's free accessors can serve any unit of work its
 * factories minted.
 */
export type TransactionRegistry<T> = WeakMap<UnitOfWork, TransactionSlot<T>>

/** Create an adapter's private transaction table. One per adapter module. */
export function transactionRegistry<T>(): TransactionRegistry<T> {
  return new WeakMap<UnitOfWork, TransactionSlot<T>>()
}

/**
 * FOR ADAPTER AUTHORS ONLY. Not exported from the package barrel — reach it at
 * `@kronos-ts/core/transaction`.
 *
 * Builds the unit-of-work factory an adapter package exports, recording
 * each unit of work's transaction in the adapter's own `registry`.
 *
 * This is shared rather than copied six times because the glue is not
 * boilerplate — it encodes ordering that must be identical across every
 * adapter: the opener caches so there is exactly ONE transaction per unit of
 * work; commit runs in the COMMIT phase; rollback runs on error but never after
 * a successful commit. Six divergent copies of that is a bug farm, and the
 * adapters bridging callback-scoped transactions (drizzle, knex, kysely) have
 * enough driver-specific subtlety already. Everything it touches on the unit of
 * work is the PUBLIC phase API — `uow.on(Phase.COMMIT, …)` and `uow.onError(…)`
 * — so an adapter has no privileged access to the handle.
 *
 * `eager` (the default) forces the transaction open in PRE_INVOCATION, before
 * the action runs. That is load-bearing: an adapter's token store and dead
 * letter queue read through the OBSERVING accessor, which by contract never
 * provokes a transaction to open. A lazy binding would leave a token-only
 * processor batch writing outside the transaction it believes it is in. Pass
 * `eager: false` only where the units of work are reads — see the postgres
 * adapter, whose read paths are the reason it opts out.
 */
export function adapterUnitOfWork<T>(
  registry: TransactionRegistry<T>,
  hooks: TransactionHooks<T>,
  make: () => UnitOfWork,
  options: { readonly eager?: boolean } = {},
): () => UnitOfWork {
  const eager = options.eager ?? true
  return () => {
    const uow = make()
    // Claim the unit of work for this adapter and this database. Claiming is
    // not opening: the slot starts with hooks and no transaction.
    registry.set(uow, { hooks })
    // Eager = one PRE_INVOCATION hook that forces the open before the action.
    // Lazy = nothing more; `openTransaction` IS the opener, so a unit of work
    // that never asks pays no begin/commit and claims no connection.
    // Async work is legal inside a phase action, which is what lets a
    // SYNCHRONOUS factory produce eager semantics.
    if (eager) uow.on(Phase.PRE_INVOCATION, async () => { await begin(registry, uow) })
    return uow
  }
}

/**
 * The LAZY-OPENING half of an adapter's accessor pair — what
 * `drizzleTransaction(uow)` calls.
 *
 * Opens the transaction if this unit of work has none yet, so a writer can ask
 * for one without the caller having arranged it. Throws if `uow` was not minted
 * by this adapter's factory: asking for a transaction on a unit of work that has
 * no adapter composed is a wiring mistake, and returning `undefined` would push
 * it downstream as a silent non-transactional write.
 */
export function openTransaction<T>(
  registry: TransactionRegistry<T>,
  uow: UnitOfWork,
  adapterName: string,
): Promise<T> {
  const slot = registry.get(uow)
  if (slot === undefined) {
    return Promise.reject(
      new Error(
        `${adapterName}: this UnitOfWork was not minted by ${adapterName}, so it has no ` +
          `transaction to open. Build the bus or processor with ${adapterName}'s factory.`,
      ),
    )
  }
  if (slot.tx !== undefined) return Promise.resolve(slot.tx)
  return begin(registry, uow)
}

/**
 * The OBSERVING half of an adapter's accessor pair — what
 * `activeDrizzleTransaction(uow)` calls.
 *
 * Returns the transaction only if one has already begun, and NEVER opens one.
 * `undefined` means "this unit of work is not running in one of my
 * transactions", which is exactly what a token store or dead-letter queue needs
 * in order to fall back to its plain handle.
 */
export function activeTransaction<T>(
  registry: TransactionRegistry<T>,
  uow: UnitOfWork | undefined,
): T | undefined {
  if (uow === undefined) return undefined
  return registry.get(uow)?.tx
}

/**
 * Whether `uow` was minted by a factory bound to this registry.
 *
 * A LAZY adapter needs this: it must tell "my unit of work, transaction not
 * opened yet" (open one and join it) from "not my unit of work at all" (open an
 * ad-hoc transaction I own outright). The observing accessor answers
 * `undefined` for both, so it cannot make that distinction.
 */
export function claimed<T>(registry: TransactionRegistry<T>, uow: UnitOfWork): boolean {
  return registry.has(uow)
}

/** Begin once per unit of work, registering commit/rollback while doing it. */
function begin<T>(registry: TransactionRegistry<T>, uow: UnitOfWork): Promise<T> {
  const slot = registry.get(uow)
  if (slot === undefined) {
    throw new Error("adapterUnitOfWork: UnitOfWork was not claimed by this adapter")
  }
  if (slot.opening !== undefined) return slot.opening

  const { hooks } = slot
  let committed = false
  slot.opening = (async () => {
    const tx = await hooks.begin()
    slot.tx = tx
    uow.on(Phase.COMMIT, async () => {
      await hooks.commit(tx)
      committed = true
    })
    uow.onError(async () => {
      if (committed) return
      await hooks.rollback(tx)
    })
    return tx
  })()
  return slot.opening
}
