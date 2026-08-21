import { Phase, type UnitOfWork } from "@kronos-ts/core"

// ---------------------------------------------------------------------------
// PRIVATE to @kronos-ts/knex. Not exported from the package barrel.
//
// The registry / factory / accessor glue this package's transaction seam is
// built from. It used to be shared, behind a `@kronos-ts/core/transaction`
// subpath — but everything it touches on a unit of work is the PUBLIC phase
// API (`uow.on(Phase.COMMIT, …)` and `uow.onError(…)`), which is exactly what
// makes it a HELPER rather than part of core. Helpers live with the code that
// uses them, so this package owns its copy, carrying only what knex does.
// ---------------------------------------------------------------------------

/**
 * INTERNAL — the three calls this package's transaction plumbing makes.
 *
 * Deliberately not a named concept a host ever implements. A host never plugs a
 * part in here: this package ships a FINISHED factory (`knexUnitOfWork(make,
 * knex)`) and a finished accessor pair, because it already knows what a
 * transaction is on its own driver.
 */
export type TransactionHooks<T> = {
  begin(): Promise<T>
  commit(tx: T): Promise<void>
  rollback(tx: T): Promise<void>
}

/**
 * One unit of work's transaction, in flight or settled — plus the hooks that
 * opened it. The hooks are recorded per unit of work because they are per
 * DATABASE: two `knexUnitOfWork(make, a)` / `knexUnitOfWork(make, b)` factories share this
 * module's registry, and the free accessors must reach the right driver for
 * whichever unit of work they are handed.
 */
type TransactionSlot<T> = {
  readonly hooks: TransactionHooks<T>
  opening?: Promise<T>
  tx?: T
}

/**
 * This package's private table of transactions, keyed by unit of work.
 *
 * The base {@link UnitOfWork} has no transaction concept — no `transaction()`,
 * no `activeTransaction()`, nothing to assert a type against. THIS is where a
 * transaction actually lives, and the only things that can read it are the
 * accessors this package exports. There is one per MODULE (not per factory),
 * so the free accessors can serve any unit of work its factories minted.
 */
export type TransactionRegistry<T> = WeakMap<UnitOfWork, TransactionSlot<T>>

/** Create this package's private transaction table. One per module. */
export function transactionRegistry<T>(): TransactionRegistry<T> {
  return new WeakMap<UnitOfWork, TransactionSlot<T>>()
}

/**
 * Builds the unit-of-work factory this package exports, recording each unit of
 * work's transaction in `registry`.
 *
 * The ordering it encodes is the whole point: the opener caches so there is
 * exactly ONE transaction per unit of work; commit runs in the COMMIT phase;
 * rollback runs on error but never after a successful commit.
 *
 * It DECORATES the handle it was given — the same object, claimed in the
 * registry and hooked through the public phase API — rather than rebuilding a
 * record from it. That is what makes it capability-preserving: `U` comes out
 * exactly as it went in, so `knexUnitOfWork(() => correlating(unitOfWork()),
 * knex)` still has `correlationData()` on it at runtime, and the WeakMap
 * that keys this package's transactions is keyed on the very object the handler
 * will later hand back through `ctx.unitOfWork`.
 *
 * It is EAGER, with no lazy mode, because this package has no read-only path
 * that would want one: a PRE_INVOCATION hook forces the transaction open before
 * the action runs. That is load-bearing — the token store and dead letter queue
 * read through the OBSERVING accessor, which by contract never provokes a
 * transaction to open, so a lazy binding would leave a token-only processor
 * batch writing outside the transaction it believes it is in.
 */
export function adapterUnitOfWork<T, U extends UnitOfWork>(
  registry: TransactionRegistry<T>,
  hooks: TransactionHooks<T>,
  make: () => U,
): () => U {
  return () => {
    const uow = make()
    // Claim the unit of work for this package and this database. Claiming is
    // not opening: the slot starts with hooks and no transaction.
    registry.set(uow, { hooks })
    // Async work is legal inside a phase action, which is what lets a
    // SYNCHRONOUS factory produce eager semantics.
    uow.on(Phase.PRE_INVOCATION, async () => { await begin(registry, uow) })
    return uow
  }
}

/**
 * The LAZY-OPENING half of the accessor pair — what `knexTransaction(uow)` calls.
 *
 * Opens the transaction if this unit of work has none yet, so a writer can ask
 * for one without the caller having arranged it. Throws if `uow` was not minted
 * by this package's factory: asking for a transaction on a unit of work that
 * has no adapter composed is a wiring mistake, and returning `undefined` would
 * push it downstream as a silent non-transactional write.
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
 * The OBSERVING half of the accessor pair — what `activeKnexTransaction(uow)` calls.
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
