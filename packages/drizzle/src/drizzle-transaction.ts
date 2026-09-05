import type {
  UnitOfWork,
} from "@kronos-ts/core"
import {
  activeTransaction,
  adapterUnitOfWork,
  openTransaction,
  type TransactionHooks,
  transactionRegistry,
} from "./transaction-glue.js"


/**
 * A Drizzle database handle. Declares only what this package needs of it —
 * the ability to open a transaction — because that is what the family is keyed
 * by. Works with any Drizzle driver (postgres-js, node-postgres, …).
 */
export type DrizzleDb = {
  transaction<T>(fn: (tx: any) => Promise<T>): Promise<T>
}

/** The Drizzle transaction object — the same query API as the database. */
export type DrizzleTransaction = any

/** Tuning for the transaction this package opens. */
export type DrizzleTransactionOptions = {
  /**
   * Runs once on every transaction, right after it opens and before the UoW
   * gets the handle. This is where a Postgres-backed deployment arms session
   * GUCs — chiefly `idle_in_transaction_session_timeout` — so a stalled UoW
   * (e.g. a hung dead-letter drain whose replay never returns) is aborted by
   * the database instead of pinning a connection indefinitely. The Postgres
   * adapter does this for its own transactions; this hook is the equivalent
   * seam for the (DB-agnostic) Drizzle path. No-op by default.
   *
   * ```ts
   * drizzleUnitOfWork(unitOfWork, db, {
   *   onBeginTransaction: (tx) => tx.execute(sql.raw(
   *     "SET LOCAL idle_in_transaction_session_timeout = 30000")),
   * })
   * ```
   */
  readonly onBeginTransaction?: (tx: DrizzleTransaction) => Promise<void>
}

/**
 * INTERNAL. Drizzle's `db.transaction()` is callback-scoped; the framework's
 * lifecycle is begin/commit/rollback. Deferred promises bridge the two.
 *
 * Not exported: there is no `TransactionManager` concept to hand around. This
 * package ships the finished factory and accessor pair, not a part for one.
 */
function transactionHooks(
  db: DrizzleDb,
  options: DrizzleTransactionOptions,
): TransactionHooks<DrizzleTransaction> {
  const { onBeginTransaction } = options
  return {
    async begin(): Promise<DrizzleTransaction> {
      let resolveTx!: (tx: DrizzleTransaction) => void
      let rejectTx!: (error: unknown) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<DrizzleTransaction>((resolve, reject) => {
        resolveTx = resolve
        rejectTx = reject
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = db.transaction(async (tx) => {
        // Arm session settings (e.g. idle-in-transaction timeout) before the
        // UoW gets the handle, so begin() only resolves once the tx is bounded.
        if (onBeginTransaction) {
          try {
            await onBeginTransaction(tx)
          } catch (err) {
            // Surface as a begin() rejection rather than hanging on txReady.
            rejectTx(err)
            throw err
          }
        }
        resolveTx(tx)
        await completionSignal
      })
      // If db.transaction() rejects before onBeginTransaction runs (e.g. the
      // pool can't hand out a connection), make begin() reject instead of
      // hanging on txReady forever.
      txPromise.catch(rejectTx)

      const tx = await txReady
      ;(tx as any).__kronos_commit = resolveCompletion
      ;(tx as any).__kronos_rollback = rejectCompletion
      ;(tx as any).__kronos_txPromise = txPromise

      return tx
    },

    async commit(tx: DrizzleTransaction): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: DrizzleTransaction): Promise<void> {
      const rollback = (tx as any).__kronos_rollback as (error: unknown) => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      rollback(new Error("Transaction rolled back"))
      try {
        await txPromise
      } catch {
        /* expected */
      }
    },
  }
}

/**
 * This package's PRIVATE table of transactions, keyed by unit of work.
 *
 * The base `UnitOfWork` has no transaction concept, so this is where a
 * drizzle transaction actually lives — and the only things that can read it
 * are the two accessors below. That is what makes them TYPED: the type comes
 * from the adapter that owns the driver, not from an assertion at the call site.
 */
const registry = transactionRegistry<DrizzleTransaction>()

/**
 * The unit-of-work factory this package exports: every unit of work it
 * mints runs inside one drizzle transaction, begun before the handler,
 * committed at COMMIT and rolled back on error.
 *
 * Same shape as the core `unitOfWork`, so it drops into any seam that takes one:
 *
 * ```ts
 * const uow = drizzleUnitOfWork(unitOfWork, db)
 * const commandBus = interceptingCommandBus(localCommandBus(uow), correlation)
 * processors: projections.map((p) => ({ ...p, eventStore, tokenStore, unitOfWork: uow }))
 * ```
 *
 * Everything on that unit of work — appended events, token updates, dead
 * letters, and the handler's own writes through {@link activeDrizzleTransaction} —
 * commits or rolls back together.
 *
 * THING-FIRST: `next` — the factory the unit of work is minted from — comes
 * first, because that is the thing being decorated, and the client is the
 * configuration. It is required and never defaulted: a default hides the
 * composition chain, and the whole point of this shape is that a call site
 * shows what its units of work are made of.
 *
 * It is also CAPABILITY-PRESERVING. The decorator returns `() => U` for
 * whatever `U` it was handed, and it decorates the SAME handle rather than
 * rebuilding a record from it — so a composed capability survives both the type
 * and the runtime:
 *
 * ```ts
 * const uow = drizzleUnitOfWork(() => correlating(unitOfWork(clock)), db)
 * //    ^ () => CorrelatingUnitOfWork, and its transactions are keyed on that
 * //      very object, which is the one `ctx.unitOfWork` hands back
 * ```
 */
export function drizzleUnitOfWork<U extends UnitOfWork = UnitOfWork>(
  next: () => U,
  db: DrizzleDb,
  options: DrizzleTransactionOptions = {},
): () => U {
  return adapterUnitOfWork(registry, transactionHooks(db, options), next) as () => U
}

/**
 * The drizzle transaction `uow` is running in, OPENING one if it has not begun.
 *
 * For writers that must be inside the transaction whether or not anything else
 * has touched it yet. Rejects when `uow` did not come from
 * {@link drizzleUnitOfWork} — that is a wiring mistake, and answering `undefined`
 * would turn it into a silent non-transactional write.
 */
export function drizzleTransaction(uow: UnitOfWork): Promise<DrizzleTransaction> {
  return openTransaction(registry, uow, "drizzleUnitOfWork")
}

/**
 * The drizzle transaction `uow` is already running in, or `undefined` — NEVER
 * opens one.
 *
 * This is what a token store, a dead-letter queue or a projection writer wants:
 *
 * ```ts
 * const write = activeDrizzleTransaction(ctx.unitOfWork) ?? db
 * ```
 *
 * `undefined` means "not running in one of my transactions", so the caller
 * falls back to its plain handle instead of provoking a transaction nobody
 * asked for.
 */
export function activeDrizzleTransaction(
  uow: UnitOfWork | undefined,
): DrizzleTransaction | undefined {
  return activeTransaction(registry, uow)
}

// ---------------------------------------------------------------------------
// THE EXTENSION STORY, in full.
//
// An extension to Kronos is four plain functions and a type. There is no
// plugin interface, no registry, no lifecycle to hook and nothing to subclass:
//
//   1. STORE IMPLEMENTATIONS — `drizzleTokenStore`, `drizzleDeadLetterQueue`, …
//      ordinary objects satisfying the framework's store interfaces.
//   2. A UNIT-OF-WORK WRAPPER — `drizzleUnitOfWork(unitOfWork, db)`, a
//      unit-of-work factory that gives every unit of work a transaction.
//   3. A HANDLER WRAPPER — `drizzleHandler(handler, db)`, which adds a capability to
//      the ctx a handler function receives. It wraps the FUNCTION, not the
//      entry: the host spreads the entry itself.
//
// All share ONE piece of state — the uow-keyed registry above — which is
// what makes the capability and the transaction the same transaction.
// ---------------------------------------------------------------------------

/** The `db()` capability this extension adds to a handler context. */
export type DrizzleCapability = {
  /**
   * This invocation's Drizzle handle: the unit of work's transaction when one is
   * open, otherwise the base handle the wrapper was built with.
   *
   * Always safe to call. A handler written against it works unchanged whether
   * or not the seam it runs in was given a transactional factory — which is the
   * point, because that is a DEPLOYMENT decision and a slice should not encode
   * it. It never OPENS a transaction; use {@link drizzleTransaction} for that.
   */
  db(): DrizzleDb | DrizzleTransaction
}


/**
 * Wrap a HANDLER FUNCTION — command, event or query — so its context gains
 * `db()`, the Drizzle handle bound to whatever unit of work the invocation is
 * running in.
 *
 * It is a plain function over a plain function: it takes the handler that ASKS
 * for `db()` and returns one that asks only for the base context, having
 * supplied the difference. Nothing about a handler ENTRY appears in the type —
 * the host spreads the entry itself, which is also where any other field
 * (`descriptor`, `name`, `appendCondition`) survives untouched:
 *
 * ```ts
 * const editWidget = commandHandler(EditWidget, async ({ payload }, ctx: CommandHandlerContext & DrizzleCapability) => {
 *   await ctx.db().update(widgets).set({ name: payload.name })
 * })
 *
 * kronos({
 *   commandHandlers: [editWidget]
 *     .map((h) => ({ ...h, handler: drizzleHandler(h.handler, db) }))
 *     .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
 * })
 * ```
 *
 * The erasure is DIRECTIONAL — `db()` goes in, the base context comes out — so
 * ordering a chain wrongly (wrapping twice, or wrapping a handler that never
 * asked for `db()`) is a compile error rather than a runtime surprise.
 *
 * Build it from the SAME handle you built {@link drizzleUnitOfWork} from. The
 * capability reads this extension's uow-keyed registry, so a handler's writes
 * and the unit of work's transaction are the same transaction and commit
 * together.
 */
export function drizzleHandler<M, C extends DrizzleCapability & { readonly unitOfWork: UnitOfWork }, R>(
  next: (message: M, context: C) => R,
  db: DrizzleDb,
): (message: M, context: Omit<C, "db">) => R {
  return (message, context) =>
    next(message, {
      ...context,
      db: () =>
        activeDrizzleTransaction((context as { readonly unitOfWork: UnitOfWork }).unitOfWork) ?? db,
    } as unknown as C)
}
