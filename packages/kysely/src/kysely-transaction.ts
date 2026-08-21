import type {
  EventHandlerContext,
  HandlerContext,
  QueryHandlerContext,
  UnitOfWork,
  PersistenceFamily,
} from "@kronos-ts/core"
import {
  activeTransaction,
  adapterUnitOfWork,
  openTransaction,
  type TransactionHooks,
  transactionRegistry,
} from "./transaction-glue.js"

/**
 * THE KYSELY FAMILY MARK — a phantom, type-only brand on every unit of work
 * kyselyUnitOfWork(next, db) mints, and the thing this package's token store and
 * dead-letter queue demand back.
 *
 * WHY IT EXISTS. This family is keyed by TRANSACTION IDENTITY: the token store,
 * the dead-letter queue and what a handler writes through `activeKyselyTransaction(ctx.unitOfWork)` must all
 * write through the SAME handle, or they do not commit together. Handing this
 * package's token store a unit of work from another family does not throw — the
 * store looks for ITS transaction, does not find one, and falls back to its
 * plain handle, so the token update commits OUTSIDE the batch. Every test
 * passes; then a crash lands between the projection write and the token write
 * and a read model is permanently wrong. The mark turns that into a build
 * error.
 *
 * IT IS ERASED AND NEVER CONSTRUCTED. `PersistenceFamily` hangs on an ambient
 * unique symbol declared in core; nothing writes the property and nothing can
 * read it. kyselyUnitOfWork(…) returns exactly what it always
 * returned and asserts the branded type, so the emitted JavaScript is
 * unchanged.
 *
 * THE FIX STRING IS THIS PACKAGE'S TO WRITE, and that is the point of putting
 * it here. Core can only say "these two are different families"; this package
 * knows precisely which factory the host should have called, so a mismatch
 * prints that sentence at the wiring site.
 */
export type KyselyFamily = PersistenceFamily<
  "kysely",
  "build this processor's unitOfWork with kyselyUnitOfWork(next, db) — this family's stores write through its transaction"
>

/**
 * A Kysely database instance. Declares only what this package needs of it —
 * the ability to open a transaction — because that is what the family is keyed
 * by. The query builder itself is reached through the handle at runtime.
 */
export type KyselyDb = {
  transaction(): { execute<T>(fn: (trx: any) => Promise<T>): Promise<T> }
}

/** The Kysely transaction object — the same query API as the database. */
export type KyselyTransaction = any

/**
 * INTERNAL. Kysely's `db.transaction().execute(fn)` is callback-scoped; the
 * framework's lifecycle is begin/commit/rollback. Deferred promises bridge the
 * two.
 *
 * Not exported: there is no `TransactionManager` concept to hand around. This
 * package ships the finished factory and accessor pair, not a part for one.
 */
function transactionHooks(db: KyselyDb): TransactionHooks<KyselyTransaction> {
  return {
    async begin(): Promise<KyselyTransaction> {
      let resolveTx!: (tx: KyselyTransaction) => void
      let rejectTx!: (error: unknown) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<KyselyTransaction>((resolve, reject) => {
        resolveTx = resolve
        rejectTx = reject
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = db.transaction().execute(async (trx) => {
        resolveTx(trx)
        await completionSignal
      })
      // If the transaction rejects before the callback runs (e.g. the pool
      // can't hand out a connection), make begin() reject instead of hanging.
      txPromise.catch(rejectTx)

      const tx = await txReady
      ;(tx as any).__kronos_commit = resolveCompletion
      ;(tx as any).__kronos_rollback = rejectCompletion
      ;(tx as any).__kronos_txPromise = txPromise

      return tx
    },

    async commit(tx: KyselyTransaction): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: KyselyTransaction): Promise<void> {
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
 * kysely transaction actually lives — and the only things that can read it
 * are the two accessors below. That is what makes them TYPED: the type comes
 * from the adapter that owns the driver, not from an assertion at the call site.
 */
const registry = transactionRegistry<KyselyTransaction>()

/**
 * The unit-of-work factory this package exports: every unit of work it
 * mints runs inside one kysely transaction, begun before the handler,
 * committed at COMMIT and rolled back on error.
 *
 * Same shape as the core `unitOfWork`, so it drops into any seam that takes one:
 *
 * ```ts
 * const uow = kyselyUnitOfWork(unitOfWork, db)
 * const commandBus = interceptingCommandBus(localCommandBus(uow), correlation)
 * processors: projections.map((p) => ({ ...p, eventStore, tokenStore, unitOfWork: uow }))
 * ```
 *
 * Everything on that unit of work — appended events, token updates, dead
 * letters, and the handler's own writes through {@link activeKyselyTransaction} —
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
 * const uow = kyselyUnitOfWork(() => correlating(unitOfWork(clock)), db)
 * //    ^ () => CorrelatingUnitOfWork, and its transactions are keyed on that
 * //      very object, which is the one `ctx.unitOfWork` hands back
 * ```
 */
export function kyselyUnitOfWork<U extends UnitOfWork = UnitOfWork>(
  next: () => U,
  db: KyselyDb,
): () => U & KyselyFamily {
  return adapterUnitOfWork(registry, transactionHooks(db), next) as () => U & KyselyFamily
}

/**
 * The kysely transaction `uow` is running in, OPENING one if it has not begun.
 *
 * For writers that must be inside the transaction whether or not anything else
 * has touched it yet. Rejects when `uow` did not come from
 * {@link kyselyUnitOfWork} — that is a wiring mistake, and answering `undefined`
 * would turn it into a silent non-transactional write.
 */
export function kyselyTransaction(uow: UnitOfWork): Promise<KyselyTransaction> {
  return openTransaction(registry, uow, "kyselyUnitOfWork")
}

/**
 * The kysely transaction `uow` is already running in, or `undefined` — NEVER
 * opens one.
 *
 * This is what a token store, a dead-letter queue or a projection writer wants:
 *
 * ```ts
 * const write = activeKyselyTransaction(ctx.unitOfWork) ?? db
 * ```
 *
 * `undefined` means "not running in one of my transactions", so the caller
 * falls back to its plain handle instead of provoking a transaction nobody
 * asked for.
 */
export function activeKyselyTransaction(
  uow: UnitOfWork | undefined,
): KyselyTransaction | undefined {
  return activeTransaction(registry, uow)
}

// ---------------------------------------------------------------------------
// THE EXTENSION STORY, in full.
//
// An extension to Kronos is four plain functions and a type. There is no
// plugin interface, no registry, no lifecycle to hook and nothing to subclass:
//
//   1. STORE IMPLEMENTATIONS — `kyselyTokenStore`, `kyselyDeadLetterQueue`, …
//      ordinary objects satisfying the framework's store interfaces.
//   2. A UNIT-OF-WORK WRAPPER — `kyselyUnitOfWork(unitOfWork, db)`, a
//      unit-of-work factory that gives every unit of work a transaction.
//   3. A HANDLER WRAPPER — `kyselyHandler(handler, db)`, which adds a capability to
//      the ctx a handler FUNCTION receives. The host spreads the entry.
//   4. A NAMED CONTEXT TYPE — `KyselyContext`, so a slice's signature reads
//      `ctx: KyselyContext` rather than an anonymous intersection.
//
// All four share ONE piece of state — the uow-keyed registry above — which is
// what makes the capability and the transaction the same transaction.
// ---------------------------------------------------------------------------

/** The `db()` capability this extension adds to a handler context. */
export type KyselyCapability = {
  /**
   * This invocation's Kysely handle: the unit of work's transaction when one is
   * open, otherwise the base handle the wrapper was built with.
   *
   * Always safe to call. A handler written against it works unchanged whether
   * or not the seam it runs in was given a transactional factory — which is the
   * point, because that is a DEPLOYMENT decision and a slice should not encode
   * it. It never OPENS a transaction; use {@link kyselyTransaction} for that.
   */
  db(): KyselyDb | KyselyTransaction
}

/** A command handler's context, plus this extension's capability. */
export type KyselyContext = HandlerContext & KyselyCapability
/** An event handler's context, plus this extension's capability. */
export type KyselyEventContext = EventHandlerContext & KyselyCapability
/** A query handler's context, plus this extension's capability. */
export type KyselyQueryContext = QueryHandlerContext & KyselyCapability

/**
 * Wrap a HANDLER FUNCTION — command, event or query — so its context gains
 * `db()`, the Kysely handle bound to whatever unit of work the invocation is
 * running in.
 *
 * It is a plain function over a plain function: it takes the handler that ASKS
 * for `db()` and returns one that asks only for the base context, having
 * supplied the difference. Nothing about a handler ENTRY appears in the type —
 * the host spreads the entry itself, which is also where any other field
 * (`descriptor`, `name`, `appendCondition`) survives untouched:
 *
 * ```ts
 * const editWidget = commandHandler(EditWidget, async ({ payload }, ctx: KyselyContext) => {
 *   await ctx.db().updateTable("widgets").set({ name: payload.name }).execute()
 * })
 *
 * kronos({
 *   commandHandlers: [editWidget]
 *     .map((h) => ({ ...h, handler: kyselyHandler(h.handler, db) }))
 *     .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
 * })
 * ```
 *
 * The erasure is DIRECTIONAL — `db()` goes in, the base context comes out — so
 * ordering a chain wrongly (wrapping twice, or wrapping a handler that never
 * asked for `db()`) is a compile error rather than a runtime surprise.
 *
 * Build it from the SAME handle you built {@link kyselyUnitOfWork} from. The
 * capability reads this extension's uow-keyed registry, so a handler's writes
 * and the unit of work's transaction are the same transaction and commit
 * together.
 */
export function kyselyHandler<M, C extends KyselyCapability & { readonly unitOfWork: UnitOfWork }, R>(
  next: (message: M, context: C) => R,
  db: KyselyDb,
): (message: M, context: Omit<C, "db">) => R {
  return (message, context) =>
    next(message, {
      ...context,
      db: () =>
        activeKyselyTransaction((context as { readonly unitOfWork: UnitOfWork }).unitOfWork) ?? db,
    } as unknown as C)
}
