import type {
  EventHandlerContext,
  CommandHandlerContext,
  QueryHandlerContext,
  UnitOfWork,
  UnitOfWorkBrand,
} from "@kronos-ts/core"
import {
  activeTransaction,
  adapterUnitOfWork,
  openTransaction,
  type TransactionHooks,
  transactionRegistry,
} from "./transaction-glue.js"

/**
 * THE TYPEORM FAMILY MARK — a phantom, type-only brand on every unit of work
 * typeormUnitOfWork(next, dataSource) mints, and the thing this package's token store and
 * dead-letter queue demand back.
 *
 * WHY IT EXISTS. This family is keyed by TRANSACTION IDENTITY: the token store,
 * the dead-letter queue and what a handler writes through `activeTypeormTransaction(ctx.unitOfWork)` must all
 * write through the SAME handle, or they do not commit together. Handing this
 * package's token store a unit of work from another family does not throw — the
 * store looks for ITS transaction, does not find one, and falls back to its
 * plain handle, so the token update commits OUTSIDE the batch. Every test
 * passes; then a crash lands between the projection write and the token write
 * and a read model is permanently wrong. The mark turns that into a build
 * error.
 *
 * IT IS ERASED AND NEVER CONSTRUCTED. `UnitOfWorkBrand` hangs on an ambient
 * unique symbol declared in core; nothing writes the property and nothing can
 * read it. typeormUnitOfWork(…) returns exactly what it always
 * returned and asserts the branded type, so the emitted JavaScript is
 * unchanged.
 *
 * THE FIX STRING IS THIS PACKAGE'S TO WRITE, and that is the point of putting
 * it here. Core can only say "these two are different families"; this package
 * knows precisely which factory the host should have called, so a mismatch
 * prints that sentence at the wiring site.
 */
export type TypeormUnitOfWork = UnitOfWorkBrand<
  "typeorm",
  "build this processor's unitOfWork with typeormUnitOfWork(next, dataSource) — this family's stores write through its transaction"
>

/**
 * A TypeORM handle — a `DataSource` or an `EntityManager`. Declares only what
 * this package needs of it — the ability to open a transaction — because that
 * is what the family is keyed by. The query API is reached through the handle
 * at runtime.
 */
export type TypeormManager = {
  transaction<T>(fn: (entityManager: any) => Promise<T>): Promise<T>
}

/** The TypeORM EntityManager inside a transaction. */
export type TypeormTransaction = any

/**
 * INTERNAL. TypeORM's `dataSource.transaction(fn)` is callback-scoped; the
 * framework's lifecycle is begin/commit/rollback. Deferred promises bridge the
 * two.
 *
 * Not exported: there is no `TransactionManager` concept to hand around. This
 * package ships the finished factory and accessor pair, not a part for one.
 */
function transactionHooks(manager: TypeormManager): TransactionHooks<TypeormTransaction> {
  return {
    async begin(): Promise<TypeormTransaction> {
      let resolveTx!: (tx: TypeormTransaction) => void
      let rejectTx!: (error: unknown) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<TypeormTransaction>((resolve, reject) => {
        resolveTx = resolve
        rejectTx = reject
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = manager.transaction(async (entityManager) => {
        resolveTx(entityManager)
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

    async commit(tx: TypeormTransaction): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: TypeormTransaction): Promise<void> {
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
 * typeorm transaction actually lives — and the only things that can read it
 * are the two accessors below. That is what makes them TYPED: the type comes
 * from the adapter that owns the driver, not from an assertion at the call site.
 */
const registry = transactionRegistry<TypeormTransaction>()

/**
 * The unit-of-work factory this package exports: every unit of work it
 * mints runs inside one typeorm transaction, begun before the handler,
 * committed at COMMIT and rolled back on error.
 *
 * Same shape as the core `unitOfWork`, so it drops into any seam that takes one:
 *
 * ```ts
 * const uow = typeormUnitOfWork(unitOfWork, dataSource)
 * const commandBus = interceptingCommandBus(localCommandBus(uow), correlation)
 * processors: projections.map((p) => ({ ...p, eventStore, tokenStore, unitOfWork: uow }))
 * ```
 *
 * Everything on that unit of work — appended events, token updates, dead
 * letters, and the handler's own writes through {@link activeTypeormTransaction} —
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
 * const uow = typeormUnitOfWork(() => correlating(unitOfWork(clock)), manager)
 * //    ^ () => CorrelatingUnitOfWork, and its transactions are keyed on that
 * //      very object, which is the one `ctx.unitOfWork` hands back
 * ```
 */
export function typeormUnitOfWork<U extends UnitOfWork = UnitOfWork>(
  next: () => U,
  manager: TypeormManager,
): () => U & TypeormUnitOfWork {
  return adapterUnitOfWork(registry, transactionHooks(manager), next) as () => U & TypeormUnitOfWork
}

/**
 * The typeorm transaction `uow` is running in, OPENING one if it has not begun.
 *
 * For writers that must be inside the transaction whether or not anything else
 * has touched it yet. Rejects when `uow` did not come from
 * {@link typeormUnitOfWork} — that is a wiring mistake, and answering `undefined`
 * would turn it into a silent non-transactional write.
 */
export function typeormTransaction(uow: UnitOfWork): Promise<TypeormTransaction> {
  return openTransaction(registry, uow, "typeormUnitOfWork")
}

/**
 * The typeorm transaction `uow` is already running in, or `undefined` — NEVER
 * opens one.
 *
 * This is what a token store, a dead-letter queue or a projection writer wants:
 *
 * ```ts
 * const write = activeTypeormTransaction(ctx.unitOfWork) ?? dataSource.manager
 * ```
 *
 * `undefined` means "not running in one of my transactions", so the caller
 * falls back to its plain handle instead of provoking a transaction nobody
 * asked for.
 */
export function activeTypeormTransaction(
  uow: UnitOfWork | undefined,
): TypeormTransaction | undefined {
  return activeTransaction(registry, uow)
}

// ---------------------------------------------------------------------------
// THE EXTENSION STORY, in full.
//
// An extension to Kronos is four plain functions and a type. There is no
// plugin interface, no registry, no lifecycle to hook and nothing to subclass:
//
//   1. STORE IMPLEMENTATIONS — `typeormTokenStore`, `typeormDeadLetterQueue`, …
//      ordinary objects satisfying the framework's store interfaces.
//   2. A UNIT-OF-WORK WRAPPER — `typeormUnitOfWork(unitOfWork, manager)`, a
//      unit-of-work factory that gives every unit of work a transaction.
//   3. A HANDLER WRAPPER — `typeormHandler(handler, manager)`, which adds a capability to
//      the ctx a handler FUNCTION receives. The host spreads the entry.
//   4. A NAMED CONTEXT TYPE — `TypeormCommandContext`, so a slice's signature reads
//      `ctx: TypeormCommandContext` rather than an anonymous intersection.
//
// All four share ONE piece of state — the uow-keyed registry above — which is
// what makes the capability and the transaction the same transaction.
// ---------------------------------------------------------------------------

/** The `manager()` capability this extension adds to a handler context. */
export type TypeormCapability = {
  /**
   * This invocation's TypeORM handle: the unit of work's transaction when one is
   * open, otherwise the base handle the wrapper was built with.
   *
   * Always safe to call. A handler written against it works unchanged whether
   * or not the seam it runs in was given a transactional factory — which is the
   * point, because that is a DEPLOYMENT decision and a slice should not encode
   * it. It never OPENS a transaction; use {@link typeormTransaction} for that.
   */
  manager(): TypeormManager | TypeormTransaction
}

/** A command handler's context, plus this extension's capability. */
export type TypeormCommandContext = CommandHandlerContext & TypeormCapability
/** An event handler's context, plus this extension's capability. */
export type TypeormEventContext = EventHandlerContext & TypeormCapability
/** A query handler's context, plus this extension's capability. */
export type TypeormQueryContext = QueryHandlerContext & TypeormCapability

/**
 * Wrap a HANDLER FUNCTION — command, event or query — so its context gains
 * `manager()`, the TypeORM handle bound to whatever unit of work the invocation is
 * running in.
 *
 * It is a plain function over a plain function: it takes the handler that ASKS
 * for `manager()` and returns one that asks only for the base context, having
 * supplied the difference. Nothing about a handler ENTRY appears in the type —
 * the host spreads the entry itself, which is also where any other field
 * (`descriptor`, `name`, `appendCondition`) survives untouched:
 *
 * ```ts
 * const editWidget = commandHandler(EditWidget, async ({ payload }, ctx: TypeormCommandContext) => {
 *   await ctx.manager().query("UPDATE widgets SET name = $2 WHERE id = $1", [payload.id, payload.name])
 * })
 *
 * kronos({
 *   commandHandlers: [editWidget]
 *     .map((h) => ({ ...h, handler: typeormHandler(h.handler, manager) }))
 *     .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
 * })
 * ```
 *
 * The erasure is DIRECTIONAL — `manager()` goes in, the base context comes out — so
 * ordering a chain wrongly (wrapping twice, or wrapping a handler that never
 * asked for `manager()`) is a compile error rather than a runtime surprise.
 *
 * Build it from the SAME handle you built {@link typeormUnitOfWork} from. The
 * capability reads this extension's uow-keyed registry, so a handler's writes
 * and the unit of work's transaction are the same transaction and commit
 * together.
 */
export function typeormHandler<M, C extends TypeormCapability & { readonly unitOfWork: UnitOfWork }, R>(
  next: (message: M, context: C) => R,
  manager: TypeormManager,
): (message: M, context: Omit<C, "manager">) => R {
  return (message, context) =>
    next(message, {
      ...context,
      manager: () =>
        activeTypeormTransaction((context as { readonly unitOfWork: UnitOfWork }).unitOfWork) ?? manager,
    } as unknown as C)
}
