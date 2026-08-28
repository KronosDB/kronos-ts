import type {
  EventHandlerContext,
  CommandHandlerContext,
  QueryHandlerContext,
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
 * A Knex instance. Declares only what this package needs of it — the ability
 * to open a transaction — because that is what the family is keyed by. The
 * query builder itself is reached through the handle at runtime.
 */
export type KnexClient = {
  transaction<T>(fn: (trx: any) => Promise<T>): Promise<T>
}

/** The Knex transaction object — the same query API as the instance. */
export type KnexTransaction = any

/** Tuning for the transaction this package opens. */
export type KnexTransactionOptions = {
  /**
   * Runs once on every transaction, right after it opens and before the UoW
   * gets the handle. This is where a Postgres-backed deployment arms session
   * GUCs — chiefly `idle_in_transaction_session_timeout` — so a stalled UoW
   * (e.g. a hung dead-letter drain whose replay never returns) is aborted by
   * the database instead of pinning a connection indefinitely. No-op by default.
   *
   * ```ts
   * knexUnitOfWork(unitOfWork, knex, {
   *   onBeginTransaction: (trx) => trx.raw(
   *     "SET LOCAL idle_in_transaction_session_timeout = 30000"),
   * })
   * ```
   */
  readonly onBeginTransaction?: (tx: KnexTransaction) => Promise<void>
}

/**
 * INTERNAL. Knex's `knex.transaction(fn)` is callback-scoped; the framework's
 * lifecycle is begin/commit/rollback. Deferred promises bridge the two.
 *
 * Not exported: there is no `TransactionManager` concept to hand around. This
 * package ships the finished factory and accessor pair, not a part for one.
 */
function transactionHooks(
  knex: KnexClient,
  options: KnexTransactionOptions,
): TransactionHooks<KnexTransaction> {
  const { onBeginTransaction } = options
  return {
    async begin(): Promise<KnexTransaction> {
      let resolveTx!: (tx: KnexTransaction) => void
      let rejectTx!: (error: unknown) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<KnexTransaction>((resolve, reject) => {
        resolveTx = resolve
        rejectTx = reject
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      const txPromise = knex.transaction(async (trx) => {
        // Arm session settings (e.g. idle-in-transaction timeout) before the
        // UoW gets the handle, so begin() only resolves once the tx is bounded.
        if (onBeginTransaction) {
          try {
            await onBeginTransaction(trx)
          } catch (err) {
            rejectTx(err)
            throw err
          }
        }
        resolveTx(trx)
        await completionSignal
      })
      // If knex.transaction() rejects before onBeginTransaction runs (e.g. the
      // pool can't hand out a connection), make begin() reject instead of
      // hanging on txReady forever.
      txPromise.catch(rejectTx)

      const tx = await txReady
      ;(tx as any).__kronos_commit = resolveCompletion
      ;(tx as any).__kronos_rollback = rejectCompletion
      ;(tx as any).__kronos_txPromise = txPromise

      return tx
    },

    async commit(tx: KnexTransaction): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: KnexTransaction): Promise<void> {
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
 * knex transaction actually lives — and the only things that can read it
 * are the two accessors below. That is what makes them TYPED: the type comes
 * from the adapter that owns the driver, not from an assertion at the call site.
 */
const registry = transactionRegistry<KnexTransaction>()

/**
 * The unit-of-work factory this package exports: every unit of work it
 * mints runs inside one knex transaction, begun before the handler,
 * committed at COMMIT and rolled back on error.
 *
 * Same shape as the core `unitOfWork`, so it drops into any seam that takes one:
 *
 * ```ts
 * const uow = knexUnitOfWork(unitOfWork, knex)
 * const commandBus = interceptingCommandBus(localCommandBus(uow), correlation)
 * processors: projections.map((p) => ({ ...p, eventStore, tokenStore, unitOfWork: uow }))
 * ```
 *
 * Everything on that unit of work — appended events, token updates, dead
 * letters, and the handler's own writes through {@link activeKnexTransaction} —
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
 * const uow = knexUnitOfWork(() => correlating(unitOfWork(clock)), knex)
 * //    ^ () => CorrelatingUnitOfWork, and its transactions are keyed on that
 * //      very object, which is the one `ctx.unitOfWork` hands back
 * ```
 */
export function knexUnitOfWork<U extends UnitOfWork = UnitOfWork>(
  next: () => U,
  knex: KnexClient,
  options: KnexTransactionOptions = {},
): () => U {
  return adapterUnitOfWork(registry, transactionHooks(knex, options), next) as () => U
}

/**
 * The knex transaction `uow` is running in, OPENING one if it has not begun.
 *
 * For writers that must be inside the transaction whether or not anything else
 * has touched it yet. Rejects when `uow` did not come from
 * {@link knexUnitOfWork} — that is a wiring mistake, and answering `undefined`
 * would turn it into a silent non-transactional write.
 */
export function knexTransaction(uow: UnitOfWork): Promise<KnexTransaction> {
  return openTransaction(registry, uow, "knexUnitOfWork")
}

/**
 * The knex transaction `uow` is already running in, or `undefined` — NEVER
 * opens one.
 *
 * This is what a token store, a dead-letter queue or a projection writer wants:
 *
 * ```ts
 * const write = activeKnexTransaction(ctx.unitOfWork) ?? knex
 * ```
 *
 * `undefined` means "not running in one of my transactions", so the caller
 * falls back to its plain handle instead of provoking a transaction nobody
 * asked for.
 */
export function activeKnexTransaction(uow: UnitOfWork | undefined): KnexTransaction | undefined {
  return activeTransaction(registry, uow)
}

// ---------------------------------------------------------------------------
// THE EXTENSION STORY, in full.
//
// An extension to Kronos is four plain functions and a type. There is no
// plugin interface, no registry, no lifecycle to hook and nothing to subclass:
//
//   1. STORE IMPLEMENTATIONS — `knexTokenStore`, `knexDeadLetterQueue`, …
//      ordinary objects satisfying the framework's store interfaces.
//   2. A UNIT-OF-WORK WRAPPER — `knexUnitOfWork(unitOfWork, knex)`, a
//      unit-of-work factory that gives every unit of work a transaction.
//   3. A HANDLER WRAPPER — `knexHandler(handler, knex)`, which adds a capability to
//      the ctx a handler FUNCTION receives. The host spreads the entry.
//   4. A NAMED CONTEXT TYPE — `KnexCommandContext`, so a slice's signature reads
//      `ctx: KnexCommandContext` rather than an anonymous intersection.
//
// All four share ONE piece of state — the uow-keyed registry above — which is
// what makes the capability and the transaction the same transaction.
// ---------------------------------------------------------------------------

/** The `knex()` capability this extension adds to a handler context. */
export type KnexCapability = {
  /**
   * This invocation's Knex handle: the unit of work's transaction when one is
   * open, otherwise the base handle the wrapper was built with.
   *
   * Always safe to call. A handler written against it works unchanged whether
   * or not the seam it runs in was given a transactional factory — which is the
   * point, because that is a DEPLOYMENT decision and a slice should not encode
   * it. It never OPENS a transaction; use {@link knexTransaction} for that.
   */
  knex(): KnexClient | KnexTransaction
}

/** A command handler's context, plus this extension's capability. */
export type KnexCommandContext = CommandHandlerContext & KnexCapability
/** An event handler's context, plus this extension's capability. */
export type KnexEventContext = EventHandlerContext & KnexCapability
/** A query handler's context, plus this extension's capability. */
export type KnexQueryContext = QueryHandlerContext & KnexCapability

/**
 * Wrap a HANDLER FUNCTION — command, event or query — so its context gains
 * `knex()`, the Knex handle bound to whatever unit of work the invocation is
 * running in.
 *
 * It is a plain function over a plain function: it takes the handler that ASKS
 * for `knex()` and returns one that asks only for the base context, having
 * supplied the difference. Nothing about a handler ENTRY appears in the type —
 * the host spreads the entry itself, which is also where any other field
 * (`descriptor`, `name`, `appendCondition`) survives untouched:
 *
 * ```ts
 * const editWidget = commandHandler(EditWidget, async ({ payload }, ctx: KnexCommandContext) => {
 *   await ctx.knex()("widgets").update({ name: payload.name })
 * })
 *
 * kronos({
 *   commandHandlers: [editWidget]
 *     .map((h) => ({ ...h, handler: knexHandler(h.handler, knex) }))
 *     .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
 * })
 * ```
 *
 * The erasure is DIRECTIONAL — `knex()` goes in, the base context comes out — so
 * ordering a chain wrongly (wrapping twice, or wrapping a handler that never
 * asked for `knex()`) is a compile error rather than a runtime surprise.
 *
 * Build it from the SAME handle you built {@link knexUnitOfWork} from. The
 * capability reads this extension's uow-keyed registry, so a handler's writes
 * and the unit of work's transaction are the same transaction and commit
 * together.
 */
export function knexHandler<M, C extends KnexCapability & { readonly unitOfWork: UnitOfWork }, R>(
  next: (message: M, context: C) => R,
  knex: KnexClient,
): (message: M, context: Omit<C, "knex">) => R {
  return (message, context) =>
    next(message, {
      ...context,
      knex: () =>
        activeKnexTransaction((context as { readonly unitOfWork: UnitOfWork }).unitOfWork) ?? knex,
    } as unknown as C)
}
