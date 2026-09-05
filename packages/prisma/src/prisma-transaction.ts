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
 * The Prisma transaction client — the `tx` parameter inside `$transaction()`.
 * Generic, because the real client type depends on the user's schema.
 */
export type PrismaTransactionClient = {
  // Marker type — the actual tx client has all model methods
  [key: string]: any
}

/**
 * A Prisma client. Declares only what this package needs of it — interactive
 * transactions — because that is what the family is keyed by. The model
 * delegates are reached through the handle at runtime.
 */
export type PrismaClientLike = {
  $transaction<T>(
    fn: (tx: PrismaTransactionClient) => Promise<T>,
    options?: { timeout?: number },
  ): Promise<T>
}

/** Tuning for the transaction this package opens. */
export type PrismaTransactionOptions = {
  /**
   * Interactive-transaction timeout handed to `$transaction`, ms. Prisma's own
   * default applies when absent — a stalled unit of work is then aborted by
   * Prisma rather than pinning a connection indefinitely.
   */
  readonly timeoutMs?: number
}

/**
 * INTERNAL. Prisma's `$transaction()` is callback-scoped — the `tx` client only
 * exists inside the callback — while the framework's lifecycle is
 * begin/commit/rollback. Deferred promises bridge the two.
 *
 * Not exported: there is no `TransactionManager` concept to hand around. This
 * package ships the finished factory and accessor pair, not a part for one.
 */
function transactionHooks(
  prisma: PrismaClientLike,
  options: PrismaTransactionOptions,
): TransactionHooks<PrismaTransactionClient> {
  return {
    async begin(): Promise<PrismaTransactionClient> {
      let resolveTx!: (tx: PrismaTransactionClient) => void
      let rejectTx!: (error: unknown) => void
      let resolveCompletion!: () => void
      let rejectCompletion!: (error: unknown) => void

      const txReady = new Promise<PrismaTransactionClient>((resolve, reject) => {
        resolveTx = resolve
        rejectTx = reject
      })

      const completionSignal = new Promise<void>((resolve, reject) => {
        resolveCompletion = resolve
        rejectCompletion = reject
      })

      // Start the transaction in the background — it waits for the completion
      // signal before committing or rolling back.
      const txPromise = prisma.$transaction(
        async (tx) => {
          resolveTx(tx)
          await completionSignal
        },
        { timeout: options.timeoutMs },
      )
      // If $transaction() rejects before the callback runs (e.g. the pool can't
      // hand out a connection), make begin() reject instead of hanging.
      txPromise.catch(rejectTx)

      // Capture the completion handlers on the tx client so commit/rollback
      // can signal from outside the callback.
      const tx = await txReady
      ;(tx as any).__kronos_commit = resolveCompletion
      ;(tx as any).__kronos_rollback = rejectCompletion
      ;(tx as any).__kronos_txPromise = txPromise

      return tx
    },

    async commit(tx: PrismaTransactionClient): Promise<void> {
      const commit = (tx as any).__kronos_commit as () => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      commit()
      await txPromise
    },

    async rollback(tx: PrismaTransactionClient): Promise<void> {
      const rollback = (tx as any).__kronos_rollback as (error: unknown) => void
      const txPromise = (tx as any).__kronos_txPromise as Promise<void>
      rollback(new Error("Transaction rolled back"))
      // Swallow the expected rejection from the $transaction promise
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
 * prisma transaction actually lives — and the only things that can read it
 * are the two accessors below. That is what makes them TYPED: the type comes
 * from the adapter that owns the driver, not from an assertion at the call site.
 */
const registry = transactionRegistry<PrismaTransactionClient>()

/**
 * The unit-of-work factory this package exports: every unit of work it
 * mints runs inside one prisma transaction, begun before the handler,
 * committed at COMMIT and rolled back on error.
 *
 * Same shape as the core `unitOfWork`, so it drops into any seam that takes one:
 *
 * ```ts
 * const uow = prismaUnitOfWork(unitOfWork, prisma)
 * const commandBus = interceptingCommandBus(localCommandBus(uow), correlation)
 * processors: projections.map((p) => ({ ...p, eventStore, tokenStore, unitOfWork: uow }))
 * ```
 *
 * Everything on that unit of work — appended events, token updates, dead
 * letters, and the handler's own writes through {@link activePrismaTransaction} —
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
 * const uow = prismaUnitOfWork(() => correlating(unitOfWork(clock)), prisma)
 * //    ^ () => CorrelatingUnitOfWork, and its transactions are keyed on that
 * //      very object, which is the one `ctx.unitOfWork` hands back
 * ```
 */
export function prismaUnitOfWork<U extends UnitOfWork = UnitOfWork>(
  next: () => U,
  prisma: PrismaClientLike,
  options: PrismaTransactionOptions = {},
): () => U {
  return adapterUnitOfWork(registry, transactionHooks(prisma, options), next) as () => U
}

/**
 * The prisma transaction `uow` is running in, OPENING one if it has not begun.
 *
 * For writers that must be inside the transaction whether or not anything else
 * has touched it yet. Rejects when `uow` did not come from
 * {@link prismaUnitOfWork} — that is a wiring mistake, and answering `undefined`
 * would turn it into a silent non-transactional write.
 */
export function prismaTransaction(uow: UnitOfWork): Promise<PrismaTransactionClient> {
  return openTransaction(registry, uow, "prismaUnitOfWork")
}

/**
 * The prisma transaction `uow` is already running in, or `undefined` — NEVER
 * opens one.
 *
 * This is what a token store, a dead-letter queue or a projection writer wants:
 *
 * ```ts
 * const write = activePrismaTransaction(ctx.unitOfWork) ?? prisma
 * ```
 *
 * `undefined` means "not running in one of my transactions", so the caller
 * falls back to its plain handle instead of provoking a transaction nobody
 * asked for.
 */
export function activePrismaTransaction(
  uow: UnitOfWork | undefined,
): PrismaTransactionClient | undefined {
  return activeTransaction(registry, uow)
}

// ---------------------------------------------------------------------------
// THE EXTENSION STORY, in full.
//
// An extension to Kronos is four plain functions and a type. There is no
// plugin interface, no registry, no lifecycle to hook and nothing to subclass:
//
//   1. STORE IMPLEMENTATIONS — `prismaTokenStore`, `prismaDeadLetterQueue`, …
//      ordinary objects satisfying the framework's store interfaces.
//   2. A UNIT-OF-WORK WRAPPER — `prismaUnitOfWork(unitOfWork, prisma)`, a
//      unit-of-work factory that gives every unit of work a transaction.
//   3. A HANDLER WRAPPER — `prismaHandler(handler, prisma)`, which adds a capability to
//      the ctx a handler FUNCTION receives. The host spreads the entry.
//
// All share ONE piece of state — the uow-keyed registry above — which is
// what makes the capability and the transaction the same transaction.
// ---------------------------------------------------------------------------

/** The `prisma()` capability this extension adds to a handler context. */
export type PrismaCapability = {
  /**
   * This invocation's Prisma handle: the unit of work's transaction when one is
   * open, otherwise the base handle the wrapper was built with.
   *
   * Always safe to call. A handler written against it works unchanged whether
   * or not the seam it runs in was given a transactional factory — which is the
   * point, because that is a DEPLOYMENT decision and a slice should not encode
   * it. It never OPENS a transaction; use {@link prismaTransaction} for that.
   */
  prisma(): PrismaClientLike | PrismaTransactionClient
}


/**
 * Wrap a HANDLER FUNCTION — command, event or query — so its context gains
 * `prisma()`, the Prisma handle bound to whatever unit of work the invocation is
 * running in.
 *
 * It is a plain function over a plain function: it takes the handler that ASKS
 * for `prisma()` and returns one that asks only for the base context, having
 * supplied the difference. Nothing about a handler ENTRY appears in the type —
 * the host spreads the entry itself, which is also where any other field
 * (`descriptor`, `name`, `appendCondition`) survives untouched:
 *
 * ```ts
 * const editWidget = commandHandler(EditWidget, async ({ payload }, ctx: CommandHandlerContext & PrismaCapability) => {
 *   await ctx.prisma().widget.update({ where: { id: payload.id }, data: { name: payload.name } })
 * })
 *
 * kronos({
 *   commandHandlers: [editWidget]
 *     .map((h) => ({ ...h, handler: prismaHandler(h.handler, prisma) }))
 *     .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
 * })
 * ```
 *
 * The erasure is DIRECTIONAL — `prisma()` goes in, the base context comes out — so
 * ordering a chain wrongly (wrapping twice, or wrapping a handler that never
 * asked for `prisma()`) is a compile error rather than a runtime surprise.
 *
 * Build it from the SAME handle you built {@link prismaUnitOfWork} from. The
 * capability reads this extension's uow-keyed registry, so a handler's writes
 * and the unit of work's transaction are the same transaction and commit
 * together.
 */
export function prismaHandler<M, C extends PrismaCapability & { readonly unitOfWork: UnitOfWork }, R>(
  next: (message: M, context: C) => R,
  prisma: PrismaClientLike,
): (message: M, context: Omit<C, "prisma">) => R {
  return (message, context) =>
    next(message, {
      ...context,
      prisma: () =>
        activePrismaTransaction((context as { readonly unitOfWork: UnitOfWork }).unitOfWork) ?? prisma,
    } as unknown as C)
}
