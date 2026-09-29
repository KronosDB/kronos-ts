/**
 * THE FAMILY, in full.
 *
 * A persistence family for Kronos is plain functions and a type. There is no
 * plugin interface, no registry, no lifecycle to hook and nothing to subclass:
 *
 *   1. A RESOURCE            — `postgresPool(connectionString)`.
 *   2. STORE IMPLEMENTATIONS — `postgresEventStore`,
 *      `postgresSnapshottingEventStore`,
 *      `postgresTokenStore`, `postgresDeadLetterQueue`: ordinary objects
 *      satisfying the framework's store interfaces.
 *   3. A UNIT-OF-WORK WRAPPER — `postgresUnitOfWork(unitOfWork, pg)`, which
 *      gives every unit of work a transaction.
 *   4. A HANDLER WRAPPER      — `postgresHandler(handler, pg)`, which adds a capability
 *      to the ctx a handler FUNCTION receives. The host spreads the entry.
 *
 * All of them share ONE piece of state — the uow-keyed registry in
 * `./postgres-transaction.js` — which is what makes the capability and the
 * transaction the SAME transaction. That is the family's whole premise:
 * persistence families are keyed by transaction identity, so never mix two
 * within one processor.
 */

import {
  describe,
  type DeclaresInside,
  type Described,
  type UnitOfWork,
} from "@kronos-ts/core"
import type { PostgresAdapter, PostgresAdapterTransaction } from "./adapter.js"
import { activePostgresTransaction, sharedPostgresTransaction } from "./postgres-transaction.js"
import { observedPoolView, observedTransactionView, type SpanningTrace } from "./postgres-observability.js"

/**
 * The one order rule this wrapper owns: statements are spanned onto the
 * `ctx.trace` it RECEIVES, so a wrapper that supplies `trace` must sit outside
 * it. Inside, the trace arrives too late and no statement is ever spanned —
 * silently — so it is refused in the type with a sentence.
 */
/** The handler must DEMAND `ctx.sql()`; wrapping one that does not (or wrapping twice) is refused with a sentence. */
type DemandsSql<H> = H extends (message: any, context: infer C) => any
  ? C extends PostgresCapability & { readonly unitOfWork: UnitOfWork }
    ? unknown
    : "postgresHandler supplies ctx.sql(), but the handler it wraps does not demand it — wrap the handler that names PostgresCapability, and only once"
  : never

type TraceInside<H> = DeclaresInside<H, "supplies", "trace"> extends true
  ? "a wrapper that supplies ctx.trace is inside postgresHandler: move it outside, so statements are spanned"
  : unknown

/** The pool-level handle — what `sql()` returns outside a transaction. */
export type Sql = PostgresAdapter
/** The transaction-level handle — what `sql()` returns inside one. */
export type Tx = PostgresAdapterTransaction

/** The `sql()` capability this family adds to a handler context. */
export type PostgresCapability = {
  /**
   * This invocation's Postgres handle. In an event handler, the unit of work's
   * transaction when one is open, otherwise the pool the wrapper was built
   * with. In a command handler, always the pool: a command's own writes commit
   * as they run, before its events are appended, so write them idempotently.
   *
   * Always safe to call. A handler written against it works unchanged whether
   * or not the seam it runs in was given a transactional factory — which is the
   * point, because that is a DEPLOYMENT decision and a slice should not encode
   * it. The accessor itself never OPENS a transaction: when an event handler's
   * unit of work is this family's, `postgresHandler` opened it before the
   * handler ran, so the handler's statements commit with the token.
   *
   * Both arms answer `query(sql, params)`, so the common case needs no
   * narrowing:
   *
   * ```ts
   * await ctx.sql().query("UPDATE widgets SET name = $2 WHERE id = $1", [id, name])
   * ```
   */
  sql(): Sql | Tx
}


/**
 * Wrap a HANDLER FUNCTION — command, event or query — so its context gains
 * `sql()`, the Postgres handle bound to whatever unit of work the invocation is
 * running in.
 *
 * ONE function for all three. The three kinds differ in the context they
 * receive, and the capability is added the same way to each, so three exported
 * names were three spellings of one operation. Nothing about a handler ENTRY
 * appears in the type: the host spreads the entry, which is also where
 * `descriptor`, `name` and `appendCondition` survive untouched.
 *
 * ```ts
 * const editWidget = commandHandler(EditWidget, async ({ payload }, ctx: CommandHandlerContext & PostgresCapability) => {
 *   await ctx.sql().query(
 *     "INSERT INTO widget_names (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
 *     [payload.id, payload.name],
 *   )
 *   ctx.append(WidgetUpdated, payload)
 * })
 *
 * kronos({
 *   commandHandlers: [editWidget]
 *     .map((h) => ({ ...h, handler: postgresHandler(h.handler, pg) }))
 *     .map((h) => ({ ...h, eventStore, commandBus, queryBus })),
 * })
 * ```
 *
 * The erasure is DIRECTIONAL — `sql()` goes in, the base context comes out — so
 * ordering a chain wrongly (wrapping twice, or wrapping a handler that never
 * asked for `sql()`) is a compile error rather than a runtime surprise.
 *
 * Build it from the SAME pool you built `postgresUnitOfWork` from. In an event
 * handler the capability reads this family's uow-keyed registry, so the
 * handler's writes and the processor's token commit together. A command
 * handler's writes never join its unit of work's transaction: they run on the
 * pool, while the handler runs, and a rejected append does not undo them.
 *
 * STATEMENTS ARE SPANNED WHEN THERE IS A TRACE. If the context this wrapper
 * receives carries `trace` (supplied by a wrapper OUTSIDE this one — see
 * `@kronos-ts/otlp`'s `otlpHandler`), every statement `ctx.sql()` runs becomes
 * one `"db.statement"` span on it, including statements a query builder issues
 * through `unwrap()`. Nothing to switch on, and no SQL text or parameter is
 * ever recorded. With no `trace`, the handle is the plain pool or transaction.
 */
export function postgresHandler<H extends (message: any, context: any) => any>(
  next: H & DemandsSql<H> & TraceInside<H>,
  pg: PostgresAdapter,
): ((message: Parameters<H>[0], context: Omit<Parameters<H>[1], "sql">) => ReturnType<H>) &
  Described<{
    readonly name: "postgresHandler"
    readonly supplies: readonly ["sql"]
    readonly next: H
  }>
export function postgresHandler(
  next: (message: unknown, context: any) => unknown,
  pg: PostgresAdapter,
): unknown {
  const invoke = (message: unknown, context: any, transaction: () => Tx | undefined) =>
    next(message, {
      ...context,
      sql: () => {
        const active = transaction()
        const trace = (context as { readonly trace?: SpanningTrace }).trace
        if (trace === undefined) return active ?? pg
        return active !== undefined ? observedTransactionView(active, trace) : observedPoolView(pg, trace)
      },
    })
  const wrapped = (message: unknown, context: any) => {
    // A COMMAND's statements run on the pool and commit as they go — never in
    // the transaction its events are appended in, even when something (a
    // `ctx.schedule`) has opened it. Holding that transaction across the
    // handler pinned a connection while `ctx.load` waited for a second one
    // from the same pool, so a burst of commands the size of the pool
    // deadlocked. A command's own writes are idempotent instead: the handler
    // runs before its append, and a retry writes them again.
    if ((message as { readonly kind?: unknown }).kind === "command") {
      return invoke(message, context, () => undefined)
    }
    const unitOfWork = (context as { readonly unitOfWork: UnitOfWork }).unitOfWork
    const transaction = () => activePostgresTransaction(unitOfWork)
    if (transaction() !== undefined) return invoke(message, context, transaction)
    // The unit of work's transaction is LAZY, and in a processor batch the
    // handler is usually its first writer: nothing else has opened it when the
    // handler runs. Open it here when the unit of work is this family's, so the
    // handler's statements land in the transaction that commits with the token
    // — not on the pool, one autocommit statement at a time. A unit of work
    // that is not ours opens nothing, and `sql()` stays the pool.
    return sharedPostgresTransaction(unitOfWork).then(() => invoke(message, context, transaction))
  }

  return describe(wrapped, { name: "postgresHandler", supplies: ["sql"], next })
}
