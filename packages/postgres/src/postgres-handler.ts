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
import { activePostgresTransaction } from "./postgres-transaction.js"
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
   * This invocation's Postgres handle: the unit of work's transaction when one
   * is open, otherwise the pool the wrapper was built with.
   *
   * Always safe to call. A handler written against it works unchanged whether
   * or not the seam it runs in was given a transactional factory — which is the
   * point, because that is a DEPLOYMENT decision and a slice should not encode
   * it. It never OPENS a transaction; use `postgresTransaction` for that.
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
 *   await ctx.sql().query("UPDATE widgets SET name = $2 WHERE id = $1", [payload.id, payload.name])
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
 * Build it from the SAME pool you built `postgresUnitOfWork` from. The
 * capability reads this family's uow-keyed registry, so a handler's writes and
 * the unit of work's transaction are the same transaction and commit together.
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
  const wrapped = (message: unknown, context: any) => {
    const unitOfWork = (context as { readonly unitOfWork: UnitOfWork }).unitOfWork
    return next(message, {
      ...context,
      sql: () => {
        const active = activePostgresTransaction(unitOfWork)
        const trace = (context as { readonly trace?: SpanningTrace }).trace
        if (trace === undefined) return active ?? pg
        return active !== undefined ? observedTransactionView(active, trace) : observedPoolView(pg, trace)
      },
    })
  }

  return describe(wrapped, { name: "postgresHandler", supplies: ["sql"], next })
}
