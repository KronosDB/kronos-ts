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
  type Described,
  type UnitOfWork,
} from "@kronos-ts/core"
import type { PostgresAdapter, PostgresAdapterTransaction } from "./adapter.js"
import { activePostgresTransaction } from "./postgres-transaction.js"
import {
  isObservedPostgres,
  observedPoolView,
  observedTransactionView,
  type ObservedPostgres,
  type SpanningTrace,
} from "./postgres-observability.js"

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
 * Building it from an {@link observed} pool is the OBSERVABILITY overload: the
 * handler's context must then also carry `trace` (supplied by a wrapper
 * OUTSIDE this one — see `@kronos-ts/otlp`'s `otlpHandler`), and every
 * statement `ctx.sql()` runs becomes one `"db.statement"` span on it:
 *
 * ```ts
 * const handler = postgresHandler(myHandler, observed(pg))
 * //    ^ (message, ctx: Omit<C, "sql"> & { trace }) => R
 * ```
 */
export function postgresHandler<M, C extends PostgresCapability & { readonly unitOfWork: UnitOfWork }, R>(
  next: (message: M, context: C) => R,
  pg: PostgresAdapter & ObservedPostgres,
): ((message: M, context: Omit<C, "sql"> & { readonly trace: SpanningTrace }) => R) &
  Described<{
    readonly name: "postgresHandler"
    readonly supplies: readonly ["sql"]
    readonly uses: readonly ["trace"]
    readonly next: (message: M, context: C) => R
    readonly hints: Readonly<{ trace: string }>
  }>
export function postgresHandler<M, C extends PostgresCapability & { readonly unitOfWork: UnitOfWork }, R>(
  next: (message: M, context: C) => R,
  pg: PostgresAdapter,
): ((message: M, context: Omit<C, "sql">) => R) &
  Described<{
    readonly name: "postgresHandler"
    readonly supplies: readonly ["sql"]
    readonly next: (message: M, context: C) => R
  }>
export function postgresHandler(
  next: (message: unknown, context: any) => unknown,
  pg: PostgresAdapter,
): unknown {
  const observedPg = isObservedPostgres(pg) ? pg : undefined

  const wrapped = (message: unknown, context: any) => {
    const unitOfWork = (context as { readonly unitOfWork: UnitOfWork }).unitOfWork
    return next(message, {
      ...context,
      sql: () => {
        const active = activePostgresTransaction(unitOfWork)
        if (observedPg === undefined) return active ?? pg
        const trace = (context as { readonly trace?: SpanningTrace }).trace
        if (trace === undefined) return active ?? pg
        return active !== undefined ? observedTransactionView(active, trace) : observedPoolView(observedPg, trace)
      },
    })
  }

  return observedPg === undefined
    ? describe(wrapped, { name: "postgresHandler", supplies: ["sql"], next })
    : describe(wrapped, {
        name: "postgresHandler",
        supplies: ["sql"],
        uses: ["trace"],
        next,
        hints: {
          trace:
            "Put the tracing wrapper (otlpHandler) outside postgresHandler, or build postgresHandler from a plain pool instead of observed(pg) if statement spans are not wanted.",
        },
      })
}
