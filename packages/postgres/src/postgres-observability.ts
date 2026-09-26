/**
 * Per-statement observability, on objects Kronos already owns.
 *
 * When an invocation's context carries `trace`, {@link postgresHandler} (in
 * `./postgres-handler.js`) hands it a SPANNED view of `ctx.sql()`: each
 * `query`/`queryOne` becomes one `"db.statement"` span,
 * and `unwrap()` returns a driver client whose own execute call is spanned
 * too — so a query builder issuing raw statements through the escape hatch
 * (Drizzle, say) is covered exactly like `ctx.sql().query(...)`.
 *
 * Every span is named `"db.statement"` and carries the statement's text as
 * `db.query.text`, OpenTelemetry's attribute for it. Parameters are never
 * recorded: the text is what the developer wrote, the parameters are what the
 * user sent. And nothing here imports
 * `@kronos-ts/otlp`: the trace this package spans onto is a minimal
 * structural type, {@link SpanningTrace}, so any object with a compatible
 * `span()` works.
 *
 * The per-driver knowledge (postgres.js / Bun.sql execute via a lazy
 * `client.unsafe(...)`; node-postgres executes via `client.query(...)`) is
 * detected structurally, off the PUBLIC shape `unwrap()` hands back — never
 * off which adapter produced it. That is what keeps the public surface to
 * exactly one name: `postgresHandler`.
 */

import type { PostgresAdapter, PostgresAdapterTransaction, QueryRow } from "./adapter.js"

/**
 * The one thing this package needs of a trace: the ability to wrap a
 * function so calling it runs inside a span, named and attributed. Deliberately
 * NOT `@kronos-ts/otlp`'s `Trace` — any object shaped like this works, which
 * keeps this package's dependency on tracing at zero.
 */
export type SpanningTrace = {
  span<F extends (...args: any[]) => any>(
    fn: F,
    options?: { name?: string; attributes?: Readonly<Record<string, string>> },
  ): F
}

const SPAN_NAME = "db.statement"
/** OpenTelemetry's attribute for the statement text. Parameters have no attribute here, ever. */
const QUERY_TEXT = "db.query.text"

/**
 * Run `fn` in one `db.statement` span carrying the statement's text — the SQL
 * as the code wrote it, placeholders and all — and never its parameters, which
 * is where user data travels. A call whose text cannot be read off its
 * arguments (a driver shape this package does not know) is spanned bare.
 */
function statement<T>(trace: SpanningTrace, text: string | undefined, fn: () => T): T {
  return trace.span(fn, {
    name: SPAN_NAME,
    ...(text !== undefined ? { attributes: { [QUERY_TEXT]: text } } : {}),
  })()
}

/**
 * The statement text a driver call carries. node-postgres takes either the
 * text or a config object with `text`; postgres.js / Bun.sql take the text
 * on `.unsafe(text, params)` and template strings on the tag itself, which
 * are joined back with `$n` placeholders — the same statement the driver
 * sends, parameters left out.
 */
function textOf(first: unknown): string | undefined {
  if (typeof first === "string") return first
  if (Array.isArray(first) && "raw" in first) {
    return (first as ReadonlyArray<string>).reduce((sql, part, index) => `${sql}$${index}${part}`)
  }
  if (typeof first === "object" && first !== null && typeof (first as { text?: unknown }).text === "string") {
    return (first as { text: string }).text
  }
  return undefined
}

/**
 * Wraps a lazy postgres.js / Bun.sql pending query — the thenable
 * `client.unsafe(...)` returns, chainable via `.values()` / `.raw()`.
 *
 * Timing must start when the statement actually EXECUTES (i.e. is awaited,
 * or `.then()`ed directly), not when `.unsafe()` is merely called — that is
 * how postgres.js and Bun.sql behave natively, and wrapping must not change
 * it. `.values()` / `.raw()` return a further pending query, so they are
 * wrapped recursively rather than spanned themselves.
 */
function wrapLazyQuery<T>(pending: T, trace: SpanningTrace, text: string | undefined): T {
  if (pending === null || (typeof pending !== "object" && typeof pending !== "function")) return pending
  return new Proxy(pending as object, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, target)
      if (prop === "then" && typeof value === "function") {
        return (...args: unknown[]) =>
          statement(trace, text, () => (value as (...a: unknown[]) => unknown).apply(target, args))
      }
      if ((prop === "values" || prop === "raw") && typeof value === "function") {
        return (...args: unknown[]) =>
          wrapLazyQuery((value as (...a: unknown[]) => unknown).apply(target, args), trace, text)
      }
      return typeof value === "function" ? value.bind(target) : value
    },
  }) as T
}

/** postgres.js / Bun.sql shape: a callable `sql` tag with `.unsafe(text, params)`. */
function observePostgresJsLikeClient<T>(client: T, trace: SpanningTrace): T {
  return new Proxy(client as object, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, target)
      if (prop === "unsafe" && typeof value === "function") {
        return (...args: unknown[]) =>
          wrapLazyQuery((value as (...a: unknown[]) => unknown).apply(target, args), trace, textOf(args[0]))
      }
      return typeof value === "function" ? value.bind(target) : value
    },
    apply(target, thisArg, args) {
      const pending = Reflect.apply(target as (...a: unknown[]) => unknown, target, args)
      return wrapLazyQuery(pending, trace, textOf(args[0]))
    },
  }) as T
}

/** node-postgres shape: a `PoolClient`/`Client` executing via `.query(...)`, eagerly. */
function observePgLikeClient<T extends { query: (...args: unknown[]) => unknown }>(
  client: T,
  trace: SpanningTrace,
): T {
  const query = (...args: unknown[]) => statement(trace, textOf(args[0]), () => client.query(...args))
  return new Proxy(client as object, {
    get(target, prop, receiver) {
      if (prop === "query") return query
      const value = Reflect.get(target, prop, target)
      return typeof value === "function" ? value.bind(target) : value
    },
  }) as T
}

/**
 * Dispatches on the unwrapped client's PUBLIC shape alone. A driver this
 * package has never heard of, that exposes neither shape, passes through
 * un-instrumented rather than throwing — observability is additive, never a
 * reason to break `unwrap()`.
 */
function observeUnwrappedClient<T>(client: T, trace: SpanningTrace): T {
  if (typeof client === "function") return observePostgresJsLikeClient(client, trace)
  if (client !== null && typeof client === "object" && typeof (client as { query?: unknown }).query === "function") {
    return observePgLikeClient(client as T & { query: (...args: unknown[]) => unknown }, trace)
  }
  return client
}

/**
 * A fresh, per-invocation spanned VIEW of an open transaction. `query` is
 * spanned directly; `unwrap()` returns a spanned driver client bound to the
 * same trace, so an external query builder issuing statements on the escape
 * hatch is covered too.
 */
export function observedTransactionView(
  tx: PostgresAdapterTransaction,
  trace: SpanningTrace,
): PostgresAdapterTransaction {
  return {
    query: <R extends QueryRow = QueryRow>(sql: string, params?: unknown[]) =>
      statement(trace, sql, () => tx.query<R>(sql, params)),
    unwrap<T = unknown>(): T {
      return observeUnwrappedClient(tx.unwrap<T>(), trace)
    },
  }
}

/**
 * A fresh, per-invocation spanned VIEW of the pool — used when `ctx.sql()`
 * has no open transaction to view. Everything besides `query`/`queryOne`/
 * `unwrap` (`transaction`, `listen`, `connect`, `disconnect`) passes straight
 * through to `pg` unchanged; those are not per-statement calls. `unwrap()` is
 * spanned the same way the transaction view's is, so a query builder built
 * over a `ctx.sql()` handle is covered whether or not a transaction happens
 * to be open.
 */
export function observedPoolView(pg: PostgresAdapter, trace: SpanningTrace): PostgresAdapter {
  return {
    ...pg,
    query: <R extends QueryRow = QueryRow>(sql: string, params?: unknown[]) =>
      statement(trace, sql, () => pg.query<R>(sql, params)),
    queryOne: <R extends QueryRow = QueryRow>(sql: string, params?: unknown[]) =>
      statement(trace, sql, () => pg.queryOne<R>(sql, params)),
    unwrap<T = unknown>(): T {
      return observeUnwrappedClient(pg.unwrap<T>(), trace)
    },
  }
}
