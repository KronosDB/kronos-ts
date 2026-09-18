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
 * Nothing here records SQL text or parameters — the span name is the static
 * string `"db.statement"`, full stop. And nothing here imports
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
 * function so calling it runs inside a span. Deliberately NOT
 * `@kronos-ts/otlp`'s `Trace` — any object shaped like this works, which
 * keeps this package's dependency on tracing at zero.
 */
export type SpanningTrace = {
  span<F extends (...args: any[]) => any>(fn: F, options?: { name?: string }): F
}

const SPAN_NAME = "db.statement"

function spanned<F extends (...args: any[]) => any>(trace: SpanningTrace, fn: F): F {
  return trace.span(fn, { name: SPAN_NAME })
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
function wrapLazyQuery<T>(pending: T, trace: SpanningTrace): T {
  if (pending === null || (typeof pending !== "object" && typeof pending !== "function")) return pending
  return new Proxy(pending as object, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, target)
      if (prop === "then" && typeof value === "function") {
        return spanned(trace, (...args: unknown[]) => (value as (...a: unknown[]) => unknown).apply(target, args))
      }
      if ((prop === "values" || prop === "raw") && typeof value === "function") {
        return (...args: unknown[]) =>
          wrapLazyQuery((value as (...a: unknown[]) => unknown).apply(target, args), trace)
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
          wrapLazyQuery((value as (...a: unknown[]) => unknown).apply(target, args), trace)
      }
      return typeof value === "function" ? value.bind(target) : value
    },
    apply(target, thisArg, args) {
      const pending = Reflect.apply(target as (...a: unknown[]) => unknown, target, args)
      return wrapLazyQuery(pending, trace)
    },
  }) as T
}

/** node-postgres shape: a `PoolClient`/`Client` executing via `.query(...)`, eagerly. */
function observePgLikeClient<T extends { query: (...args: unknown[]) => unknown }>(
  client: T,
  trace: SpanningTrace,
): T {
  const query = spanned(trace, (...args: unknown[]) => client.query(...args))
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
    query: spanned(trace, <R extends QueryRow = QueryRow>(sql: string, params?: unknown[]) =>
      tx.query<R>(sql, params),
    ),
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
 * once per `ctx.sql()` handle (e.g. `@kronos-ts/postgres/drizzle`) is covered
 * whether or not a transaction happens to be open.
 */
export function observedPoolView(pg: PostgresAdapter, trace: SpanningTrace): PostgresAdapter {
  return {
    ...pg,
    query: spanned(trace, <R extends QueryRow = QueryRow>(sql: string, params?: unknown[]) => pg.query<R>(sql, params)),
    queryOne: spanned(trace, <R extends QueryRow = QueryRow>(sql: string, params?: unknown[]) =>
      pg.queryOne<R>(sql, params),
    ),
    unwrap<T = unknown>(): T {
      return observeUnwrappedClient(pg.unwrap<T>(), trace)
    },
  }
}
