/**
 * postgresPool — the RESOURCE the rest of this package is built on.
 *
 * There is no `postgres()` bundle any more. A bundle decided, on the host's
 * behalf, that you wanted an event store AND a snapshot store AND a scheduler
 * AND a unit-of-work factory, and returned them in a record you then had to
 * take apart. The pool is the one thing that genuinely has a LIFETIME — a
 * connection pool, opened and drained — and every store is an ordinary function
 * of it:
 *
 * ```ts
 * const pg = postgresPool(connectionString)
 * await pg.start()
 *
 * const eventStore    = postgresEventStore(pg, { serializer, tagResolver })
 * const snapshotStore = postgresSnapshotStore(pg, { serializer })
 * const tokenStore    = postgresTokenStore(pg)
 * const uow           = postgresUnitOfWork(pg, unitOfWork)
 * // …only the ones this deployment actually needs.
 *
 * await pg.close()
 * ```
 *
 * The resource IS a {@link PostgresAdapter} — it forwards every method — so a
 * function written against the adapter contract takes a pool unchanged. What it
 * adds is (a) the start/close pair, which owns connect + schema bootstrap, and
 * (b) {@link PostgresResource.tables}, the table names every store on this pool
 * reads and writes. Table names are a property of the DATABASE, not of each
 * store, which is why they are configured once here instead of six times.
 */

import type { ResilienceConfig } from "@kronos-ts/core"
import { withRetry } from "@kronos-ts/core"
import type {
  ListenSubscription,
  PostgresAdapter,
  PostgresAdapterTransaction,
  QueryRow,
} from "./adapter.js"
import type { IsolationLevel } from "./adapter.js"
import type { SessionTimeoutOptions } from "./session-timeouts.js"
import { bootstrapSchema, DEFAULT_TABLE_NAMES, type TableNames } from "./schema.js"

/**
 * A live Postgres pool: everything a {@link PostgresAdapter} does, plus the
 * lifetime and the table names.
 */
export interface PostgresResource extends PostgresAdapter {
  /**
   * The tables every store built on this pool reads and writes. Defaults to
   * {@link DEFAULT_TABLE_NAMES}; override on the pool, never per store.
   */
  readonly tables: TableNames
  /**
   * Open the pool and (unless `bootstrap: false`) create the schema.
   * Idempotent — a second call is a no-op.
   */
  start(): Promise<void>
  /** Drain the pool and release every socket. Idempotent. */
  close(): Promise<void>
}

export interface PostgresPoolOptions extends SessionTimeoutOptions {
  /**
   * Create the schema during {@link PostgresResource.start}. Defaults to true.
   * Set false when you run your own migrations — the DDL builders in
   * `./schema.js` are exported for exactly that.
   */
  readonly bootstrap?: boolean
  /** Override the default table names. */
  readonly tableNames?: TableNames
  /** Retry policy for the initial connect + bootstrap. */
  readonly resilience?: Partial<ResilienceConfig>
}

/**
 * Build a pool over a connection string, or wrap an adapter you built yourself.
 *
 * The connection-string form loads the `pg` (node-postgres) adapter at
 * {@link PostgresResource.start} time via a dynamic import, so the package
 * keeps its zero static dependency on any driver — a postgres.js-only or
 * Bun.sql-only deployment never resolves `pg`. Reach for the adapter form when
 * you want a different driver or non-default pool tuning:
 *
 * ```ts
 * postgresPool(bunSqlAdapter({ connectionString }))
 * postgresPool(pgAdapter({ connectionString, poolConfig: { max: 40 } }))
 * ```
 *
 * Passing an adapter hands over its lifecycle too: `start()` connects it and
 * `close()` disconnects it, so a host still has exactly one pair of calls to
 * write down.
 */
export function postgresPool(
  source: string | PostgresAdapter,
  options: PostgresPoolOptions = {},
): PostgresResource {
  const tables = options.tableNames ?? DEFAULT_TABLE_NAMES
  const bootstrap = options.bootstrap ?? true
  const { resilience } = options

  // Present from the start for the adapter form; resolved by start() for the
  // connection-string form, where loading a driver is asynchronous.
  let adapter: PostgresAdapter | undefined = typeof source === "string" ? undefined : source
  let started: Promise<void> | undefined
  let closed = false

  function handle(): PostgresAdapter {
    if (adapter === undefined) {
      throw new Error(
        "postgresPool: no connection yet — await pool.start() before using a pool built from a connection string.",
      )
    }
    return adapter
  }

  async function loadPgAdapter(connectionString: string): Promise<PostgresAdapter> {
    const { pgAdapter } = await import("./adapters/pg.js")
    return pgAdapter({
      connectionString,
      idleInTransactionTimeoutMs: options.idleInTransactionTimeoutMs,
      statementTimeoutMs: options.statementTimeoutMs,
    })
  }

  async function open(): Promise<void> {
    if (adapter === undefined && typeof source === "string") {
      adapter = await loadPgAdapter(source)
    }
    const pg = handle()
    await withRetry(() => pg.connect(), { event: "initial-connect", ...resilience })
    if (bootstrap) {
      await withRetry(() => bootstrapSchema(pg, { tableNames: tables }), {
        event: "initial-connect",
        ...resilience,
      })
    }
  }

  return {
    tables,

    start(): Promise<void> {
      // Cached rather than flag-guarded: two concurrent start() calls must
      // await ONE connect, not race two pools into existence.
      started ??= open()
      return started
    },

    async close(): Promise<void> {
      if (closed) return
      closed = true
      if (adapter !== undefined) await adapter.disconnect()
    },

    // `async` throughout so a use-before-start surfaces as a rejection, never
    // as a synchronous throw out of a call the caller is awaiting.
    async query<R extends QueryRow = QueryRow>(sql: string, params?: unknown[]): Promise<R[]> {
      return handle().query<R>(sql, params)
    },

    async queryOne<R extends QueryRow = QueryRow>(
      sql: string,
      params?: unknown[],
    ): Promise<R | null> {
      return handle().queryOne<R>(sql, params)
    },

    async transaction<T>(
      isolationLevel: IsolationLevel,
      fn: (tx: PostgresAdapterTransaction) => Promise<T>,
    ): Promise<T> {
      return handle().transaction(isolationLevel, fn)
    },

    async listen(
      channel: string,
      onNotification: (payload: string | undefined) => void,
    ): Promise<ListenSubscription> {
      return handle().listen(channel, onNotification)
    },

    // connect/disconnect are the adapter contract's half of the lifecycle, kept
    // so a pool is substitutable for the adapter it wraps. start()/close() are
    // what a host calls: they add the schema bootstrap and the idempotency.
    connect(): Promise<void> {
      return this.start()
    },

    disconnect(): Promise<void> {
      return this.close()
    },
  }
}
