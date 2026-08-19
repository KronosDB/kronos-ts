/**
 * Driver-agnostic adapter contract for @kronos-ts/postgres.
 *
 * Per D-12.05 the package itself has zero direct dependency on any specific
 * Postgres client library. Engine code (Plan 04) talks to this interface;
 * adapter implementations (pg, postgres.js, Bun.sql) live under
 * `./adapters/*` and are imported via package sub-path exports (D-12.08).
 *
 * Capability coverage per D-12.07:
 *   - query execution (parameterised)         -> query / queryOne
 *   - transactions with isolation level       -> transaction(isolationLevel, fn)
 *   - LISTEN-style subscription               -> listen(channel, onNotification)
 *   - lifecycle                               -> connect / disconnect
 *
 * Error contract per D-12.12: SQLSTATE on thrown errors as `.code` (string).
 * Adapter implementations MUST NOT swallow / rewrap SQLSTATE-bearing errors
 * — the engine relies on `isDcbViolation(err)` reading `err.code === "KR001"`.
 */

/**
 * Postgres isolation levels the engine actually emits. SQL string values are
 * the verbatim Postgres syntax used after `SET TRANSACTION ISOLATION LEVEL`,
 * so adapters can interpolate directly without a lookup table.
 */
export const IsolationLevel = {
  READ_COMMITTED: "READ COMMITTED",
  REPEATABLE_READ: "REPEATABLE READ",
  SERIALIZABLE: "SERIALIZABLE",
} as const
export type IsolationLevel = (typeof IsolationLevel)[keyof typeof IsolationLevel]

/** Plain row shape returned by query/queryOne. Adapter implementations cast
 *  driver-specific row objects to this. */
export type QueryRow = Record<string, unknown>

/**
 * Active in-transaction handle. Pinned to a single underlying connection so
 * statements never interleave with sibling pool traffic. Intentionally NO
 * nested-transaction method — Plan 04's engine does not need it and savepoints
 * would invite confusion about which level a SQLSTATE error propagates from.
 */
export interface PostgresAdapterTransaction {
  query<R extends QueryRow = QueryRow>(sql: string, params?: unknown[]): Promise<R[]>
  /**
   * Escape hatch returning the live driver-specific handle backing this
   * transaction — the pg `PoolClient`, or the scoped `sql` for postgres.js /
   * Bun.sql. Lets an external query builder (e.g. Drizzle) issue statements on
   * the SAME connection, and therefore the SAME transaction, as the engine's
   * appends — so an application's CRUD writes commit or roll back atomically
   * with its events.
   *
   * The caller owns the cast (the handle type is driver-specific) and the
   * handle is valid ONLY for the lifetime of this transaction — never retain
   * it past the UoW that opened it.
   */
  unwrap<T = unknown>(): T
}

/** Handle to a live LISTEN subscription. unlisten() unregisters + releases
 *  the dedicated connection (if any). */
export interface ListenSubscription {
  unlisten(): Promise<void>
}

/**
 * The single interface the engine layer (Plan 04+) consumes. All concurrency
 * (pool sizing, idle eviction, reconnect) lives below this seam — the engine
 * code MUST NOT know about pools.
 */
export interface PostgresAdapter {
  /**
   * Run a parameterised SQL statement on a pool-borrowed connection.
   * Returns rows; empty array if none. Thrown errors carry SQLSTATE on
   * `.code` unchanged so `isDcbViolation(err)` works at any call site.
   */
  query<R extends QueryRow = QueryRow>(sql: string, params?: unknown[]): Promise<R[]>

  /**
   * Convenience for single-row queries. Returns `null` if zero rows.
   * Throws if more than one row is returned (caller bug — use query()).
   */
  queryOne<R extends QueryRow = QueryRow>(sql: string, params?: unknown[]): Promise<R | null>

  /**
   * Open a transaction at the given isolation level, run `fn` against a
   * pinned-connection handle, and COMMIT on resolution or ROLLBACK on
   * rejection. If `fn` throws, the original error is re-thrown after
   * ROLLBACK; rollback failures do NOT mask the original.
   *
   * The framework's AppendTransaction has a synchronous `rollback(): void`
   * (see packages/eventsourcing/src/event-storage-engine.ts) — the
   * implementation translates that into a fire-and-forget reject inside
   * this method's promise, NOT an awaited rollback round-trip.
   */
  transaction<T>(
    isolationLevel: IsolationLevel,
    fn: (tx: PostgresAdapterTransaction) => Promise<T>,
  ): Promise<T>

  /**
   * Subscribe to `LISTEN <channel>` on a dedicated long-lived connection.
   * Adapter implementations that lack a real LISTEN channel (e.g. Bun.sql
   * pre-1.x) MAY implement a polling shim — the subscription handle is the
   * same regardless. Plan 05's streaming open() uses this to wake up
   * tailers immediately instead of waiting for a poll tick.
   */
  listen(
    channel: string,
    onNotification: (payload: string | undefined) => void,
  ): Promise<ListenSubscription>

  /** Initialise pool / verify reachability. Idempotent. */
  connect(): Promise<void>

  /** Drain pool, close LISTEN connections, release sockets. Idempotent. */
  disconnect(): Promise<void>
}
