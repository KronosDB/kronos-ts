/**
 * Bun.sql adapter for @kronos-ts/postgres.
 *
 * Import via the sub-path (Bun runtime only):
 *   import { bunSqlAdapter } from "@kronos-ts/postgres/adapters/bun-sql"
 *
 * Requires Bun >= 1.2 for Bun.SQL (sql.transaction()). LISTEN support is
 * feature-detected; if Bun.SQL lacks native LISTEN on the running version,
 * the adapter falls back to a 250ms polling shim — slower wake-up but
 * correct semantics.
 *
 * This file uses `globalThis as { Bun?: ... }` access to avoid a hard
 * compile-time reference to Bun's global, so the package can ship under
 * Node (where this file would never be imported) without TypeScript
 * compilation errors. Users importing the sub-path under Node will get
 * a clear runtime error from the connect() call.
 */

import type {
  PostgresAdapter,
  PostgresAdapterTransaction,
  ListenSubscription,
  QueryRow,
} from "../adapter.js"
import { IsolationLevel } from "../adapter.js"

export interface BunSqlAdapterConfig {
  readonly connectionString: string
}

// Minimal structural type for Bun.SQL we depend on (kept local to avoid
// a hard dependency on @types/bun).
interface BunSqlInstance {
  (template: TemplateStringsArray, ...values: unknown[]): Promise<unknown[]>
  unsafe(text: string, params?: unknown[]): Promise<unknown[]>
  begin<T>(
    isolation: string,
    fn: (sql: BunSqlInstance) => Promise<T>,
  ): Promise<T>
  listen?(channel: string, cb: (payload: string) => void): Promise<{ unlisten: () => Promise<void> }>
  close(): Promise<void>
  end(): Promise<void>
}

/**
 * Bun.SQL surfaces SQLSTATE in `err.errno` (not `err.code`). The adapter
 * contract (D-12.12) requires SQLSTATE on `.code` so that `isDcbViolation(err)`
 * works uniformly across all adapters.
 *
 * This helper normalises Bun.SQL errors: if `err.code === "ERR_POSTGRES_SERVER_ERROR"`
 * and `err.errno` is a non-empty string, we copy `errno` to `code` before
 * re-throwing, giving callers the SQLSTATE they expect on `.code`.
 */
function normalizeBunSqlError(err: unknown): never {
  if (
    typeof err === "object" &&
    err !== null &&
    (err as { code?: string }).code === "ERR_POSTGRES_SERVER_ERROR" &&
    typeof (err as { errno?: unknown }).errno === "string" &&
    (err as { errno: string }).errno.length > 0
  ) {
    ;(err as { code: string }).code = (err as { errno: string }).errno
  }
  throw err
}

interface BunSqlConstructor {
  new (config: { url: string }): BunSqlInstance
}

function getBunSql(): BunSqlConstructor {
  const g = globalThis as { Bun?: { SQL?: BunSqlConstructor } }
  if (!g.Bun?.SQL) {
    throw new Error(
      "bunSqlAdapter requires the Bun runtime with built-in Bun.SQL (>= 1.2). " +
        "Detected non-Bun runtime — use pgAdapter or postgresAdapter instead.",
    )
  }
  return g.Bun.SQL
}

export function bunSqlAdapter(config: BunSqlAdapterConfig): PostgresAdapter {
  let sql: BunSqlInstance | undefined
  let disconnected = false

  function getInstance(): BunSqlInstance {
    if (!sql) {
      const SQL = getBunSql()
      sql = new SQL({ url: config.connectionString })
    }
    return sql
  }

  return {
    async connect(): Promise<void> {
      const inst = getInstance()
      const rows = await inst.unsafe("SELECT 1 AS ok").catch(normalizeBunSqlError) as Array<{ ok: number }>
      if (rows[0]?.ok !== 1) {
        throw new Error("bunSqlAdapter.connect: unexpected health-check response")
      }
    },

    async disconnect(): Promise<void> {
      if (disconnected) return
      disconnected = true
      if (sql) {
        // Bun.SQL exposes both close() and end() — try end() first (graceful
        // drain), fall back to close() if end is unavailable.
        try {
          if (typeof (sql as unknown as { end?: () => Promise<void> }).end === "function") {
            await (sql as unknown as { end: () => Promise<void> }).end()
          } else {
            await sql.close()
          }
        } catch {
          /* ignore disconnect errors */
        }
        sql = undefined
      }
    },

    async query<R extends QueryRow = QueryRow>(text: string, params?: unknown[]): Promise<R[]> {
      const inst = getInstance()
      return inst.unsafe(text, params ?? []).catch(normalizeBunSqlError) as Promise<R[]>
    },

    async queryOne<R extends QueryRow = QueryRow>(
      text: string,
      params?: unknown[],
    ): Promise<R | null> {
      const rows = await this.query<R>(text, params)
      if (rows.length === 0) return null
      if (rows.length > 1) {
        throw new Error(
          `bunSqlAdapter.queryOne: more than one row returned (got ${rows.length}). Use query() for multi-row results.`,
        )
      }
      return rows[0] ?? null
    },

    async transaction<T>(
      isolationLevel: IsolationLevel,
      fn: (tx: PostgresAdapterTransaction) => Promise<T>,
    ): Promise<T> {
      const inst = getInstance()
      // Bun.SQL.begin(isolation, fn) starts a transaction at the given isolation
      // level. The callback receives a scoped sql instance pinned to the
      // underlying connection. SQLSTATE errors from within the transaction are
      // normalized (errno -> code) via normalizeBunSqlError before propagating.
      return inst.begin(`ISOLATION LEVEL ${isolationLevel}`, async (txSql) => {
        const tx: PostgresAdapterTransaction = {
          async query<R extends QueryRow = QueryRow>(
            text: string,
            params?: unknown[],
          ): Promise<R[]> {
            return txSql.unsafe(text, params ?? []).catch(normalizeBunSqlError) as Promise<R[]>
          },
        }
        return fn(tx)
      }).catch(normalizeBunSqlError)
    },

    async listen(
      channel: string,
      onNotification: (payload: string | undefined) => void,
    ): Promise<ListenSubscription> {
      if (!/^[A-Za-z0-9_]+$/.test(channel)) {
        throw new Error(
          `bunSqlAdapter.listen: channel name must match /^[A-Za-z0-9_]+$/, got: ${channel}`,
        )
      }
      const inst = getInstance()
      // Feature-detect native LISTEN
      if (typeof inst.listen === "function") {
        const sub = await inst.listen(channel, (p) => onNotification(p || undefined))
        return {
          async unlisten() {
            await sub.unlisten()
          },
        }
      }
      // Polling fallback for Bun versions without native LISTEN.
      // The fallback fires the callback on every tick with undefined payload,
      // which causes the streaming engine to re-poll. This is coarse but
      // semantically correct — the caller (open() in postgres-event-store.ts)
      // already falls back to 250ms polling as a safety net, so this shim
      // merely triggers additional pump cycles.
      let stopped = false
      const tick = setInterval(() => {
        if (stopped) {
          clearInterval(tick)
          return
        }
        onNotification(undefined)
      }, 250)
      return {
        async unlisten() {
          stopped = true
          clearInterval(tick)
        },
      }
    },
  }
}
