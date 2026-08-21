/**
 * pg (node-postgres 8.20+) reference adapter for @kronos-ts/postgres.
 *
 * Import via the sub-path:
 *   import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"
 *
 * Implements every PostgresAdapter method per the contract in
 * ../adapter.ts. Pool sizing follows pg defaults; override via the
 * `poolConfig` field if needed.
 *
 * LISTEN uses a dedicated long-lived PoolClient pinned outside the pool
 * (never released) so notification delivery is not racing pool eviction.
 * That client is closed by `disconnect()`.
 */

import { Pool, type PoolClient, type PoolConfig } from "pg"
import type {
  PostgresAdapter,
  PostgresAdapterTransaction,
  ListenSubscription,
  QueryRow,
} from "../adapter.js"
import { IsolationLevel } from "../adapter.js"
import {
  type SessionTimeoutOptions,
  applySessionTimeouts,
  resolveSessionTimeouts,
} from "../session-timeouts.js"

export type PgAdapterConfig = SessionTimeoutOptions & {
  /** Standard libpq URI: postgresql://user:pass@host:port/db */
  readonly connectionString: string
  /** Optional pg.Pool config overrides (max connections, idleTimeoutMillis, etc.). */
  readonly poolConfig?: Omit<PoolConfig, "connectionString">
}

type ListenerSlot = {
  channel: string
  callback: (payload: string | undefined) => void
}

export function pgAdapter(config: PgAdapterConfig): PostgresAdapter {
  let pool: Pool | undefined
  let listenClient: PoolClient | undefined
  const listenSlots = new Map<string, Set<ListenerSlot>>()
  let disconnected = false
  const timeouts = resolveSessionTimeouts(config)

  function getPool(): Pool {
    if (!pool) {
      pool = new Pool({ connectionString: config.connectionString, ...config.poolConfig })
      pool.on("error", () => {
        // Connection-level errors on idle clients are non-fatal for the
        // adapter; pg removes the bad client from the pool automatically.
        // Surfacing them would force every consumer to attach a handler.
      })
    }
    return pool
  }

  async function ensureListenClient(): Promise<PoolClient> {
    if (listenClient) return listenClient
    listenClient = await getPool().connect()
    listenClient.on("notification", (msg) => {
      const slots = listenSlots.get(msg.channel)
      if (!slots) return
      for (const slot of slots) slot.callback(msg.payload)
    })
    return listenClient
  }

  const adapter: PostgresAdapter = {
    async connect(): Promise<void> {
      // Force pool creation + a no-op query so connect() actually verifies
      // reachability. Without this, a bad connection string would only
      // surface on the first real query.
      const result = await getPool().query<{ ok: number }>("SELECT 1 AS ok")
      if (result.rows[0]?.ok !== 1) {
        throw new Error("pgAdapter.connect(): unexpected health-check response")
      }
    },

    async disconnect(): Promise<void> {
      if (disconnected) return
      disconnected = true
      if (listenClient) {
        try {
          listenClient.release()
        } catch {
          /* ignore — client may already be gone */
        }
        listenClient = undefined
      }
      if (pool) {
        await pool.end()
        pool = undefined
      }
    },

    async query<R extends QueryRow = QueryRow>(sql: string, params?: unknown[]): Promise<R[]> {
      const result = await getPool().query<R>(sql, params as unknown[])
      return result.rows
    },

    async queryOne<R extends QueryRow = QueryRow>(
      sql: string,
      params?: unknown[],
    ): Promise<R | null> {
      const result = await getPool().query<R>(sql, params as unknown[])
      if (result.rows.length === 0) return null
      if (result.rows.length > 1) {
        throw new Error(
          `pgAdapter.queryOne: more than one row returned (got ${result.rows.length}). ` +
            `Use query() for multi-row results.`,
        )
      }
      return result.rows[0] ?? null
    },

    async transaction<T>(
      isolationLevel: IsolationLevel,
      fn: (tx: PostgresAdapterTransaction) => Promise<T>,
    ): Promise<T> {
      const client = await getPool().connect()
      try {
        await client.query(`BEGIN ISOLATION LEVEL ${isolationLevel}`)
        const tx: PostgresAdapterTransaction = {
          unwrap<T = unknown>(): T {
            return client as unknown as T
          },
          async query<R extends QueryRow = QueryRow>(
            sql: string,
            params?: unknown[],
          ): Promise<R[]> {
            const result = await client.query<R>(sql, params as unknown[])
            return result.rows
          },
        }
        let result: T
        try {
          // Arm the per-transaction safety timeouts before handing the tx to
          // the caller, so even the very first awaited statement is bounded.
          await applySessionTimeouts(tx, timeouts)
          result = await fn(tx)
        } catch (err) {
          // ROLLBACK best-effort; preserve the ORIGINAL error.
          try {
            await client.query("ROLLBACK")
          } catch {
            /* ignore rollback failure — original error matters more */
          }
          throw err
        }
        await client.query("COMMIT")
        return result
      } finally {
        client.release()
      }
    },

    async listen(
      channel: string,
      onNotification: (payload: string | undefined) => void,
    ): Promise<ListenSubscription> {
      const client = await ensureListenClient()
      const slot: ListenerSlot = { channel, callback: onNotification }
      let slots = listenSlots.get(channel)
      if (!slots) {
        slots = new Set()
        listenSlots.set(channel, slots)
        // Channel identifiers cannot be parameterised in pg's LISTEN — use
        // a safelist: alphanumerics + underscore only. Anything else is a
        // SQL-injection risk by definition.
        if (!/^[A-Za-z0-9_]+$/.test(channel)) {
          throw new Error(
            `pgAdapter.listen: channel name must match /^[A-Za-z0-9_]+$/, got: ${channel}`,
          )
        }
        await client.query(`LISTEN ${channel}`)
      }
      slots.add(slot)
      return {
        async unlisten() {
          const cur = listenSlots.get(channel)
          if (!cur) return
          cur.delete(slot)
          if (cur.size === 0) {
            listenSlots.delete(channel)
            try {
              await client.query(`UNLISTEN ${channel}`)
            } catch {
              /* connection may already be gone */
            }
          }
        },
      }
    },
  }

  return adapter
}
