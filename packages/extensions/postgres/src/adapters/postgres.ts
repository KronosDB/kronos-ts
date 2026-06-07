/**
 * postgres.js (porsager) reference adapter for @kronos-ts/postgres.
 *
 * Import via the sub-path:
 *   import { postgresAdapter } from "@kronos-ts/postgres/adapters/postgres"
 *
 * Notable porsager/postgres quirks handled here:
 *   - Default behaviour transforms column names (snake_case → camelCase).
 *     We DISABLE this because the SQL we author (and the SP signatures
 *     produced by schema.ts) use snake_case column names — letting the
 *     transform fire would produce row objects with `sequencePosition`
 *     instead of `sequence_position`, breaking the engine code.
 *   - Transactions: sql.begin returns the callback's value when it resolves
 *     and ROLLBACKs on rejection. We use the `BEGIN ISOLATION LEVEL ${lvl}`
 *     option since sql.begin accepts isolation as part of the BEGIN clause.
 *   - LISTEN: sql.listen(channel, cb) returns a Promise<{ unlisten() }>.
 *     The shape already matches our ListenSubscription contract.
 */

import postgresClient from "postgres"
import type { Sql } from "postgres"
import type {
  PostgresAdapter,
  PostgresAdapterTransaction,
  ListenSubscription,
  QueryRow,
} from "../adapter.js"
import { IsolationLevel } from "../adapter.js"

export interface PostgresAdapterConfig {
  readonly connectionString: string
  /** Additional postgres.js options. `transform.column.from` is forced off regardless. */
  readonly clientOptions?: Parameters<typeof postgresClient>[1]
}

export function postgresAdapter(config: PostgresAdapterConfig): PostgresAdapter {
  let sql: Sql | undefined
  let disconnected = false

  function getSql(): Sql {
    if (!sql) {
      sql = postgresClient(config.connectionString, {
        ...(config.clientOptions ?? {}),
        // Hard-override the column transform so our snake_case SQL works
        // verbatim. Users who pass a transform in clientOptions are told
        // (via JSDoc) that the column-from transform is ignored.
        transform: {
          ...(config.clientOptions?.transform ?? {}),
          column: { from: undefined as never, to: undefined as never },
        },
      })
    }
    return sql
  }

  return {
    async connect(): Promise<void> {
      const c = getSql()
      const rows = await c.unsafe<Array<{ ok: number }>>("SELECT 1 AS ok")
      if (rows[0]?.ok !== 1) {
        throw new Error("postgresAdapter.connect: unexpected health-check response")
      }
    },

    async disconnect(): Promise<void> {
      if (disconnected) return
      disconnected = true
      if (sql) {
        await sql.end({ timeout: 5 })
        sql = undefined
      }
    },

    async query<R extends QueryRow = QueryRow>(text: string, params?: unknown[]): Promise<R[]> {
      const c = getSql()
      const rows = (await c.unsafe(text, ((params as unknown[]) ?? []) as never[])) as unknown as R[]
      return rows
    },

    async queryOne<R extends QueryRow = QueryRow>(
      text: string,
      params?: unknown[],
    ): Promise<R | null> {
      const rows = await this.query<R>(text, params)
      if (rows.length === 0) return null
      if (rows.length > 1) {
        throw new Error(
          `postgresAdapter.queryOne: more than one row returned (got ${rows.length}). Use query() for multi-row results.`,
        )
      }
      return rows[0] ?? null
    },

    async transaction<T>(
      isolationLevel: IsolationLevel,
      fn: (tx: PostgresAdapterTransaction) => Promise<T>,
    ): Promise<T> {
      const c = getSql()
      // sql.begin's first argument is the BEGIN options; we pass the
      // isolation level. The callback receives a scoped `sql` that
      // pins to the underlying connection for the duration.
      return (await c.begin(`ISOLATION LEVEL ${isolationLevel}`, async (txSql) => {
        const tx: PostgresAdapterTransaction = {
          unwrap<T = unknown>(): T {
            return txSql as unknown as T
          },
          async query<R extends QueryRow = QueryRow>(
            text: string,
            params?: unknown[],
          ): Promise<R[]> {
            const rows = (await txSql.unsafe(text, ((params as unknown[]) ?? []) as never[])) as unknown as R[]
            return rows
          },
        }
        return fn(tx)
      })) as T
    },

    async listen(
      channel: string,
      onNotification: (payload: string | undefined) => void,
    ): Promise<ListenSubscription> {
      if (!/^[A-Za-z0-9_]+$/.test(channel)) {
        throw new Error(
          `postgresAdapter.listen: channel name must match /^[A-Za-z0-9_]+$/, got: ${channel}`,
        )
      }
      const c = getSql()
      const sub = await c.listen(channel, (payload) => onNotification(payload || undefined))
      return {
        async unlisten() {
          await sub.unlisten()
        },
      }
    },
  }
}
