/**
 * postgresTokenStore — a {@link TokenStore} over raw SQL. No ORM required.
 *
 * The rows are the SAME rows every other persistence family writes
 * (`kronos_token_entries`, see `./schema.js`), so a deployment can move between
 * families without a migration. What differs is only the client — and the
 * client is the point:
 *
 * PRINCIPLE: every write goes through {@link activePostgresTransaction}, so a
 * token update lands in the SAME postgres transaction as the handler's own
 * `ctx.sql()` writes and the event store's appends. A crash cannot advance a
 * processor's token while losing the work that token accounts for. Passing no
 * unit of work — lifecycle and admin paths — writes on the pool, outside any
 * transaction, which is what those paths want.
 */

import type { TokenStore, TrackingToken, UnitOfWork } from "@kronos-ts/core"
import {
  UnableToClaimTokenError,
  deserializeToken as deserializeTokenData,
  serializeToken as serializeTokenData,
} from "@kronos-ts/core"
import type { QueryRow } from "./adapter.js"
import type { PostgresResource } from "./postgres-pool.js"
import { activePostgresTransaction } from "./postgres-transaction.js"

/** Tuning only — everything required is the positional pool. */
export interface PostgresTokenStoreOptions {
  /**
   * How long a claim survives without being extended, ms. Default 10000 —
   * the same figure the other families use, so a mixed fleet does not
   * disagree about when a dead node's segment becomes available.
   */
  readonly claimTimeoutMs?: number
}

/** The two operations both a pool and a transaction answer. */
interface SqlHandle {
  query<R extends QueryRow = QueryRow>(sql: string, params?: unknown[]): Promise<R[]>
}

interface TokenRow extends QueryRow {
  token_type: string | null
  token: string | null
  timestamp: string | null
  owner: string | null
}

function nowIso(): string {
  return new Date().toISOString()
}

export function postgresTokenStore(
  pg: PostgresResource,
  options: PostgresTokenStoreOptions = {},
): TokenStore {
  const table = pg.tables.tokens
  const claimTimeoutMs = options.claimTimeoutMs ?? 10000

  /**
   * The writer for one call: the unit of work's transaction when it has one,
   * else the pool. This is the whole reason the store takes `uow` on every
   * method rather than looking one up.
   */
  function sql(uow?: UnitOfWork): SqlHandle {
    return activePostgresTransaction(uow) ?? pg
  }

  function readToken(row: TokenRow | undefined): TrackingToken | undefined {
    if (row === undefined) return undefined
    return deserializeTokenData(row.token_type, row.token)
  }

  async function rowFor(
    handle: SqlHandle,
    processorName: string,
    segment: number,
  ): Promise<TokenRow | undefined> {
    const rows = await handle.query<TokenRow>(
      `SELECT token_type, token, timestamp, owner FROM ${table}
        WHERE processor_name = $1 AND segment = $2`,
      [processorName, segment],
    )
    return rows[0]
  }

  return {
    async store(processorName, segment, token, uow) {
      const { type, data } = serializeTokenData(token)
      await sql(uow).query(
        `INSERT INTO ${table} (processor_name, segment, mask, token_type, token, timestamp, owner)
         VALUES ($1, $2, 0, $3, $4, $5, NULL)
         ON CONFLICT (processor_name, segment) DO UPDATE
           SET token_type = EXCLUDED.token_type,
               token      = EXCLUDED.token,
               timestamp  = EXCLUDED.timestamp`,
        [processorName, segment, type, data, nowIso()],
      )
    },

    async get(processorName, segment, uow) {
      return readToken(await rowFor(sql(uow), processorName, segment))
    },

    async initializeSegments(processorName, segmentCount, uow) {
      const handle = sql(uow)
      for (let i = 0; i < segmentCount; i++) {
        await handle.query(
          `INSERT INTO ${table} (processor_name, segment, mask, token_type, token, timestamp, owner)
           VALUES ($1, $2, 0, NULL, NULL, NULL, NULL)
           ON CONFLICT (processor_name, segment) DO NOTHING`,
          [processorName, i],
        )
      }
    },

    async claimToken(processorName, segment, ownerId, uow) {
      const handle = sql(uow)
      const row = await rowFor(handle, processorName, segment)

      if (row === undefined) {
        await handle.query(
          `INSERT INTO ${table} (processor_name, segment, mask, token_type, token, timestamp, owner)
           VALUES ($1, $2, 0, NULL, NULL, $3, $4)`,
          [processorName, segment, nowIso(), ownerId],
        )
        return undefined
      }

      const expired =
        !row.owner ||
        !row.timestamp ||
        Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs

      if (row.owner === ownerId || expired) {
        await handle.query(
          `UPDATE ${table} SET owner = $3, timestamp = $4
            WHERE processor_name = $1 AND segment = $2`,
          [processorName, segment, ownerId, nowIso()],
        )
        return readToken(row)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId, uow) {
      await sql(uow).query(
        `UPDATE ${table} SET timestamp = $4
          WHERE processor_name = $1 AND segment = $2 AND owner = $3`,
        [processorName, segment, ownerId, nowIso()],
      )
    },

    async releaseClaim(processorName, segment, ownerId, uow) {
      await sql(uow).query(
        `UPDATE ${table} SET owner = NULL, timestamp = NULL
          WHERE processor_name = $1 AND segment = $2 AND owner = $3`,
        [processorName, segment, ownerId],
      )
    },

    async fetchSegments(processorName, uow) {
      const rows = await sql(uow).query<{ segment: number | string }>(
        `SELECT segment FROM ${table} WHERE processor_name = $1 ORDER BY segment`,
        [processorName],
      )
      return rows.map((r) => Number(r.segment))
    },

    async fetchAvailableSegments(processorName, uow) {
      // Same cutoff rule as claimToken: unowned, or owned by a claim whose
      // heartbeat has gone stale. The ISO-8601 `timestamp` column compares
      // lexicographically in exactly chronological order.
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await sql(uow).query<{ segment: number | string }>(
        `SELECT segment FROM ${table}
          WHERE processor_name = $1 AND (owner IS NULL OR timestamp < $2)
          ORDER BY segment`,
        [processorName, cutoff],
      )
      return rows.map((r) => Number(r.segment))
    },

    async deleteToken(processorName, segment, uow) {
      await sql(uow).query(`DELETE FROM ${table} WHERE processor_name = $1 AND segment = $2`, [
        processorName,
        segment,
      ])
    },
  }
}
