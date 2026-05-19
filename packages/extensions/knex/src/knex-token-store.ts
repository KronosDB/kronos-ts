import type { TokenStore, TrackingToken } from "@kronos-ts/messaging"
import { getActiveTransaction, UnableToClaimTokenError, globalSequenceToken } from "@kronos-ts/messaging"
import type { KnexTransaction } from "./knex-transaction-manager.js"

function serializeToken(token: TrackingToken): { token_type: string; token: string } {
  return {
    token_type: "GlobalSequenceToken",
    token: JSON.stringify({ position: token.position().toString() }),
  }
}

function deserializeToken(tokenType: string | null, token: string | null): TrackingToken | undefined {
  if (!token || !tokenType) return undefined
  const data = JSON.parse(token)
  return globalSequenceToken(BigInt(data.position))
}

function nowIso(): string {
  return new Date().toISOString()
}

/**
 * A Knex instance or transaction with query builder methods.
 */
export interface KnexQueryable {
  (tableName: string): any
  raw(sql: string, ...bindings: any[]): any
}

/**
 * Creates a TokenStore backed by Knex.
 *
 * Participates in the active transaction via `getActiveTransaction()`.
 *
 * ```typescript
 * import { knexTokenStore } from "@kronos-ts/knex"
 *
 * // tokenStore wiring to a kronos() App is pending a typed `tokenStore` slot
 * // (Phase 9). For now, construct the store and pass it directly to the
 * // tracking processor that owns it:
 * const tokenStore = knexTokenStore(knex)
 * ```
 */
export function knexTokenStore(
  knex: KnexQueryable,
  options?: { claimTimeoutMs?: number; tableName?: string },
): TokenStore {
  const claimTimeoutMs = options?.claimTimeoutMs ?? 10000
  const table = options?.tableName ?? "kronos_token_entries"

  function getKnex(): KnexQueryable {
    return getActiveTransaction<KnexTransaction>() ?? knex
  }

  return {
    async store(processorName, segment, token) {
      const k = getKnex()
      const { token_type, token: tokenData } = serializeToken(token)
      await k.raw(
        `INSERT INTO ?? (processor_name, segment, mask, token_type, token, timestamp, owner)
         VALUES (?, ?, 0, ?, ?, ?, NULL)
         ON CONFLICT (processor_name, segment) DO UPDATE SET token_type = ?, token = ?, timestamp = ?`,
        [table, processorName, segment, token_type, tokenData, nowIso(), token_type, tokenData, nowIso()],
      )
    },

    async get(processorName, segment) {
      const k = getKnex()
      const row = await k(table)
        .where({ processor_name: processorName, segment })
        .first()
      if (!row) return undefined
      return deserializeToken(row.token_type, row.token)
    },

    async initializeSegments(processorName, segmentCount) {
      const k = getKnex()
      for (let i = 0; i < segmentCount; i++) {
        await k.raw(
          `INSERT INTO ?? (processor_name, segment, mask, token_type, token, timestamp, owner)
           VALUES (?, ?, 0, NULL, NULL, NULL, NULL)
           ON CONFLICT (processor_name, segment) DO NOTHING`,
          [table, processorName, i],
        )
      }
    },

    async claimToken(processorName, segment, ownerId) {
      const k = getKnex()
      const row = await k(table)
        .where({ processor_name: processorName, segment })
        .first()

      if (!row) {
        await k(table).insert({
          processor_name: processorName, segment, mask: 0,
          token_type: null, token: null, timestamp: nowIso(), owner: ownerId,
        })
        return undefined
      }

      const isExpired = !row.owner || !row.timestamp ||
        (Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs)

      if (row.owner === ownerId || isExpired) {
        await k(table)
          .where({ processor_name: processorName, segment })
          .update({ owner: ownerId, timestamp: nowIso() })
        return deserializeToken(row.token_type, row.token)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId) {
      const k = getKnex()
      await k(table)
        .where({ processor_name: processorName, segment, owner: ownerId })
        .update({ timestamp: nowIso() })
    },

    async releaseClaim(processorName, segment, ownerId) {
      const k = getKnex()
      await k(table)
        .where({ processor_name: processorName, segment, owner: ownerId })
        .update({ owner: null, timestamp: null })
    },

    async fetchSegments(processorName) {
      const k = getKnex()
      const rows = await k(table)
        .where({ processor_name: processorName })
        .select("segment")
        .orderBy("segment", "asc")
      return rows.map((r: any) => r.segment)
    },

    async fetchAvailableSegments(processorName) {
      const k = getKnex()
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await k(table)
        .where({ processor_name: processorName })
        .where(function (this: any) {
          this.whereNull("owner").orWhere("timestamp", "<", cutoff)
        })
        .select("segment")
        .orderBy("segment", "asc")
      return rows.map((r: any) => r.segment)
    },

    async deleteToken(processorName, segment) {
      const k = getKnex()
      await k(table)
        .where({ processor_name: processorName, segment })
        .delete()
    },
  }
}
