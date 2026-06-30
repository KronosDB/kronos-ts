import type { TokenStore, TrackingToken } from "@kronos-ts/messaging"
import {
  getActiveTransaction,
  UnableToClaimTokenError,
  serializeToken as serializeTokenData,
  deserializeToken as deserializeTokenData,
} from "@kronos-ts/messaging"
import type { KyselyTransaction } from "./kysely-transaction-manager.js"

/**
 * Token table interface for Kysely. Users must define this table in their
 * Kysely database interface:
 *
 * ```typescript
 * interface Database {
 *   kronos_token_entries: {
 *     processor_name: string
 *     segment: number
 *     mask: number
 *     token_type: string | null
 *     token: string | null
 *     timestamp: string | null
 *     owner: string | null
 *   }
 * }
 * ```
 */

function serializeToken(token: TrackingToken): { token_type: string; token: string } {
  const { type, data } = serializeTokenData(token)
  return { token_type: type, token: data }
}

function deserializeToken(tokenType: string | null, token: string | null): TrackingToken | undefined {
  return deserializeTokenData(tokenType, token)
}

function nowIso(): string {
  return new Date().toISOString()
}

/**
 * A Kysely database instance (or transaction) with query methods.
 */
export interface KyselyDbLike {
  selectFrom(table: string): any
  insertInto(table: string): any
  updateTable(table: string): any
  deleteFrom(table: string): any
}

/**
 * Creates a TokenStore backed by Kysely.
 *
 * Participates in the active transaction via `getActiveTransaction()`.
 * Uses the `kronos_token_entries` table with snake_case column names.
 *
 * ```typescript
 * import { kyselyTokenStore } from "@kronos-ts/kysely"
 *
 * // tokenStore wiring to a kronos() App is pending a typed `tokenStore` slot
 * // (Phase 9). For now, construct the store and pass it directly to the
 * // tracking processor that owns it:
 * const tokenStore = kyselyTokenStore(db)
 * ```
 */
export function kyselyTokenStore(
  db: KyselyDbLike,
  options?: { claimTimeoutMs?: number; tableName?: string },
): TokenStore {
  const claimTimeoutMs = options?.claimTimeoutMs ?? 10000
  const table = options?.tableName ?? "kronos_token_entries"

  function getDb(): KyselyDbLike {
    return getActiveTransaction<KyselyTransaction>() ?? db
  }

  return {
    async store(processorName, segment, token) {
      const d = getDb()
      const { token_type, token: tokenData } = serializeToken(token)
      // Upsert via onConflict
      await d.insertInto(table)
        .values({ processor_name: processorName, segment, mask: 0, token_type, token: tokenData, timestamp: nowIso(), owner: null })
        .onConflict((oc: any) => oc.columns(["processor_name", "segment"]).doUpdateSet({ token_type, token: tokenData, timestamp: nowIso() }))
        .execute()
    },

    async get(processorName, segment) {
      const d = getDb()
      const row = await d.selectFrom(table)
        .selectAll()
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .executeTakeFirst()
      if (!row) return undefined
      return deserializeToken(row.token_type, row.token)
    },

    async initializeSegments(processorName, segmentCount) {
      const d = getDb()
      for (let i = 0; i < segmentCount; i++) {
        await d.insertInto(table)
          .values({ processor_name: processorName, segment: i, mask: 0, token_type: null, token: null, timestamp: null, owner: null })
          .onConflict((oc: any) => oc.columns(["processor_name", "segment"]).doNothing())
          .execute()
      }
    },

    async claimToken(processorName, segment, ownerId) {
      const d = getDb()
      const row = await d.selectFrom(table)
        .selectAll()
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .executeTakeFirst()

      if (!row) {
        await d.insertInto(table)
          .values({ processor_name: processorName, segment, mask: 0, token_type: null, token: null, timestamp: nowIso(), owner: ownerId })
          .execute()
        return undefined
      }

      const isExpired = !row.owner || !row.timestamp ||
        (Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs)

      if (row.owner === ownerId || isExpired) {
        await d.updateTable(table)
          .set({ owner: ownerId, timestamp: nowIso() })
          .where("processor_name", "=", processorName)
          .where("segment", "=", segment)
          .execute()
        return deserializeToken(row.token_type, row.token)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId) {
      const d = getDb()
      await d.updateTable(table)
        .set({ timestamp: nowIso() })
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .where("owner", "=", ownerId)
        .execute()
    },

    async releaseClaim(processorName, segment, ownerId) {
      const d = getDb()
      await d.updateTable(table)
        .set({ owner: null, timestamp: null })
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .where("owner", "=", ownerId)
        .execute()
    },

    async fetchSegments(processorName) {
      const d = getDb()
      const rows = await d.selectFrom(table)
        .select("segment")
        .where("processor_name", "=", processorName)
        .orderBy("segment", "asc")
        .execute()
      return rows.map((r: any) => r.segment)
    },

    async fetchAvailableSegments(processorName) {
      const d = getDb()
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await d.selectFrom(table)
        .select("segment")
        .where("processor_name", "=", processorName)
        .where((eb: any) => eb.or([
          eb("owner", "is", null),
          eb("timestamp", "<", cutoff),
        ]))
        .orderBy("segment", "asc")
        .execute()
      return rows.map((r: any) => r.segment)
    },

    async deleteToken(processorName, segment) {
      const d = getDb()
      await d.deleteFrom(table)
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .execute()
    },
  }
}
