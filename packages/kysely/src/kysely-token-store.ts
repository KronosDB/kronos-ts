import type { TokenStore, TrackingToken } from "@kronos-ts/core"
import {
  type UnitOfWork,
  UnableToClaimTokenError,
  serializeToken as serializeTokenData,
  deserializeToken as deserializeTokenData,
} from "@kronos-ts/core"
import type { KyselyDb } from "./kysely-transaction.js"
import { activeKyselyTransaction } from "./kysely-transaction.js"

/** The table this adapter owns. Not a parameter — the columns are not the caller's choice. */
export const KYSELY_TOKEN_TABLE = "kronos_token_entries"

/** Tuning only — everything required is a positional argument. */
export interface KyselyTokenStoreOptions {
  /** Claim timeout in ms. Default: 10000. */
  claimTimeoutMs?: number
}

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

function deserializeToken(
  tokenType: string | null,
  token: string | null,
): TrackingToken | undefined {
  return deserializeTokenData(tokenType, token)
}

function nowIso(): string {
  return new Date().toISOString()
}

/**
 * Creates a TokenStore backed by Kysely.
 *
 * Participates in the unit of work's transaction, read off the trailing
 * `uow` parameter every method takes — so a token advance and the projection
 * writes beside it commit together. Uses the `kronos_token_entries` table with
 * snake_case column names.
 *
 * ```typescript
 * import { kyselyTokenStore } from "@kronos-ts/kysely"
 *
 * const tokenStore = kyselyTokenStore(db)
 * ```
 */
export function kyselyTokenStore(db: KyselyDb, options: KyselyTokenStoreOptions = {}): TokenStore {
  const claimTimeoutMs = options.claimTimeoutMs ?? 10000
  const table = KYSELY_TOKEN_TABLE

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction.
   */
  function getDb(uow?: UnitOfWork): any {
    return activeKyselyTransaction(uow) ?? db
  }

  return {
    async store(processorName, segment, token, uow) {
      const d = getDb(uow)
      const { token_type, token: tokenData } = serializeToken(token)
      // Upsert via onConflict
      await d
        .insertInto(table)
        .values({
          processor_name: processorName,
          segment,
          mask: 0,
          token_type,
          token: tokenData,
          timestamp: nowIso(),
          owner: null,
        })
        .onConflict((oc: any) =>
          oc
            .columns(["processor_name", "segment"])
            .doUpdateSet({ token_type, token: tokenData, timestamp: nowIso() }),
        )
        .execute()
    },

    async get(processorName, segment, uow) {
      const d = getDb(uow)
      const row = await d
        .selectFrom(table)
        .selectAll()
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .executeTakeFirst()
      if (!row) return undefined
      return deserializeToken(row.token_type, row.token)
    },

    async initializeSegments(processorName, segmentCount, uow) {
      const d = getDb(uow)
      for (let i = 0; i < segmentCount; i++) {
        await d
          .insertInto(table)
          .values({
            processor_name: processorName,
            segment: i,
            mask: 0,
            token_type: null,
            token: null,
            timestamp: null,
            owner: null,
          })
          .onConflict((oc: any) => oc.columns(["processor_name", "segment"]).doNothing())
          .execute()
      }
    },

    async claimToken(processorName, segment, ownerId, uow) {
      const d = getDb(uow)
      const row = await d
        .selectFrom(table)
        .selectAll()
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .executeTakeFirst()

      if (!row) {
        await d
          .insertInto(table)
          .values({
            processor_name: processorName,
            segment,
            mask: 0,
            token_type: null,
            token: null,
            timestamp: nowIso(),
            owner: ownerId,
          })
          .execute()
        return undefined
      }

      const isExpired =
        !row.owner ||
        !row.timestamp ||
        Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs

      if (row.owner === ownerId || isExpired) {
        await d
          .updateTable(table)
          .set({ owner: ownerId, timestamp: nowIso() })
          .where("processor_name", "=", processorName)
          .where("segment", "=", segment)
          .execute()
        return deserializeToken(row.token_type, row.token)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId, uow) {
      const d = getDb(uow)
      await d
        .updateTable(table)
        .set({ timestamp: nowIso() })
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .where("owner", "=", ownerId)
        .execute()
    },

    async releaseClaim(processorName, segment, ownerId, uow) {
      const d = getDb(uow)
      await d
        .updateTable(table)
        .set({ owner: null, timestamp: null })
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .where("owner", "=", ownerId)
        .execute()
    },

    async fetchSegments(processorName, uow) {
      const d = getDb(uow)
      const rows = await d
        .selectFrom(table)
        .select("segment")
        .where("processor_name", "=", processorName)
        .orderBy("segment", "asc")
        .execute()
      return rows.map((r: any) => r.segment)
    },

    async fetchAvailableSegments(processorName, uow) {
      const d = getDb(uow)
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await d
        .selectFrom(table)
        .select("segment")
        .where("processor_name", "=", processorName)
        .where((eb: any) => eb.or([eb("owner", "is", null), eb("timestamp", "<", cutoff)]))
        .orderBy("segment", "asc")
        .execute()
      return rows.map((r: any) => r.segment)
    },

    async deleteToken(processorName, segment, uow) {
      const d = getDb(uow)
      await d
        .deleteFrom(table)
        .where("processor_name", "=", processorName)
        .where("segment", "=", segment)
        .execute()
    },
  }
}
