import type { TokenStore, TrackingToken } from "@kronos-ts/core"
import {
  type UnitOfWork,
  UnableToClaimTokenError,
  serializeToken as serializeTokenData,
  deserializeToken as deserializeTokenData,
} from "@kronos-ts/core"
import type { KnexClient, KnexFamily } from "./knex-transaction.js"
import { activeKnexTransaction } from "./knex-transaction.js"

/** The table this adapter owns. Not a parameter — the columns are not the caller's choice. */
export const KNEX_TOKEN_TABLE = "kronos_token_entries"

/** Tuning only — everything required is a positional argument. */
export type KnexTokenStoreOptions = {
  /** Claim timeout in ms. Default: 10000. */
  claimTimeoutMs?: number
}

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
 * Creates a TokenStore backed by Knex.
 *
 * Participates in the unit of work's transaction, read off the trailing
 * `uow` parameter every method takes — so a token advance and the projection
 * writes beside it commit together.
 *
 * ```typescript
 * import { knexTokenStore } from "@kronos-ts/knex"
 *
 * const tokenStore = knexTokenStore(knex)
 * ```
 */
export function knexTokenStore(knex: KnexClient, options: KnexTokenStoreOptions = {}): TokenStore<UnitOfWork & KnexFamily> {
  const claimTimeoutMs = options.claimTimeoutMs ?? 10000
  const table = KNEX_TOKEN_TABLE

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction.
   */
  function getKnex(uow?: UnitOfWork): any {
    return activeKnexTransaction(uow) ?? knex
  }

  return {
    async store(processorName, segment, token, uow) {
      const k = getKnex(uow)
      const { token_type, token: tokenData } = serializeToken(token)
      await k.raw(
        `INSERT INTO ?? (processor_name, segment, mask, token_type, token, timestamp, owner)
         VALUES (?, ?, 0, ?, ?, ?, NULL)
         ON CONFLICT (processor_name, segment) DO UPDATE SET token_type = ?, token = ?, timestamp = ?`,
        [
          table,
          processorName,
          segment,
          token_type,
          tokenData,
          nowIso(),
          token_type,
          tokenData,
          nowIso(),
        ],
      )
    },

    async get(processorName, segment, uow) {
      const k = getKnex(uow)
      const row = await k(table).where({ processor_name: processorName, segment }).first()
      if (!row) return undefined
      return deserializeToken(row.token_type, row.token)
    },

    async initializeSegments(processorName, segmentCount, uow) {
      const k = getKnex(uow)
      for (let i = 0; i < segmentCount; i++) {
        await k.raw(
          `INSERT INTO ?? (processor_name, segment, mask, token_type, token, timestamp, owner)
           VALUES (?, ?, 0, NULL, NULL, NULL, NULL)
           ON CONFLICT (processor_name, segment) DO NOTHING`,
          [table, processorName, i],
        )
      }
    },

    async claimToken(processorName, segment, ownerId, uow) {
      const k = getKnex(uow)
      const row = await k(table).where({ processor_name: processorName, segment }).first()

      if (!row) {
        await k(table).insert({
          processor_name: processorName,
          segment,
          mask: 0,
          token_type: null,
          token: null,
          timestamp: nowIso(),
          owner: ownerId,
        })
        return undefined
      }

      const isExpired =
        !row.owner ||
        !row.timestamp ||
        Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs

      if (row.owner === ownerId || isExpired) {
        await k(table)
          .where({ processor_name: processorName, segment })
          .update({ owner: ownerId, timestamp: nowIso() })
        return deserializeToken(row.token_type, row.token)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId, uow) {
      const k = getKnex(uow)
      await k(table)
        .where({ processor_name: processorName, segment, owner: ownerId })
        .update({ timestamp: nowIso() })
    },

    async releaseClaim(processorName, segment, ownerId, uow) {
      const k = getKnex(uow)
      await k(table)
        .where({ processor_name: processorName, segment, owner: ownerId })
        .update({ owner: null, timestamp: null })
    },

    async fetchSegments(processorName, uow) {
      const k = getKnex(uow)
      const rows = await k(table)
        .where({ processor_name: processorName })
        .select("segment")
        .orderBy("segment", "asc")
      return rows.map((r: any) => r.segment)
    },

    async fetchAvailableSegments(processorName, uow) {
      const k = getKnex(uow)
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

    async deleteToken(processorName, segment, uow) {
      const k = getKnex(uow)
      await k(table).where({ processor_name: processorName, segment }).delete()
    },
  }
}
