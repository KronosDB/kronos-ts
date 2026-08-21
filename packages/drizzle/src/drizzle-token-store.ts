import { and, eq, isNull, lt, or } from "drizzle-orm"
import type { TokenStore, TrackingToken } from "@kronos-ts/core"
import {
  type UnitOfWork,
  UnableToClaimTokenError,
  serializeToken as serializeTokenData,
  deserializeToken as deserializeTokenData,
} from "@kronos-ts/core"
import type { DrizzleDb, DrizzleFamily } from "./drizzle-transaction.js"
import { activeDrizzleTransaction } from "./drizzle-transaction.js"
import { kronosTokenEntries } from "./drizzle-schema.js"

/** Tuning only — everything required is a positional argument. */
export type DrizzleTokenStoreOptions = {
  /** Claim timeout in ms. Default: 10000. */
  claimTimeoutMs?: number
}

function serializeToken(token: TrackingToken): { tokenType: string; token: string } {
  const { type, data } = serializeTokenData(token)
  return { tokenType: type, token: data }
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
 * Creates a TokenStore backed by Drizzle ORM.
 *
 * Participates in the unit of work's transaction, read off the trailing
 * `uow` parameter every method takes — so a token advance and the projection
 * writes beside it commit together.
 *
 * The table is this adapter's own ({@link kronosTokenEntries}), exported for
 * migrations and never passed back in.
 *
 * ```typescript
 * import { drizzleTokenStore } from "@kronos-ts/drizzle"
 *
 * const tokenStore = drizzleTokenStore(db)
 * ```
 */
export function drizzleTokenStore(
  db: DrizzleDb,
  options: DrizzleTokenStoreOptions = {},
): TokenStore<UnitOfWork & DrizzleFamily> {
  const table: any = kronosTokenEntries
  const claimTimeoutMs = options.claimTimeoutMs ?? 10000

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction.
   */
  function getDb(uow?: UnitOfWork): any {
    return activeDrizzleTransaction(uow) ?? db
  }

  return {
    async store(processorName, segment, token, uow) {
      const d = getDb(uow)
      const { tokenType, token: tokenData } = serializeToken(token)
      await d
        .insert(table)
        .values({
          processorName,
          segment,
          mask: 0,
          tokenType,
          token: tokenData,
          timestamp: nowIso(),
          owner: null,
        })
        .onConflictDoUpdate({
          target: [table.processorName, table.segment],
          set: { tokenType, token: tokenData, timestamp: nowIso() },
        })
    },

    async get(processorName, segment, uow) {
      const d = getDb(uow)
      const rows = await d
        .select()
        .from(table)
        .where(and(eq(table.processorName, processorName), eq(table.segment, segment)))
        .limit(1)
      if (rows.length === 0) return undefined
      return deserializeToken(rows[0].tokenType, rows[0].token)
    },

    async initializeSegments(processorName, segmentCount, uow) {
      const d = getDb(uow)
      for (let i = 0; i < segmentCount; i++) {
        await d
          .insert(table)
          .values({
            processorName,
            segment: i,
            mask: 0,
            tokenType: null,
            token: null,
            timestamp: null,
            owner: null,
          })
          .onConflictDoNothing()
      }
    },

    async claimToken(processorName, segment, ownerId, uow) {
      const d = getDb(uow)
      const rows = await d
        .select()
        .from(table)
        .where(and(eq(table.processorName, processorName), eq(table.segment, segment)))
        .limit(1)

      if (rows.length === 0) {
        await d.insert(table).values({
          processorName,
          segment,
          mask: 0,
          tokenType: null,
          token: null,
          timestamp: nowIso(),
          owner: ownerId,
        })
        return undefined
      }

      const row = rows[0]
      const isExpired =
        !row.owner ||
        !row.timestamp ||
        Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs

      if (row.owner === ownerId || isExpired) {
        await d
          .update(table)
          .set({ owner: ownerId, timestamp: nowIso() })
          .where(and(eq(table.processorName, processorName), eq(table.segment, segment)))
        return deserializeToken(row.tokenType, row.token)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId, uow) {
      const d = getDb(uow)
      await d
        .update(table)
        .set({ timestamp: nowIso() })
        .where(
          and(
            eq(table.processorName, processorName),
            eq(table.segment, segment),
            eq(table.owner, ownerId),
          ),
        )
    },

    async releaseClaim(processorName, segment, ownerId, uow) {
      const d = getDb(uow)
      await d
        .update(table)
        .set({ owner: null, timestamp: null })
        .where(
          and(
            eq(table.processorName, processorName),
            eq(table.segment, segment),
            eq(table.owner, ownerId),
          ),
        )
    },

    async fetchSegments(processorName, uow) {
      const d = getDb(uow)
      const rows = await d
        .select({ segment: table.segment })
        .from(table)
        .where(eq(table.processorName, processorName))
        .orderBy(table.segment)
      return rows.map((r: any) => r.segment)
    },

    async fetchAvailableSegments(processorName, uow) {
      const d = getDb(uow)
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await d
        .select({ segment: table.segment })
        .from(table)
        .where(
          and(
            eq(table.processorName, processorName),
            or(isNull(table.owner), lt(table.timestamp, cutoff)),
          ),
        )
        .orderBy(table.segment)
      return rows.map((r: any) => r.segment)
    },

    async deleteToken(processorName, segment, uow) {
      const d = getDb(uow)
      await d
        .delete(table)
        .where(and(eq(table.processorName, processorName), eq(table.segment, segment)))
    },
  }
}
