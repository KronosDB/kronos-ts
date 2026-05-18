import type { TokenStore, TrackingToken } from "@kronos-ts/messaging"
import { getActiveTransaction, UnableToClaimTokenError, globalSequenceToken } from "@kronos-ts/messaging"
import type { DrizzleDatabaseLike, DrizzleTransaction } from "./drizzle-transaction-manager.js"

/**
 * Token table configuration. Users pass their Drizzle table reference
 * and the `eq`, `and`, `or`, `lt` operators from drizzle-orm.
 *
 * The table must match the Kronos `TokenEntry` schema:
 *
 * ```typescript
 * import { pgTable, varchar, integer, primaryKey } from "drizzle-orm/pg-core"
 *
 * export const kronosTokenEntries = pgTable("kronos_token_entries", {
 *   processorName: varchar("processor_name", { length: 255 }).notNull(),
 *   segment: integer("segment").notNull(),
 *   mask: integer("mask").notNull().default(0),
 *   tokenType: varchar("token_type", { length: 255 }),
 *   token: varchar("token", { length: 10000 }),
 *   timestamp: varchar("timestamp", { length: 255 }),
 *   owner: varchar("owner", { length: 255 }),
 * }, (table) => ({
 *   pk: primaryKey({ columns: [table.processorName, table.segment] }),
 * }))
 * ```
 */
export interface DrizzleTokenStoreConfig {
  /** The Drizzle database instance. */
  db: DrizzleDatabaseLike
  /** The Drizzle table reference for `kronos_token_entries`. */
  table: any
  /** Drizzle `eq` operator. */
  eq: (column: any, value: any) => any
  /** Drizzle `and` operator. */
  and: (...conditions: any[]) => any
  /** Drizzle `or` operator. */
  or: (...conditions: any[]) => any
  /** Drizzle `lt` operator. */
  lt: (column: any, value: any) => any
  /** Drizzle `isNull` operator. */
  isNull: (column: any) => any
  /** Claim timeout in ms. Default: 10000. */
  claimTimeoutMs?: number
}

function serializeToken(token: TrackingToken): { tokenType: string; token: string } {
  return {
    tokenType: "GlobalSequenceToken",
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
 * Creates a TokenStore backed by Drizzle ORM.
 *
 * Participates in the active transaction via `getActiveTransaction()`.
 *
 * ```typescript
 * import { eq, and, or, lt, isNull } from "drizzle-orm"
 * import { drizzleTokenStore } from "@kronos-ts/extensions/drizzle"
 * import { kronosTokenEntries } from "./schema"
 *
 * // tokenStore wiring to a kronos() App is pending a typed `tokenStore` slot
 * // (Phase 9). For now, construct the store and pass it directly to the
 * // tracking processor that owns it:
 * const tokenStore = drizzleTokenStore({
 *   db, table: kronosTokenEntries, eq, and, or, lt, isNull,
 * })
 * ```
 */
export function drizzleTokenStore(config: DrizzleTokenStoreConfig): TokenStore {
  const { table, eq, and, or, lt, isNull } = config
  const claimTimeoutMs = config.claimTimeoutMs ?? 10000

  function getDb(): any {
    return getActiveTransaction<DrizzleTransaction>() ?? config.db
  }

  return {
    async store(processorName, segment, token) {
      const db = getDb()
      const { tokenType, token: tokenData } = serializeToken(token)
      await db.insert(table)
        .values({ processorName, segment, mask: 0, tokenType, token: tokenData, timestamp: nowIso(), owner: null })
        .onConflictDoUpdate({
          target: [table.processorName, table.segment],
          set: { tokenType, token: tokenData, timestamp: nowIso() },
        })
    },

    async get(processorName, segment) {
      const db = getDb()
      const rows = await db.select().from(table)
        .where(and(eq(table.processorName, processorName), eq(table.segment, segment)))
        .limit(1)
      if (rows.length === 0) return undefined
      return deserializeToken(rows[0].tokenType, rows[0].token)
    },

    async initializeSegments(processorName, segmentCount) {
      const db = getDb()
      for (let i = 0; i < segmentCount; i++) {
        await db.insert(table)
          .values({ processorName, segment: i, mask: 0, tokenType: null, token: null, timestamp: null, owner: null })
          .onConflictDoNothing()
      }
    },

    async claimToken(processorName, segment, ownerId) {
      const db = getDb()
      const rows = await db.select().from(table)
        .where(and(eq(table.processorName, processorName), eq(table.segment, segment)))
        .limit(1)

      if (rows.length === 0) {
        await db.insert(table).values({
          processorName, segment, mask: 0, tokenType: null, token: null, timestamp: nowIso(), owner: ownerId,
        })
        return undefined
      }

      const row = rows[0]
      const isExpired = !row.owner || !row.timestamp ||
        (Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs)

      if (row.owner === ownerId || isExpired) {
        await db.update(table)
          .set({ owner: ownerId, timestamp: nowIso() })
          .where(and(eq(table.processorName, processorName), eq(table.segment, segment)))
        return deserializeToken(row.tokenType, row.token)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId) {
      const db = getDb()
      await db.update(table)
        .set({ timestamp: nowIso() })
        .where(and(
          eq(table.processorName, processorName),
          eq(table.segment, segment),
          eq(table.owner, ownerId),
        ))
    },

    async releaseClaim(processorName, segment, ownerId) {
      const db = getDb()
      await db.update(table)
        .set({ owner: null, timestamp: null })
        .where(and(
          eq(table.processorName, processorName),
          eq(table.segment, segment),
          eq(table.owner, ownerId),
        ))
    },

    async fetchSegments(processorName) {
      const db = getDb()
      const rows = await db.select({ segment: table.segment }).from(table)
        .where(eq(table.processorName, processorName))
        .orderBy(table.segment)
      return rows.map((r: any) => r.segment)
    },

    async fetchAvailableSegments(processorName) {
      const db = getDb()
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await db.select({ segment: table.segment }).from(table)
        .where(and(
          eq(table.processorName, processorName),
          or(isNull(table.owner), lt(table.timestamp, cutoff)),
        ))
        .orderBy(table.segment)
      return rows.map((r: any) => r.segment)
    },

    async deleteToken(processorName, segment) {
      const db = getDb()
      await db.delete(table)
        .where(and(eq(table.processorName, processorName), eq(table.segment, segment)))
    },
  }
}
