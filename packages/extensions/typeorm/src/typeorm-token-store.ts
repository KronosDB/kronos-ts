import type { TokenStore, TrackingToken } from "@kronos-ts/messaging"
import { getActiveTransaction, UnableToClaimTokenError, globalSequenceToken } from "@kronos-ts/messaging"
import type { TypeOrmTransaction } from "./typeorm-transaction-manager.js"

/**
 * TypeORM entity for the token table. Users should define this entity:
 *
 * ```typescript
 * @Entity("kronos_token_entries")
 * export class KronosTokenEntry {
 *   @PrimaryColumn({ name: "processor_name" }) processorName: string
 *   @PrimaryColumn() segment: number
 *   @Column({ default: 0 }) mask: number
 *   @Column({ name: "token_type", nullable: true }) tokenType: string | null
 *   @Column({ nullable: true }) token: string | null
 *   @Column({ nullable: true }) timestamp: string | null
 *   @Column({ nullable: true }) owner: string | null
 * }
 * ```
 */

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
 * A TypeORM EntityManager or DataSource with query/save methods.
 */
export interface TypeOrmManagerLike {
  query(sql: string, params?: any[]): Promise<any[]>
}

/**
 * Creates a TokenStore backed by TypeORM.
 *
 * Uses raw SQL queries via EntityManager for maximum compatibility.
 * Participates in the active transaction via `getActiveTransaction()`.
 *
 * ```typescript
 * import { typeormTokenStore } from "@kronos-ts/typeorm"
 *
 * // tokenStore wiring to a kronos() App is pending a typed `tokenStore` slot
 * // (Phase 9). For now, construct the store and pass it directly to the
 * // tracking processor that owns it:
 * const tokenStore = typeormTokenStore(dataSource.manager)
 * ```
 */
export function typeormTokenStore(
  manager: TypeOrmManagerLike,
  options?: { claimTimeoutMs?: number; tableName?: string },
): TokenStore {
  const claimTimeoutMs = options?.claimTimeoutMs ?? 10000
  const table = options?.tableName ?? "kronos_token_entries"

  function getManager(): TypeOrmManagerLike {
    return getActiveTransaction<TypeOrmTransaction>() ?? manager
  }

  return {
    async store(processorName, segment, token) {
      const m = getManager()
      const { tokenType, token: tokenData } = serializeToken(token)
      const ts = nowIso()
      await m.query(
        `INSERT INTO ${table} (processor_name, segment, mask, token_type, token, timestamp, owner)
         VALUES ($1, $2, 0, $3, $4, $5, NULL)
         ON CONFLICT (processor_name, segment) DO UPDATE SET token_type = $3, token = $4, timestamp = $5`,
        [processorName, segment, tokenType, tokenData, ts],
      )
    },

    async get(processorName, segment) {
      const m = getManager()
      const rows = await m.query(
        `SELECT token_type, token FROM ${table} WHERE processor_name = $1 AND segment = $2`,
        [processorName, segment],
      )
      if (rows.length === 0) return undefined
      return deserializeToken(rows[0].token_type, rows[0].token)
    },

    async initializeSegments(processorName, segmentCount) {
      const m = getManager()
      for (let i = 0; i < segmentCount; i++) {
        await m.query(
          `INSERT INTO ${table} (processor_name, segment, mask, token_type, token, timestamp, owner)
           VALUES ($1, $2, 0, NULL, NULL, NULL, NULL)
           ON CONFLICT (processor_name, segment) DO NOTHING`,
          [processorName, i],
        )
      }
    },

    async claimToken(processorName, segment, ownerId) {
      const m = getManager()
      const rows = await m.query(
        `SELECT token_type, token, owner, timestamp FROM ${table} WHERE processor_name = $1 AND segment = $2`,
        [processorName, segment],
      )

      if (rows.length === 0) {
        await m.query(
          `INSERT INTO ${table} (processor_name, segment, mask, token_type, token, timestamp, owner)
           VALUES ($1, $2, 0, NULL, NULL, $3, $4)`,
          [processorName, segment, nowIso(), ownerId],
        )
        return undefined
      }

      const row = rows[0]
      const isExpired = !row.owner || !row.timestamp ||
        (Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs)

      if (row.owner === ownerId || isExpired) {
        await m.query(
          `UPDATE ${table} SET owner = $3, timestamp = $4 WHERE processor_name = $1 AND segment = $2`,
          [processorName, segment, ownerId, nowIso()],
        )
        return deserializeToken(row.token_type, row.token)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId) {
      const m = getManager()
      await m.query(
        `UPDATE ${table} SET timestamp = $4 WHERE processor_name = $1 AND segment = $2 AND owner = $3`,
        [processorName, segment, ownerId, nowIso()],
      )
    },

    async releaseClaim(processorName, segment, ownerId) {
      const m = getManager()
      await m.query(
        `UPDATE ${table} SET owner = NULL, timestamp = NULL WHERE processor_name = $1 AND segment = $2 AND owner = $3`,
        [processorName, segment, ownerId],
      )
    },

    async fetchSegments(processorName) {
      const m = getManager()
      const rows = await m.query(
        `SELECT segment FROM ${table} WHERE processor_name = $1 ORDER BY segment ASC`,
        [processorName],
      )
      return rows.map((r: any) => r.segment)
    },

    async fetchAvailableSegments(processorName) {
      const m = getManager()
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await m.query(
        `SELECT segment FROM ${table} WHERE processor_name = $1 AND (owner IS NULL OR timestamp < $2) ORDER BY segment ASC`,
        [processorName, cutoff],
      )
      return rows.map((r: any) => r.segment)
    },

    async deleteToken(processorName, segment) {
      const m = getManager()
      await m.query(
        `DELETE FROM ${table} WHERE processor_name = $1 AND segment = $2`,
        [processorName, segment],
      )
    },
  }
}
