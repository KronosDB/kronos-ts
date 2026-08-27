import type { TokenStore, TrackingToken } from "@kronos-ts/core"
import {
  type UnitOfWork,
  UnableToClaimTokenError,
  serializeToken as serializeTokenData,
  deserializeToken as deserializeTokenData,
} from "@kronos-ts/core"
import type {TypeormManager} from "./typeorm-transaction.js"
import { activeTypeormTransaction } from "./typeorm-transaction.js"

/** The table this adapter owns. Not a parameter — the columns are not the caller's choice. */
export const TYPEORM_TOKEN_TABLE = "kronos_token_entries"

/** Tuning only — everything required is a positional argument. */
export type TypeormTokenStoreOptions = {
  /** Claim timeout in ms. Default: 10000. */
  claimTimeoutMs?: number
}

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
 * Creates a TokenStore backed by TypeORM.
 *
 * Uses raw SQL queries via the EntityManager for maximum compatibility.
 * Participates in the unit of work's transaction, read off the trailing
 * `uow` parameter every method takes — so a token advance and the projection
 * writes beside it commit together.
 *
 * ```typescript
 * import { typeormTokenStore } from "@kronos-ts/typeorm"
 *
 * const tokenStore = typeormTokenStore(dataSource.manager)
 * ```
 */
export function typeormTokenStore(
  manager: TypeormManager,
  options: TypeormTokenStoreOptions = {},
): TokenStore<UnitOfWork> {
  const claimTimeoutMs = options.claimTimeoutMs ?? 10000
  const table = TYPEORM_TOKEN_TABLE

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction.
   */
  function getManager(uow?: UnitOfWork): any {
    const tx = activeTypeormTransaction(uow)
    if (tx !== undefined) return tx
    // NO SILENT FALLBACK. A token write that lands outside the batch's
    // transaction is the failure this store exists to avoid: it commits on its
    // own, a crash lands between it and the projection it accounts for, and the
    // read model is permanently wrong with nothing to read as the cause. A
    // handler's accessor may fall back — whether a seam is transactional is a
    // deployment decision — but this one may not.
    if (uow !== undefined) {
      throw new Error(
        "@kronos-ts/typeorm: this unit of work carries no typeorm transaction, so the " +
          "token write would commit outside the batch it accounts for. Build the " +
          "processor's unitOfWork with `typeormUnitOfWork(next, manager)`.",
      )
    }
    // No unit of work at all — lifecycle and admin paths, which are honestly
    // outside any transaction.
    return manager
  }

  return {
    async store(processorName, segment, token, uow) {
      const m = getManager(uow)
      const { tokenType, token: tokenData } = serializeToken(token)
      const ts = nowIso()
      await m.query(
        `INSERT INTO ${table} (processor_name, segment, mask, token_type, token, timestamp, owner)
         VALUES ($1, $2, 0, $3, $4, $5, NULL)
         ON CONFLICT (processor_name, segment) DO UPDATE SET token_type = $3, token = $4, timestamp = $5`,
        [processorName, segment, tokenType, tokenData, ts],
      )
    },

    async get(processorName, segment, uow) {
      const m = getManager(uow)
      const rows = await m.query(
        `SELECT token_type, token FROM ${table} WHERE processor_name = $1 AND segment = $2`,
        [processorName, segment],
      )
      if (rows.length === 0) return undefined
      return deserializeToken(rows[0].token_type, rows[0].token)
    },

    async initializeSegments(processorName, segmentCount, uow) {
      const m = getManager(uow)
      for (let i = 0; i < segmentCount; i++) {
        await m.query(
          `INSERT INTO ${table} (processor_name, segment, mask, token_type, token, timestamp, owner)
           VALUES ($1, $2, 0, NULL, NULL, NULL, NULL)
           ON CONFLICT (processor_name, segment) DO NOTHING`,
          [processorName, i],
        )
      }
    },

    async claimToken(processorName, segment, ownerId, uow) {
      const m = getManager(uow)
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
      const isExpired =
        !row.owner ||
        !row.timestamp ||
        Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs

      if (row.owner === ownerId || isExpired) {
        await m.query(
          `UPDATE ${table} SET owner = $3, timestamp = $4 WHERE processor_name = $1 AND segment = $2`,
          [processorName, segment, ownerId, nowIso()],
        )
        return deserializeToken(row.token_type, row.token)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId, uow) {
      const m = getManager(uow)
      await m.query(
        `UPDATE ${table} SET timestamp = $4 WHERE processor_name = $1 AND segment = $2 AND owner = $3`,
        [processorName, segment, ownerId, nowIso()],
      )
    },

    async releaseClaim(processorName, segment, ownerId, uow) {
      const m = getManager(uow)
      await m.query(
        `UPDATE ${table} SET owner = NULL, timestamp = NULL WHERE processor_name = $1 AND segment = $2 AND owner = $3`,
        [processorName, segment, ownerId],
      )
    },

    async fetchSegments(processorName, uow) {
      const m = getManager(uow)
      const rows = await m.query(
        `SELECT segment FROM ${table} WHERE processor_name = $1 ORDER BY segment ASC`,
        [processorName],
      )
      return rows.map((r: any) => r.segment)
    },

    async fetchAvailableSegments(processorName, uow) {
      const m = getManager(uow)
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await m.query(
        `SELECT segment FROM ${table} WHERE processor_name = $1 AND (owner IS NULL OR timestamp < $2) ORDER BY segment ASC`,
        [processorName, cutoff],
      )
      return rows.map((r: any) => r.segment)
    },

    async deleteToken(processorName, segment, uow) {
      const m = getManager(uow)
      await m.query(`DELETE FROM ${table} WHERE processor_name = $1 AND segment = $2`, [
        processorName,
        segment,
      ])
    },
  }
}
