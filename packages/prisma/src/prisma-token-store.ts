import type { TokenStore, TrackingToken } from "@kronos-ts/core"
import {
  type UnitOfWork,
  UnableToClaimTokenError,
  serializeToken as serializeTokenData,
  deserializeToken as deserializeTokenData,
} from "@kronos-ts/core"
import type { PrismaClientLike, PrismaFamily } from "./prisma-transaction.js"
import { activePrismaTransaction } from "./prisma-transaction.js"

/** Tuning only — everything required is a positional argument. */
export type PrismaTokenStoreOptions = {
  /** Claim timeout in ms. Default: 10000. */
  claimTimeoutMs?: number
}

/**
 * Expected shape of the `kronos_token_entries` table in the Prisma schema.
 *
 * ```prisma
 * model KronosTokenEntry {
 *   processorName String
 *   segment       Int
 *   mask          Int          @default(0)
 *   tokenType     String?
 *   token         String?
 *   timestamp     String?
 *   owner         String?
 *
 *   @@id([processorName, segment])
 *   @@map("kronos_token_entries")
 * }
 * ```
 */

type TokenRow = {
  processorName: string
  segment: number
  mask: number
  tokenType: string | null
  token: string | null
  timestamp: string | null
  owner: string | null
}

function serializeToken(token: TrackingToken): { tokenType: string; token: string } {
  const { type, data } = serializeTokenData(token)
  return { tokenType: type, token: data }
}

function deserializeToken(row: TokenRow): TrackingToken | undefined {
  return deserializeTokenData(row.tokenType, row.token)
}

function nowIso(): string {
  return new Date().toISOString()
}

function isClaimExpired(row: TokenRow, claimTimeoutMs: number): boolean {
  if (!row.owner) return true
  if (!row.timestamp) return true
  return Date.now() - new Date(row.timestamp).getTime() > claimTimeoutMs
}

/**
 * Creates a TokenStore backed by Prisma.
 *
 * Uses the `kronosTokenEntry` model from the Prisma schema. Participates in
 * the unit of work's transaction (read off the trailing `uow` parameter) so a
 * token advance and the projection writes beside it commit together.
 *
 * ```typescript
 * import { prismaTokenStore } from "@kronos-ts/prisma"
 *
 * const tokenStore = prismaTokenStore(prisma)
 * ```
 */
export function prismaTokenStore(
  prisma: PrismaClientLike,
  options: PrismaTokenStoreOptions = {},
): TokenStore<UnitOfWork & PrismaFamily> {
  const claimTimeoutMs = options.claimTimeoutMs ?? 10000

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction.
   */
  function getClient(uow?: UnitOfWork): any {
    return activePrismaTransaction(uow) ?? prisma
  }

  return {
    async store(processorName, segment, token, uow) {
      const client = getClient(uow)
      const { tokenType, token: tokenData } = serializeToken(token)
      await client.kronosTokenEntry.upsert({
        where: { processorName_segment: { processorName, segment } },
        update: { tokenType, token: tokenData, timestamp: nowIso() },
        create: {
          processorName,
          segment,
          mask: 0,
          tokenType,
          token: tokenData,
          timestamp: nowIso(),
          owner: null,
        },
      })
    },

    async get(processorName, segment, uow) {
      const client = getClient(uow)
      const row = await client.kronosTokenEntry.findUnique({
        where: { processorName_segment: { processorName, segment } },
      })
      if (!row) return undefined
      return deserializeToken(row)
    },

    async initializeSegments(processorName, segmentCount, uow) {
      const client = getClient(uow)
      for (let i = 0; i < segmentCount; i++) {
        await client.kronosTokenEntry.upsert({
          where: { processorName_segment: { processorName, segment: i } },
          update: {},
          create: {
            processorName,
            segment: i,
            mask: 0,
            tokenType: null,
            token: null,
            timestamp: null,
            owner: null,
          },
        })
      }
    },

    async claimToken(processorName, segment, ownerId, uow) {
      const client = getClient(uow)
      const row = await client.kronosTokenEntry.findUnique({
        where: { processorName_segment: { processorName, segment } },
      })

      if (!row) {
        await client.kronosTokenEntry.create({
          data: {
            processorName,
            segment,
            mask: 0,
            tokenType: null,
            token: null,
            timestamp: nowIso(),
            owner: ownerId,
          },
        })
        return undefined
      }

      if (row.owner === ownerId || isClaimExpired(row, claimTimeoutMs)) {
        await client.kronosTokenEntry.update({
          where: { processorName_segment: { processorName, segment } },
          data: { owner: ownerId, timestamp: nowIso() },
        })
        return deserializeToken(row)
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId, uow) {
      const client = getClient(uow)
      await client.kronosTokenEntry.updateMany({
        where: { processorName, segment, owner: ownerId },
        data: { timestamp: nowIso() },
      })
    },

    async releaseClaim(processorName, segment, ownerId, uow) {
      const client = getClient(uow)
      await client.kronosTokenEntry.updateMany({
        where: { processorName, segment, owner: ownerId },
        data: { owner: null, timestamp: null },
      })
    },

    async fetchSegments(processorName, uow) {
      const client = getClient(uow)
      const rows = await client.kronosTokenEntry.findMany({
        where: { processorName },
        select: { segment: true },
        orderBy: { segment: "asc" },
      })
      return rows.map((r: { segment: number }) => r.segment)
    },

    async fetchAvailableSegments(processorName, uow) {
      const client = getClient(uow)
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await client.kronosTokenEntry.findMany({
        where: {
          processorName,
          OR: [{ owner: null }, { timestamp: { lt: cutoff } }],
        },
        select: { segment: true },
        orderBy: { segment: "asc" },
      })
      return rows.map((r: { segment: number }) => r.segment)
    },

    async deleteToken(processorName, segment, uow) {
      const client = getClient(uow)
      await client.kronosTokenEntry.deleteMany({
        where: { processorName, segment },
      })
    },
  }
}
