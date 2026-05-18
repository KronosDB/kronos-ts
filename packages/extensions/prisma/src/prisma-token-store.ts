import type { TokenStore, TrackingToken } from "@kronos-ts/messaging"
import { getActiveTransaction, UnableToClaimTokenError, globalSequenceToken } from "@kronos-ts/messaging"
import type { PrismaClientLike, PrismaTransactionClient } from "./prisma-transaction-manager.js"

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

interface TokenRow {
  processorName: string
  segment: number
  mask: number
  tokenType: string | null
  token: string | null
  timestamp: string | null
  owner: string | null
}

function serializeToken(token: TrackingToken): { tokenType: string; token: string } {
  return {
    tokenType: "GlobalSequenceToken",
    token: JSON.stringify({ position: token.position().toString() }),
  }
}

function deserializeToken(row: TokenRow): TrackingToken | undefined {
  if (!row.token || !row.tokenType) return undefined
  const data = JSON.parse(row.token)
  return globalSequenceToken(BigInt(data.position))
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
 * Uses the `kronosTokenEntry` model from the Prisma schema. Participates
 * in the active transaction (via `getActiveTransaction()`) so token
 * updates and projection updates are atomic.
 *
 * ```typescript
 * import { prismaTokenStore } from "@kronos-ts/extensions-prisma"
 *
 * // tokenStore wiring to a kronos() App is pending a typed `tokenStore` slot
 * // (Phase 9). For now, construct the store and pass it directly to the
 * // tracking processor that owns it:
 * const tokenStore = prismaTokenStore(prisma)
 * ```
 */
export function prismaTokenStore(
  prisma: PrismaClientLike,
  options?: { claimTimeoutMs?: number },
): TokenStore {
  const claimTimeoutMs = options?.claimTimeoutMs ?? 10000

  function getClient(): any {
    return getActiveTransaction<PrismaTransactionClient>() ?? prisma
  }

  return {
    async store(processorName, segment, token) {
      const client = getClient()
      const { tokenType, token: tokenData } = serializeToken(token)
      await client.kronosTokenEntry.upsert({
        where: { processorName_segment: { processorName, segment } },
        update: { tokenType, token: tokenData, timestamp: nowIso() },
        create: { processorName, segment, mask: 0, tokenType, token: tokenData, timestamp: nowIso(), owner: null },
      })
    },

    async get(processorName, segment) {
      const client = getClient()
      const row = await client.kronosTokenEntry.findUnique({
        where: { processorName_segment: { processorName, segment } },
      })
      if (!row) return undefined
      return deserializeToken(row)
    },

    async initializeSegments(processorName, segmentCount) {
      const client = getClient()
      for (let i = 0; i < segmentCount; i++) {
        await client.kronosTokenEntry.upsert({
          where: { processorName_segment: { processorName, segment: i } },
          update: {},
          create: { processorName, segment: i, mask: 0, tokenType: null, token: null, timestamp: null, owner: null },
        })
      }
    },

    async claimToken(processorName, segment, ownerId) {
      const client = getClient()
      const row = await client.kronosTokenEntry.findUnique({
        where: { processorName_segment: { processorName, segment } },
      })

      if (!row) {
        await client.kronosTokenEntry.create({
          data: { processorName, segment, mask: 0, tokenType: null, token: null, timestamp: nowIso(), owner: ownerId },
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

    async extendClaim(processorName, segment, ownerId) {
      const client = getClient()
      await client.kronosTokenEntry.updateMany({
        where: { processorName, segment, owner: ownerId },
        data: { timestamp: nowIso() },
      })
    },

    async releaseClaim(processorName, segment, ownerId) {
      const client = getClient()
      await client.kronosTokenEntry.updateMany({
        where: { processorName, segment, owner: ownerId },
        data: { owner: null, timestamp: null },
      })
    },

    async fetchSegments(processorName) {
      const client = getClient()
      const rows = await client.kronosTokenEntry.findMany({
        where: { processorName },
        select: { segment: true },
        orderBy: { segment: "asc" },
      })
      return rows.map((r: { segment: number }) => r.segment)
    },

    async fetchAvailableSegments(processorName) {
      const client = getClient()
      const cutoff = new Date(Date.now() - claimTimeoutMs).toISOString()
      const rows = await client.kronosTokenEntry.findMany({
        where: {
          processorName,
          OR: [
            { owner: null },
            { timestamp: { lt: cutoff } },
          ],
        },
        select: { segment: true },
        orderBy: { segment: "asc" },
      })
      return rows.map((r: { segment: number }) => r.segment)
    },

    async deleteToken(processorName, segment) {
      const client = getClient()
      await client.kronosTokenEntry.deleteMany({
        where: { processorName, segment },
      })
    },
  }
}
