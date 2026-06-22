import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "@kronos-ts/messaging"
import { getActiveTransaction, DeadLetterQueueOverflowError } from "@kronos-ts/messaging"
import type { PrismaClientLike, PrismaTransactionClient } from "./prisma-transaction-manager.js"

/**
 * Persistent {@link SequencedDeadLetterQueue} backed by Prisma — a faithful
 * translation of the Drizzle reference DLQ (`drizzle-dead-letter-queue.ts`)
 * onto Prisma's delegate API.
 *
 * Like {@link prismaTokenStore} it reads the active transaction via
 * `getActiveTransaction()`, so enqueue/evict/requeue commit in the **same
 * transaction as the token update** — a crash cannot advance the processor's
 * token while losing the parked letter.
 *
 * The table is shared across processors and partitioned by `processingGroup`.
 * Per-sequence FIFO order is held by a monotonic `sequenceIndex`; a
 * `processingStarted` lease column makes `process()` safe across multiple
 * nodes (Axon parity).
 *
 * Expected shape of the `kronos_dead_letters` table in the Prisma schema.
 *
 * ```prisma
 * model KronosDeadLetter {
 *   deadLetterId       String  @id @map("dead_letter_id")
 *   processingGroup    String  @map("processing_group")
 *   sequenceIdentifier String  @map("sequence_identifier")
 *   sequenceIndex      Int     @map("sequence_index")
 *   message            String  @map("message")
 *   causeType          String? @map("cause_type")
 *   causeMessage       String? @map("cause_message")
 *   diagnostics        String  @map("diagnostics")
 *   enqueuedAt         String  @map("enqueued_at")
 *   lastTouched        String  @map("last_touched")
 *   processingStarted  String? @map("processing_started")
 *
 *   @@index([processingGroup, sequenceIdentifier, sequenceIndex], name: "kronos_dl_seq")
 *   @@map("kronos_dead_letters")
 * }
 * ```
 */
export interface PrismaDeadLetterQueueConfig {
  /** Processing group (the processor name) this queue serves. */
  processingGroup: string
  /** Maximum number of sequences. Default: 1024 (Axon parity). */
  maxSequences?: number
  /** Maximum letters per sequence. Default: 1024 (Axon parity). */
  maxSequenceSize?: number
  /** Lease duration for in-flight processing, ms. Default: 30000 (Axon parity). */
  claimDurationMs?: number
}

/** Reserved diagnostics key carrying the persistent row id across read → evict/requeue. */
const DL_ID = "__dlqId"

let idCounter = 0
function newId(group: string): string {
  // Unique within the table: time + per-process counter + group.
  idCounter += 1
  return `${group}:${Date.now()}:${idCounter}`
}

interface DeadLetterRow {
  deadLetterId: string
  processingGroup: string
  sequenceIdentifier: string
  sequenceIndex: number
  message: string
  causeType: string | null
  causeMessage: string | null
  diagnostics: string
  enqueuedAt: string
  lastTouched: string
  processingStarted: string | null
}

/**
 * Creates a {@link SequencedDeadLetterQueue} backed by Prisma.
 *
 * Uses the `kronosDeadLetter` model from the Prisma schema. Participates in
 * the active transaction (via `getActiveTransaction()`) so dead-letter writes
 * and token/projection updates are atomic.
 *
 * ```typescript
 * import { prismaDeadLetterQueue } from "@kronos-ts/prisma"
 *
 * const dlq = prismaDeadLetterQueue(prisma, { processingGroup: "my-processor" })
 * ```
 */
export function prismaDeadLetterQueue(
  prisma: PrismaClientLike,
  config: PrismaDeadLetterQueueConfig,
): SequencedDeadLetterQueue {
  const { processingGroup } = config
  const maxSequences = config.maxSequences ?? 1024
  const maxSequenceSize = config.maxSequenceSize ?? 1024
  const claimDurationMs = config.claimDurationMs ?? 30000

  function getClient(): any {
    return getActiveTransaction<PrismaTransactionClient>() ?? prisma
  }

  function rowToLetter(row: DeadLetterRow): DeadLetter {
    const cause = new Error(row.causeMessage ?? "")
    if (row.causeType) cause.name = row.causeType
    return {
      message: JSON.parse(row.message),
      cause,
      enqueuedAt: Number(row.enqueuedAt),
      lastTouched: Number(row.lastTouched),
      diagnostics: { ...JSON.parse(row.diagnostics), [DL_ID]: row.deadLetterId },
      sequenceIdentifier: row.sequenceIdentifier,
    }
  }

  function letterToRow(letter: DeadLetter, sequenceIndex: number, deadLetterId: string): DeadLetterRow {
    const { [DL_ID]: _omit, ...diagnostics } = letter.diagnostics as Record<string, unknown>
    return {
      deadLetterId,
      processingGroup,
      sequenceIdentifier: letter.sequenceIdentifier,
      sequenceIndex,
      message: JSON.stringify(letter.message),
      causeType: letter.cause.name,
      causeMessage: letter.cause.message,
      diagnostics: JSON.stringify(diagnostics),
      enqueuedAt: String(letter.enqueuedAt),
      lastTouched: String(letter.lastTouched),
      processingStarted: null,
    }
  }

  async function sequenceRows(client: any, seqId: string): Promise<DeadLetterRow[]> {
    return client.kronosDeadLetter.findMany({
      where: { processingGroup, sequenceIdentifier: seqId },
      orderBy: { sequenceIndex: "asc" },
    })
  }

  async function distinctSequences(client: any): Promise<string[]> {
    const rows = await client.kronosDeadLetter.findMany({
      where: { processingGroup },
      select: { sequenceIdentifier: true },
      distinct: ["sequenceIdentifier"],
    })
    return rows.map((r: { sequenceIdentifier: string }) => r.sequenceIdentifier)
  }

  return {
    async enqueue(letter) {
      const client = getClient()
      const existing = await sequenceRows(client, letter.sequenceIdentifier)
      if (existing.length === 0) {
        if ((await distinctSequences(client)).length >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
      } else if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing.length === 0 ? 0 : existing[existing.length - 1].sequenceIndex + 1
      await client.kronosDeadLetter.create({ data: letterToRow(letter, idx, newId(processingGroup)) })
    },

    async enqueueIfPresent(sequenceIdentifier, letterSupplier) {
      const client = getClient()
      const existing = await sequenceRows(client, sequenceIdentifier)
      if (existing.length === 0) return false
      if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing[existing.length - 1].sequenceIndex + 1
      await client.kronosDeadLetter.create({
        data: letterToRow(letterSupplier(), idx, newId(processingGroup)),
      })
      return true
    },

    async evict(_sequenceIdentifier, letter) {
      const client = getClient()
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await client.kronosDeadLetter.deleteMany({
        where: { processingGroup, deadLetterId: id },
      })
    },

    async requeue(letter, update) {
      const client = getClient()
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiag } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics ? { ...baseDiag, ...update.diagnostics } : baseDiag
      await client.kronosDeadLetter.updateMany({
        where: { processingGroup, deadLetterId: id },
        data: {
          causeType: cause.name,
          causeMessage: cause.message,
          diagnostics: JSON.stringify(diagnostics),
          lastTouched: String(Date.now()),
        },
      })
    },

    async contains(sequenceIdentifier) {
      const client = getClient()
      const row = await client.kronosDeadLetter.findFirst({
        where: { processingGroup, sequenceIdentifier },
        select: { deadLetterId: true },
      })
      return row != null
    },

    async deadLetterSequence(sequenceIdentifier) {
      const client = getClient()
      return (await sequenceRows(client, sequenceIdentifier)).map(rowToLetter)
    },

    async sequenceIdentifiers() {
      return distinctSequences(getClient())
    },

    async process(sequenceFilter, processingTask) {
      const client = getClient()
      const candidates = (await distinctSequences(client)).filter(sequenceFilter)
      if (candidates.length === 0) return false

      // Pick the oldest sequence by its head letter's lastTouched, skipping
      // sequences under an unexpired processing lease (multi-node safety).
      const cutoff = Date.now() - claimDurationMs
      let chosen: string | undefined
      let oldest = Infinity
      for (const seqId of candidates) {
        const rows = await sequenceRows(client, seqId)
        if (rows.length === 0) continue
        const head = rows[0]
        const leased = head.processingStarted != null && Number(head.processingStarted) > cutoff
        if (leased) continue
        if (Number(head.lastTouched) < oldest) {
          oldest = Number(head.lastTouched)
          chosen = seqId
        }
      }
      if (!chosen) return false

      // Claim the sequence head's lease for the duration of this pass.
      const headRows = await sequenceRows(client, chosen)
      await client.kronosDeadLetter.updateMany({
        where: { processingGroup, deadLetterId: headRows[0].deadLetterId },
        data: { processingStarted: String(Date.now()) },
      })

      try {
        for (const row of headRows) {
          const letter = rowToLetter(row)
          const decision: EnqueueDecision = await processingTask(letter)
          if (decision.shouldEnqueue) {
            await this.requeue(letter, { cause: decision.cause, diagnostics: decision.diagnostics })
            return true
          }
          await this.evict(chosen, letter)
        }
        return true
      } finally {
        // Release any lease still set on a surviving head.
        const remaining = await sequenceRows(client, chosen)
        if (remaining.length > 0 && remaining[0].processingStarted != null) {
          await client.kronosDeadLetter.updateMany({
            where: { processingGroup, deadLetterId: remaining[0].deadLetterId },
            data: { processingStarted: null },
          })
        }
      }
    },

    async size() {
      const client = getClient()
      return client.kronosDeadLetter.count({ where: { processingGroup } })
    },

    async amountOfSequences() {
      return (await distinctSequences(getClient())).length
    },

    async clear() {
      const client = getClient()
      await client.kronosDeadLetter.deleteMany({ where: { processingGroup } })
    },

    async isFull(sequenceIdentifier) {
      const client = getClient()
      const rows = await sequenceRows(client, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(client)).length >= maxSequences
    },
  }
}
