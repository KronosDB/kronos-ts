import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "@kronos-ts/core"
import { DeadLetterQueueOverflowError, type UnitOfWork } from "@kronos-ts/core"
import type {PrismaClientLike} from "./prisma-transaction.js"
import { activePrismaTransaction } from "./prisma-transaction.js"

/**
 * Persistent {@link SequencedDeadLetterQueue} backed by Prisma — a faithful
 * translation of the Drizzle reference DLQ (`drizzle-dead-letter-queue.ts`)
 * onto Prisma's delegate API.
 *
 * Like {@link prismaTokenStore} it reads the active transaction via
 * the unit of work handed to each method, so enqueue/evict/requeue commit in the **same
 * transaction as the token update** — a crash cannot advance the processor's
 * token while losing the parked letter.
 *
 * The table is shared across processors and partitioned by `group`.
 * Per-sequence FIFO order is held by a monotonic `sequenceIndex`; a
 * `processingStarted` lease column makes `process()` safe across multiple
 * nodes (Axon parity).
 *
 * Expected shape of the `kronos_dead_letters` table in the Prisma schema.
 *
 * ```prisma
 * model KronosDeadLetter {
 *   deadLetterId       String  @id @map("dead_letter_id")
 *   group    String  @map("processing_group")
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
 *   @@index([group, sequenceIdentifier, sequenceIndex], name: "kronos_dl_seq")
 *   @@map("kronos_dead_letters")
 * }
 * ```
 */
/** Tuning only — everything required is a positional argument. */
export type PrismaDeadLetterQueueOptions = {
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

type DeadLetterRow = {
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
 * the unit of work's transaction (read off the trailing `uow` parameter) so dead-letter writes
 * and token/projection updates are atomic.
 *
 * ```typescript
 * import { prismaDeadLetterQueue } from "@kronos-ts/prisma"
 *
 * const dlq = prismaDeadLetterQueue(prisma)
 * ```
 *
 * The PROCESSING GROUP is per CALL, not per construction — every method takes
 * it first, exactly as a token store takes `processorName`. One queue object is
 * one table, and which partition a call touches is the caller's business:
 *
 * ```typescript
 * await dlq.enqueue("balances", letter, uow)
 * await dlq.clear("balances")
 * ```
 */
export function prismaDeadLetterQueue(
  prisma: PrismaClientLike,
  options: PrismaDeadLetterQueueOptions = {},
): SequencedDeadLetterQueue<UnitOfWork> {
  const maxSequences = options.maxSequences ?? 1024
  const maxSequenceSize = options.maxSequenceSize ?? 1024
  const claimDurationMs = options.claimDurationMs ?? 30000

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction.
   */
  function getClient(uow?: UnitOfWork): any {
    const tx = activePrismaTransaction(uow)
    if (tx !== undefined) return tx
    // NO SILENT FALLBACK. A dead-letter write that lands outside the batch's
    // transaction is the failure this store exists to avoid: it commits on its
    // own, a crash lands between it and the projection it accounts for, and the
    // read model is permanently wrong with nothing to read as the cause. A
    // handler's accessor may fall back — whether a seam is transactional is a
    // deployment decision — but this one may not.
    if (uow !== undefined) {
      throw new Error(
        "@kronos-ts/prisma: this unit of work carries no prisma transaction, so the " +
          "dead-letter write would commit outside the batch it accounts for. Build the " +
          "processor's unitOfWork with `prismaUnitOfWork(next, prisma)`.",
      )
    }
    // No unit of work at all — lifecycle and admin paths, which are honestly
    // outside any transaction.
    return prisma
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

  function letterToRow(
    group: string,
    letter: DeadLetter,
    sequenceIndex: number,
    deadLetterId: string,
  ): DeadLetterRow {
    const { [DL_ID]: _omit, ...diagnostics } = letter.diagnostics as Record<string, unknown>
    return {
      deadLetterId,
      processingGroup: group,
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

  async function sequenceRows(client: any, group: string, seqId: string): Promise<DeadLetterRow[]> {
    return client.kronosDeadLetter.findMany({
      where: { group, sequenceIdentifier: seqId },
      orderBy: { sequenceIndex: "asc" },
    })
  }

  async function distinctSequences(client: any, group: string): Promise<string[]> {
    const rows = await client.kronosDeadLetter.findMany({
      where: { group },
      select: { sequenceIdentifier: true },
      distinct: ["sequenceIdentifier"],
    })
    return rows.map((r: { sequenceIdentifier: string }) => r.sequenceIdentifier)
  }

  const queue: SequencedDeadLetterQueue = {
    async enqueue(group, letter, uow) {
      const client = getClient(uow)
      const existing = await sequenceRows(client, group, letter.sequenceIdentifier)
      if (existing.length === 0) {
        if ((await distinctSequences(client, group)).length >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
      } else if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing.length === 0 ? 0 : existing[existing.length - 1].sequenceIndex + 1
      await client.kronosDeadLetter.create({ data: letterToRow(group, letter, idx, newId(group)) })
    },

    async enqueueIfPresent(group, sequenceIdentifier, letterSupplier, uow) {
      const client = getClient(uow)
      const existing = await sequenceRows(client, group, sequenceIdentifier)
      if (existing.length === 0) return false
      if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing[existing.length - 1].sequenceIndex + 1
      await client.kronosDeadLetter.create({
        data: letterToRow(group, letterSupplier(), idx, newId(group)),
      })
      return true
    },

    async evict(group, _sequenceIdentifier, letter, uow) {
      const client = getClient(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await client.kronosDeadLetter.deleteMany({
        where: { group, deadLetterId: id },
      })
    },

    async requeue(group, letter, update, uow) {
      const client = getClient(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiag } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics ? { ...baseDiag, ...update.diagnostics } : baseDiag
      await client.kronosDeadLetter.updateMany({
        where: { group, deadLetterId: id },
        data: {
          causeType: cause.name,
          causeMessage: cause.message,
          diagnostics: JSON.stringify(diagnostics),
          lastTouched: String(Date.now()),
        },
      })
    },

    async contains(group, sequenceIdentifier, uow) {
      const client = getClient(uow)
      const row = await client.kronosDeadLetter.findFirst({
        where: { group, sequenceIdentifier },
        select: { deadLetterId: true },
      })
      return row != null
    },

    async deadLetterSequence(group, sequenceIdentifier, uow) {
      const client = getClient(uow)
      return (await sequenceRows(client, group, sequenceIdentifier)).map(rowToLetter)
    },

    async sequenceIdentifiers(group, uow) {
      return distinctSequences(getClient(uow), group)
    },

    async process(group, sequenceFilter, processingTask, uow) {
      const client = getClient(uow)
      const candidates = (await distinctSequences(client, group)).filter(sequenceFilter)
      if (candidates.length === 0) return false

      // Pick the oldest sequence by its head letter's lastTouched, skipping
      // sequences under an unexpired processing lease (multi-node safety).
      const cutoff = Date.now() - claimDurationMs
      let chosen: string | undefined
      let oldest = Infinity
      for (const seqId of candidates) {
        const rows = await sequenceRows(client, group, seqId)
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
      const headRows = await sequenceRows(client, group, chosen)
      await client.kronosDeadLetter.updateMany({
        where: { group, deadLetterId: headRows[0].deadLetterId },
        data: { processingStarted: String(Date.now()) },
      })

      try {
        for (const row of headRows) {
          const letter = rowToLetter(row)
          const decision: EnqueueDecision = await processingTask(letter)
          if (decision.shouldEnqueue) {
            await queue.requeue(
              group,
              letter,
              { cause: decision.cause, diagnostics: decision.diagnostics },
              uow,
            )
            return true
          }
          await queue.evict(group, chosen, letter, uow)
        }
        return true
      } finally {
        // Release any lease still set on a surviving head.
        const remaining = await sequenceRows(client, group, chosen)
        if (remaining.length > 0 && remaining[0].processingStarted != null) {
          await client.kronosDeadLetter.updateMany({
            where: { group, deadLetterId: remaining[0].deadLetterId },
            data: { processingStarted: null },
          })
        }
      }
    },

    async size(group, uow) {
      const client = getClient(uow)
      return client.kronosDeadLetter.count({ where: { group } })
    },

    async amountOfSequences(group, uow) {
      return (await distinctSequences(getClient(uow), group)).length
    },

    async clear(group, uow) {
      const client = getClient(uow)
      await client.kronosDeadLetter.deleteMany({ where: { group } })
    },

    async isFull(group, sequenceIdentifier, uow) {
      const client = getClient(uow)
      const rows = await sequenceRows(client, group, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(client, group)).length >= maxSequences
    },
  }

  return queue
}
