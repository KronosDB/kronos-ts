import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "@kronos-ts/core"
import { DeadLetterQueueOverflowError, type UnitOfWork } from "@kronos-ts/core"
import type { KyselyDb, KyselyUnitOfWork } from "./kysely-transaction.js"
import { activeKyselyTransaction } from "./kysely-transaction.js"

/**
 * Dead letter table shape for Kysely. Users must define this table in
 * their Kysely database type:
 *
 * ```typescript
 * type Database = {
 *   kronos_dead_letters: {
 *     dead_letter_id: string
 *     processing_group: string
 *     sequence_identifier: string
 *     sequence_index: number
 *     message: string
 *     cause_type: string | null
 *     cause_message: string | null
 *     diagnostics: string
 *     enqueued_at: string
 *     last_touched: string
 *     processing_started: string | null
 *   }
 * }
 * ```
 *
 * Persistent {@link SequencedDeadLetterQueue} backed by Kysely — mirrors the
 * Drizzle reference implementation in query semantics.
 *
 * Like {@link kyselyTokenStore} it reads the active transaction via
 * the unit of work handed to each method, so enqueue/evict/requeue commit in the **same
 * transaction as the token update** — a crash cannot advance the processor's
 * token while losing the parked letter.
 *
 * The table is shared across processors and partitioned by `group`.
 * Per-sequence FIFO order is held by a monotonic `sequence_index`; a
 * `processing_started` lease column makes `process()` safe across multiple
 * nodes (Axon parity).
 */

/** Tuning only — everything required is a positional argument. */
export type KyselyDeadLetterQueueOptions = {
  /** Maximum number of sequences. Default: 1024 (Axon parity). */
  maxSequences?: number
  /** Maximum letters per sequence. Default: 1024 (Axon parity). */
  maxSequenceSize?: number
  /** Lease duration for in-flight processing, ms. Default: 30000 (Axon parity). */
  claimDurationMs?: number
}

/** Reserved diagnostics key carrying the persistent row id across read → evict/requeue. */
const DL_ID = "__dlqId"

/** The table this adapter owns. Not a parameter — the columns are not the caller's choice. */
export const KYSELY_DEAD_LETTER_TABLE = "kronos_dead_letters"

let idCounter = 0
function newId(group: string): string {
  // Unique within the table: time + per-process counter + group.
  idCounter += 1
  return `${group}:${Date.now()}:${idCounter}`
}

type DeadLetterRow = {
  dead_letter_id: string
  processing_group: string
  sequence_identifier: string
  sequence_index: number
  message: string
  cause_type: string | null
  cause_message: string | null
  diagnostics: string
  enqueued_at: string
  last_touched: string
  processing_started: string | null
}

/**
 * Creates a {@link SequencedDeadLetterQueue} backed by Kysely.
 *
 * ```typescript
 * const dlq = kyselyDeadLetterQueue(db)
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
export function kyselyDeadLetterQueue(
  db: KyselyDb,
  options: KyselyDeadLetterQueueOptions = {},
): SequencedDeadLetterQueue<UnitOfWork & KyselyUnitOfWork> {
  const table = KYSELY_DEAD_LETTER_TABLE
  const maxSequences = options.maxSequences ?? 1024
  const maxSequenceSize = options.maxSequenceSize ?? 1024
  const claimDurationMs = options.claimDurationMs ?? 30000

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction.
   */
  function getDb(uow?: UnitOfWork): any {
    return activeKyselyTransaction(uow) ?? db
  }

  function rowToLetter(row: DeadLetterRow): DeadLetter {
    const cause = new Error(row.cause_message ?? "")
    if (row.cause_type) cause.name = row.cause_type
    return {
      message: JSON.parse(row.message),
      cause,
      enqueuedAt: Number(row.enqueued_at),
      lastTouched: Number(row.last_touched),
      diagnostics: { ...JSON.parse(row.diagnostics), [DL_ID]: row.dead_letter_id },
      sequenceIdentifier: row.sequence_identifier,
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
      dead_letter_id: deadLetterId,
      processing_group: group,
      sequence_identifier: letter.sequenceIdentifier,
      sequence_index: sequenceIndex,
      message: JSON.stringify(letter.message),
      cause_type: letter.cause.name,
      cause_message: letter.cause.message,
      diagnostics: JSON.stringify(diagnostics),
      enqueued_at: String(letter.enqueuedAt),
      last_touched: String(letter.lastTouched),
      processing_started: null,
    }
  }

  async function sequenceRows(db: any, group: string, seqId: string): Promise<DeadLetterRow[]> {
    return db
      .selectFrom(table)
      .selectAll()
      .where("processing_group", "=", group)
      .where("sequence_identifier", "=", seqId)
      .orderBy("sequence_index", "asc")
      .execute()
  }

  async function distinctSequences(db: any, group: string): Promise<string[]> {
    const rows = await db
      .selectFrom(table)
      .select("sequence_identifier")
      .distinct()
      .where("processing_group", "=", group)
      .execute()
    return rows.map((r: { sequence_identifier: string }) => r.sequence_identifier)
  }

  const queue: SequencedDeadLetterQueue = {
    async enqueue(group, letter, uow) {
      const db = getDb(uow)
      const existing = await sequenceRows(db, group, letter.sequenceIdentifier)
      if (existing.length === 0) {
        if ((await distinctSequences(db, group)).length >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
      } else if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing.length === 0 ? 0 : existing[existing.length - 1].sequence_index + 1
      await db
        .insertInto(table)
        .values(letterToRow(group, letter, idx, newId(group)))
        .execute()
    },

    async enqueueIfPresent(group, sequenceIdentifier, letterSupplier, uow) {
      const db = getDb(uow)
      const existing = await sequenceRows(db, group, sequenceIdentifier)
      if (existing.length === 0) return false
      if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing[existing.length - 1].sequence_index + 1
      await db
        .insertInto(table)
        .values(letterToRow(group, letterSupplier(), idx, newId(group)))
        .execute()
      return true
    },

    async evict(group, _sequenceIdentifier, letter, uow) {
      const db = getDb(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await db
        .deleteFrom(table)
        .where("processing_group", "=", group)
        .where("dead_letter_id", "=", id)
        .execute()
    },

    async requeue(group, letter, update, uow) {
      const db = getDb(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiag } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics ? { ...baseDiag, ...update.diagnostics } : baseDiag
      await db
        .updateTable(table)
        .set({
          cause_type: cause.name,
          cause_message: cause.message,
          diagnostics: JSON.stringify(diagnostics),
          last_touched: String(Date.now()),
        })
        .where("processing_group", "=", group)
        .where("dead_letter_id", "=", id)
        .execute()
    },

    async contains(group, sequenceIdentifier, uow) {
      const db = getDb(uow)
      const row = await db
        .selectFrom(table)
        .select("dead_letter_id")
        .where("processing_group", "=", group)
        .where("sequence_identifier", "=", sequenceIdentifier)
        .limit(1)
        .executeTakeFirst()
      return row != null
    },

    async deadLetterSequence(group, sequenceIdentifier, uow) {
      const db = getDb(uow)
      return (await sequenceRows(db, group, sequenceIdentifier)).map(rowToLetter)
    },

    async sequenceIdentifiers(group, uow) {
      return distinctSequences(getDb(uow), group)
    },

    async process(group, sequenceFilter, processingTask, uow) {
      const db = getDb(uow)
      const candidates = (await distinctSequences(db, group)).filter(sequenceFilter)
      if (candidates.length === 0) return false

      // Pick the oldest sequence by its head letter's lastTouched, skipping
      // sequences under an unexpired processing lease (multi-node safety).
      const cutoff = Date.now() - claimDurationMs
      let chosen: string | undefined
      let oldest = Infinity
      for (const seqId of candidates) {
        const rows = await sequenceRows(db, group, seqId)
        if (rows.length === 0) continue
        const head = rows[0]
        const leased = head.processing_started != null && Number(head.processing_started) > cutoff
        if (leased) continue
        if (Number(head.last_touched) < oldest) {
          oldest = Number(head.last_touched)
          chosen = seqId
        }
      }
      if (!chosen) return false

      // Claim the sequence head's lease for the duration of this pass.
      const headRows = await sequenceRows(db, group, chosen)
      await db
        .updateTable(table)
        .set({ processing_started: String(Date.now()) })
        .where("processing_group", "=", group)
        .where("dead_letter_id", "=", headRows[0].dead_letter_id)
        .execute()

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
        const remaining = await sequenceRows(db, group, chosen)
        if (remaining.length > 0 && remaining[0].processing_started != null) {
          await db
            .updateTable(table)
            .set({ processing_started: null })
            .where("processing_group", "=", group)
            .where("dead_letter_id", "=", remaining[0].dead_letter_id)
            .execute()
        }
      }
    },

    async size(group, uow) {
      const db = getDb(uow)
      const rows = await db
        .selectFrom(table)
        .select("dead_letter_id")
        .where("processing_group", "=", group)
        .execute()
      return rows.length
    },

    async amountOfSequences(group, uow) {
      return (await distinctSequences(getDb(uow), group)).length
    },

    async clear(group, uow) {
      const db = getDb(uow)
      await db.deleteFrom(table).where("processing_group", "=", group).execute()
    },

    async isFull(group, sequenceIdentifier, uow) {
      const db = getDb(uow)
      const rows = await sequenceRows(db, group, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(db, group)).length >= maxSequences
    },
  }

  return queue
}
