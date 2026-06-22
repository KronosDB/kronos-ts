import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "@kronos-ts/messaging"
import { getActiveTransaction, DeadLetterQueueOverflowError } from "@kronos-ts/messaging"
import type { KyselyTransaction } from "./kysely-transaction-manager.js"
import type { KyselyDbLike } from "./kysely-token-store.js"

/**
 * Dead letter table interface for Kysely. Users must define this table in
 * their Kysely database interface:
 *
 * ```typescript
 * interface Database {
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
 * `getActiveTransaction()`, so enqueue/evict/requeue commit in the **same
 * transaction as the token update** — a crash cannot advance the processor's
 * token while losing the parked letter.
 *
 * The table is shared across processors and partitioned by `processingGroup`.
 * Per-sequence FIFO order is held by a monotonic `sequence_index`; a
 * `processing_started` lease column makes `process()` safe across multiple
 * nodes (Axon parity).
 */
export interface KyselyDeadLetterQueueConfig {
  /** The Kysely database instance (or transaction). */
  db: KyselyDbLike
  /** Processing group (the processor name) this queue serves. */
  processingGroup: string
  /** Table name. Default: `kronos_dead_letters`. */
  tableName?: string
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

export function kyselyDeadLetterQueue(config: KyselyDeadLetterQueueConfig): SequencedDeadLetterQueue {
  const { processingGroup } = config
  const table = config.tableName ?? "kronos_dead_letters"
  const maxSequences = config.maxSequences ?? 1024
  const maxSequenceSize = config.maxSequenceSize ?? 1024
  const claimDurationMs = config.claimDurationMs ?? 30000

  function getDb(): KyselyDbLike {
    return getActiveTransaction<KyselyTransaction>() ?? config.db
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

  function letterToRow(letter: DeadLetter, sequenceIndex: number, deadLetterId: string): DeadLetterRow {
    const { [DL_ID]: _omit, ...diagnostics } = letter.diagnostics as Record<string, unknown>
    return {
      dead_letter_id: deadLetterId,
      processing_group: processingGroup,
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

  async function sequenceRows(db: KyselyDbLike, seqId: string): Promise<DeadLetterRow[]> {
    return db.selectFrom(table)
      .selectAll()
      .where("processing_group", "=", processingGroup)
      .where("sequence_identifier", "=", seqId)
      .orderBy("sequence_index", "asc")
      .execute()
  }

  async function distinctSequences(db: KyselyDbLike): Promise<string[]> {
    const rows = await db.selectFrom(table)
      .select("sequence_identifier")
      .distinct()
      .where("processing_group", "=", processingGroup)
      .execute()
    return rows.map((r: { sequence_identifier: string }) => r.sequence_identifier)
  }

  return {
    async enqueue(letter) {
      const db = getDb()
      const existing = await sequenceRows(db, letter.sequenceIdentifier)
      if (existing.length === 0) {
        if ((await distinctSequences(db)).length >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
      } else if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing.length === 0 ? 0 : existing[existing.length - 1].sequence_index + 1
      await db.insertInto(table).values(letterToRow(letter, idx, newId(processingGroup))).execute()
    },

    async enqueueIfPresent(sequenceIdentifier, letterSupplier) {
      const db = getDb()
      const existing = await sequenceRows(db, sequenceIdentifier)
      if (existing.length === 0) return false
      if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing[existing.length - 1].sequence_index + 1
      await db.insertInto(table).values(letterToRow(letterSupplier(), idx, newId(processingGroup))).execute()
      return true
    },

    async evict(_sequenceIdentifier, letter) {
      const db = getDb()
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await db.deleteFrom(table)
        .where("processing_group", "=", processingGroup)
        .where("dead_letter_id", "=", id)
        .execute()
    },

    async requeue(letter, update) {
      const db = getDb()
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiag } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics ? { ...baseDiag, ...update.diagnostics } : baseDiag
      await db.updateTable(table)
        .set({
          cause_type: cause.name,
          cause_message: cause.message,
          diagnostics: JSON.stringify(diagnostics),
          last_touched: String(Date.now()),
        })
        .where("processing_group", "=", processingGroup)
        .where("dead_letter_id", "=", id)
        .execute()
    },

    async contains(sequenceIdentifier) {
      const db = getDb()
      const row = await db.selectFrom(table)
        .select("dead_letter_id")
        .where("processing_group", "=", processingGroup)
        .where("sequence_identifier", "=", sequenceIdentifier)
        .limit(1)
        .executeTakeFirst()
      return row != null
    },

    async deadLetterSequence(sequenceIdentifier) {
      const db = getDb()
      return (await sequenceRows(db, sequenceIdentifier)).map(rowToLetter)
    },

    async sequenceIdentifiers() {
      return distinctSequences(getDb())
    },

    async process(sequenceFilter, processingTask) {
      const db = getDb()
      const candidates = (await distinctSequences(db)).filter(sequenceFilter)
      if (candidates.length === 0) return false

      // Pick the oldest sequence by its head letter's lastTouched, skipping
      // sequences under an unexpired processing lease (multi-node safety).
      const cutoff = Date.now() - claimDurationMs
      let chosen: string | undefined
      let oldest = Infinity
      for (const seqId of candidates) {
        const rows = await sequenceRows(db, seqId)
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
      const headRows = await sequenceRows(db, chosen)
      await db.updateTable(table)
        .set({ processing_started: String(Date.now()) })
        .where("processing_group", "=", processingGroup)
        .where("dead_letter_id", "=", headRows[0].dead_letter_id)
        .execute()

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
        const remaining = await sequenceRows(db, chosen)
        if (remaining.length > 0 && remaining[0].processing_started != null) {
          await db.updateTable(table)
            .set({ processing_started: null })
            .where("processing_group", "=", processingGroup)
            .where("dead_letter_id", "=", remaining[0].dead_letter_id)
            .execute()
        }
      }
    },

    async size() {
      const db = getDb()
      const rows = await db.selectFrom(table)
        .select("dead_letter_id")
        .where("processing_group", "=", processingGroup)
        .execute()
      return rows.length
    },

    async amountOfSequences() {
      return (await distinctSequences(getDb())).length
    },

    async clear() {
      const db = getDb()
      await db.deleteFrom(table)
        .where("processing_group", "=", processingGroup)
        .execute()
    },

    async isFull(sequenceIdentifier) {
      const db = getDb()
      const rows = await sequenceRows(db, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(db)).length >= maxSequences
    },
  }
}
