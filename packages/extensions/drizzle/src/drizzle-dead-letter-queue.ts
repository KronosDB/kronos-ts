import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "@kronos-ts/messaging"
import { getActiveTransaction, DeadLetterQueueOverflowError } from "@kronos-ts/messaging"
import type { DrizzleTransaction } from "./drizzle-transaction-manager.js"

/**
 * Persistent {@link SequencedDeadLetterQueue} backed by Drizzle ORM — the
 * reference implementation other extension backends mirror.
 *
 * Like {@link drizzleTokenStore} it reads the active transaction via
 * `getActiveTransaction()`, so enqueue/evict/requeue commit in the **same
 * transaction as the token update** — a crash cannot advance the processor's
 * token while losing the parked letter.
 *
 * The table is shared across processors and partitioned by `processingGroup`.
 * Per-sequence FIFO order is held by a monotonic `sequence_index`; a `processing_started`
 * lease column makes `process()` safe across multiple nodes (Axon parity).
 *
 * Expected table (`kronos_dead_letters`):
 * ```typescript
 * import { pgTable, varchar, integer, text, primaryKey, index } from "drizzle-orm/pg-core"
 *
 * export const kronosDeadLetters = pgTable("kronos_dead_letters", {
 *   deadLetterId: varchar("dead_letter_id", { length: 255 }).primaryKey(),
 *   processingGroup: varchar("processing_group", { length: 255 }).notNull(),
 *   sequenceIdentifier: varchar("sequence_identifier", { length: 255 }).notNull(),
 *   sequenceIndex: integer("sequence_index").notNull(),
 *   message: text("message").notNull(),
 *   causeType: varchar("cause_type", { length: 255 }),
 *   causeMessage: text("cause_message"),
 *   diagnostics: text("diagnostics").notNull(),
 *   enqueuedAt: varchar("enqueued_at", { length: 32 }).notNull(),
 *   lastTouched: varchar("last_touched", { length: 32 }).notNull(),
 *   processingStarted: varchar("processing_started", { length: 32 }),
 * }, (t) => ({
 *   seq: index("kronos_dl_seq").on(t.processingGroup, t.sequenceIdentifier, t.sequenceIndex),
 * }))
 * ```
 */
export interface DrizzleDeadLetterQueueConfig {
  /** The Drizzle database instance. */
  db: { select: (...a: any[]) => any; insert: (...a: any[]) => any; update: (...a: any[]) => any; delete: (...a: any[]) => any }
  /** The Drizzle table reference for `kronos_dead_letters`. */
  table: any
  /** Processing group (the processor name) this queue serves. */
  processingGroup: string
  /** Drizzle `eq` operator. */
  eq: (column: any, value: any) => any
  /** Drizzle `and` operator. */
  and: (...conditions: any[]) => any
  /** Drizzle `asc` ordering helper. */
  asc: (column: any) => any
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

export function drizzleDeadLetterQueue(config: DrizzleDeadLetterQueueConfig): SequencedDeadLetterQueue {
  const { table, processingGroup, eq, and, asc } = config
  const maxSequences = config.maxSequences ?? 1024
  const maxSequenceSize = config.maxSequenceSize ?? 1024
  const claimDurationMs = config.claimDurationMs ?? 30000

  function getDb(): any {
    return getActiveTransaction<DrizzleTransaction>() ?? config.db
  }
  const inGroup = (seqId?: string) =>
    seqId === undefined
      ? eq(table.processingGroup, processingGroup)
      : and(eq(table.processingGroup, processingGroup), eq(table.sequenceIdentifier, seqId))

  function rowToLetter(row: any): DeadLetter {
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

  function letterToRow(letter: DeadLetter, sequenceIndex: number, deadLetterId: string) {
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
      processingStarted: null as string | null,
    }
  }

  async function sequenceRows(db: any, seqId: string): Promise<any[]> {
    return db.select().from(table).where(inGroup(seqId)).orderBy(asc(table.sequenceIndex))
  }

  async function distinctSequences(db: any): Promise<string[]> {
    const rows = await db.selectDistinct({ s: table.sequenceIdentifier }).from(table).where(inGroup())
    return rows.map((r: any) => r.s)
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
      const idx = existing.length === 0 ? 0 : existing[existing.length - 1].sequenceIndex + 1
      await db.insert(table).values(letterToRow(letter, idx, newId(processingGroup)))
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
      const idx = existing[existing.length - 1].sequenceIndex + 1
      await db.insert(table).values(letterToRow(letterSupplier(), idx, newId(processingGroup)))
      return true
    },

    async evict(_sequenceIdentifier, letter) {
      const db = getDb()
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await db.delete(table).where(and(eq(table.processingGroup, processingGroup), eq(table.deadLetterId, id)))
    },

    async requeue(letter, update) {
      const db = getDb()
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiag } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics ? { ...baseDiag, ...update.diagnostics } : baseDiag
      await db.update(table)
        .set({
          causeType: cause.name,
          causeMessage: cause.message,
          diagnostics: JSON.stringify(diagnostics),
          lastTouched: String(Date.now()),
        })
        .where(and(eq(table.processingGroup, processingGroup), eq(table.deadLetterId, id)))
    },

    async contains(sequenceIdentifier) {
      const db = getDb()
      const rows = await db.select().from(table).where(inGroup(sequenceIdentifier)).limit(1)
      return rows.length > 0
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
        const leased = head.processingStarted != null && Number(head.processingStarted) > cutoff
        if (leased) continue
        if (Number(head.lastTouched) < oldest) {
          oldest = Number(head.lastTouched)
          chosen = seqId
        }
      }
      if (!chosen) return false

      // Claim the sequence head's lease for the duration of this pass.
      const headRows = await sequenceRows(db, chosen)
      await db.update(table)
        .set({ processingStarted: String(Date.now()) })
        .where(and(eq(table.processingGroup, processingGroup), eq(table.deadLetterId, headRows[0].deadLetterId)))

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
        if (remaining.length > 0 && remaining[0].processingStarted != null) {
          await db.update(table)
            .set({ processingStarted: null })
            .where(and(eq(table.processingGroup, processingGroup), eq(table.deadLetterId, remaining[0].deadLetterId)))
        }
      }
    },

    async size() {
      const db = getDb()
      const rows = await db.select({ id: table.deadLetterId }).from(table).where(inGroup())
      return rows.length
    },

    async amountOfSequences() {
      return (await distinctSequences(getDb())).length
    },

    async clear() {
      const db = getDb()
      await db.delete(table).where(inGroup())
    },

    async isFull(sequenceIdentifier) {
      const db = getDb()
      const rows = await sequenceRows(db, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(db)).length >= maxSequences
    },
  }
}
