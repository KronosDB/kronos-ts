import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "@kronos-ts/core"
import { and, asc, eq } from "drizzle-orm"
import { DeadLetterQueueOverflowError, type UnitOfWork } from "@kronos-ts/core"
import type { DrizzleDb, DrizzleUnitOfWork } from "./drizzle-transaction.js"
import { activeDrizzleTransaction } from "./drizzle-transaction.js"
import { kronosDeadLetters } from "./drizzle-schema.js"

/**
 * Persistent {@link SequencedDeadLetterQueue} backed by Drizzle ORM — the
 * reference implementation other extension backends mirror.
 *
 * Like {@link drizzleTokenStore} it reads the active transaction via
 * the unit of work handed to each method, so enqueue/evict/requeue commit in the **same
 * transaction as the token update** — a crash cannot advance the processor's
 * token while losing the parked letter.
 *
 * The table is shared across processors and partitioned by `processingGroup`,
 * which every method takes as its FIRST argument — the same way a token store
 * takes `processorName`. One queue object is one table, and which partition a
 * call touches is a property of the caller.
 *
 * The table itself is owned by this adapter ({@link kronosDeadLetters}) and
 * exported for migrations. Per-sequence FIFO order is held by a monotonic
 * `sequence_index`; a `processing_started` lease column makes `process()` safe
 * across multiple nodes (Axon parity).
 */
/** Tuning only — everything required is a positional argument. */
export type DrizzleDeadLetterQueueOptions = {
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

/**
 * ```ts
 * const dlq = drizzleDeadLetterQueue(db)
 * ```
 *
 * The PROCESSING GROUP is per CALL, not per construction — every method takes
 * it first, exactly as a token store takes `processorName`. One queue object is
 * one table, and which partition a call touches is the caller's business:
 *
 * ```ts
 * await dlq.enqueue("balances", letter, uow)
 * await dlq.clear("balances")
 * ```
 *
 * Ordering is not a parameter. Per-sequence FIFO is this queue's CONTRACT, so
 * `asc` is imported from the extension's own `drizzle-orm` peer dependency
 * rather than handed in — a parameter with exactly one valid value is not
 * configuration, and passing the helper never protected against a
 * dual-instance `drizzle-orm` anyway (the table reference and the `db` handle
 * come from the caller's instance regardless).
 */
export function drizzleDeadLetterQueue(
  db: DrizzleDb,
  options: DrizzleDeadLetterQueueOptions = {},
): SequencedDeadLetterQueue<UnitOfWork & DrizzleUnitOfWork> {
  const table: any = kronosDeadLetters
  const maxSequences = options.maxSequences ?? 1024
  const maxSequenceSize = options.maxSequenceSize ?? 1024
  const claimDurationMs = options.claimDurationMs ?? 30000

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction, exactly as the old
   * permissive `getActiveTransaction()` did.
   */
  function getDb(uow?: UnitOfWork): any {
    return activeDrizzleTransaction(uow) ?? db
  }
  const inGroup = (group: string, seqId?: string) =>
    seqId === undefined
      ? eq(table.processingGroup, group)
      : and(eq(table.processingGroup, group), eq(table.sequenceIdentifier, seqId))

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

  function letterToRow(
    group: string,
    letter: DeadLetter,
    sequenceIndex: number,
    deadLetterId: string,
  ) {
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
      processingStarted: null as string | null,
    }
  }

  async function sequenceRows(db: any, group: string, seqId: string): Promise<any[]> {
    return db.select().from(table).where(inGroup(group, seqId)).orderBy(asc(table.sequenceIndex))
  }

  async function distinctSequences(db: any, group: string): Promise<string[]> {
    const rows = await db
      .selectDistinct({ s: table.sequenceIdentifier })
      .from(table)
      .where(inGroup(group))
    return rows.map((r: any) => r.s)
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
      const idx = existing.length === 0 ? 0 : existing[existing.length - 1].sequenceIndex + 1
      await db.insert(table).values(letterToRow(group, letter, idx, newId(group)))
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
      const idx = existing[existing.length - 1].sequenceIndex + 1
      await db.insert(table).values(letterToRow(group, letterSupplier(), idx, newId(group)))
      return true
    },

    async evict(group, _sequenceIdentifier, letter, uow) {
      const db = getDb(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await db
        .delete(table)
        .where(and(eq(table.processingGroup, group), eq(table.deadLetterId, id)))
    },

    async requeue(group, letter, update, uow) {
      const db = getDb(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiag } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics ? { ...baseDiag, ...update.diagnostics } : baseDiag
      await db
        .update(table)
        .set({
          causeType: cause.name,
          causeMessage: cause.message,
          diagnostics: JSON.stringify(diagnostics),
          lastTouched: String(Date.now()),
        })
        .where(and(eq(table.processingGroup, group), eq(table.deadLetterId, id)))
    },

    async contains(group, sequenceIdentifier, uow) {
      const db = getDb(uow)
      const rows = await db.select().from(table).where(inGroup(group, sequenceIdentifier)).limit(1)
      return rows.length > 0
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
        const leased = head.processingStarted != null && Number(head.processingStarted) > cutoff
        if (leased) continue
        if (Number(head.lastTouched) < oldest) {
          oldest = Number(head.lastTouched)
          chosen = seqId
        }
      }
      if (!chosen) return false

      // Claim the sequence head's lease for the duration of this pass.
      const headRows = await sequenceRows(db, group, chosen)
      await db
        .update(table)
        .set({ processingStarted: String(Date.now()) })
        .where(
          and(eq(table.processingGroup, group), eq(table.deadLetterId, headRows[0].deadLetterId)),
        )

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
        if (remaining.length > 0 && remaining[0].processingStarted != null) {
          await db
            .update(table)
            .set({ processingStarted: null })
            .where(
              and(
                eq(table.processingGroup, group),
                eq(table.deadLetterId, remaining[0].deadLetterId),
              ),
            )
        }
      }
    },

    async size(group, uow) {
      const db = getDb(uow)
      const rows = await db.select({ id: table.deadLetterId }).from(table).where(inGroup(group))
      return rows.length
    },

    async amountOfSequences(group, uow) {
      return (await distinctSequences(getDb(uow), group)).length
    },

    async clear(group, uow) {
      const db = getDb(uow)
      await db.delete(table).where(inGroup(group))
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
