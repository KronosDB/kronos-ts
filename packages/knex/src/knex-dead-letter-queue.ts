import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "@kronos-ts/core"
import { DeadLetterQueueOverflowError, type UnitOfWork } from "@kronos-ts/core"
import type {KnexClient} from "./knex-transaction.js"
import { activeKnexTransaction } from "./knex-transaction.js"

/**
 * Persistent {@link SequencedDeadLetterQueue} backed by Knex — mirrors the
 * Drizzle reference implementation, translated to Knex's query builder style.
 *
 * Like {@link knexTokenStore} it reads the active transaction via
 * the unit of work handed to each method, so enqueue/evict/requeue commit in the **same
 * transaction as the token update** — a crash cannot advance the processor's
 * token while losing the parked letter.
 *
 * The table is shared across processors and partitioned by `group`.
 * Per-sequence FIFO order is held by a monotonic `sequence_index`; a
 * `processing_started` lease column makes `process()` safe across multiple
 * nodes (Axon parity).
 *
 * Expected table (`kronos_dead_letters`), columns:
 * - `dead_letter_id` (PK) — persistent row id (varchar)
 * - `processing_group` — the processor name partition (varchar, not null)
 * - `sequence_identifier` — the sequence this letter belongs to (varchar, not null)
 * - `sequence_index` — monotonic per-sequence FIFO index (integer, not null)
 * - `message` — JSON-serialized EventMessage (text, not null)
 * - `cause_type` — error name / type (varchar, nullable)
 * - `cause_message` — error message (text, nullable)
 * - `diagnostics` — JSON-serialized diagnostics map (text, not null)
 * - `enqueued_at` — epoch-ms as string (varchar, not null)
 * - `last_touched` — epoch-ms as string (varchar, not null)
 * - `processing_started` — epoch-ms lease as string (varchar, nullable)
 *
 * A composite index on `(processing_group, sequence_identifier, sequence_index)`
 * keeps per-sequence reads ordered.
 */

/** Tuning only — everything required is a positional argument. */
export type KnexDeadLetterQueueOptions = {
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
export const KNEX_DEAD_LETTER_TABLE = "kronos_dead_letters"

let idCounter = 0
function newId(group: string): string {
  // Unique within the table: time + per-process counter + group.
  idCounter += 1
  return `${group}:${Date.now()}:${idCounter}`
}

/**
 * Creates a {@link SequencedDeadLetterQueue} backed by Knex.
 *
 * Participates in the unit of work's transaction, read off the trailing
 * `uow` parameter every method now takes.
 *
 * ```typescript
 * import { knexDeadLetterQueue } from "@kronos-ts/knex"
 *
 * const dlq = knexDeadLetterQueue(knex)
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
export function knexDeadLetterQueue(
  knex: KnexClient,
  options: KnexDeadLetterQueueOptions = {},
): SequencedDeadLetterQueue<UnitOfWork> {
  const table = KNEX_DEAD_LETTER_TABLE
  const maxSequences = options.maxSequences ?? 1024
  const maxSequenceSize = options.maxSequenceSize ?? 1024
  const claimDurationMs = options.claimDurationMs ?? 30000

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction.
   */
  function getKnex(uow?: UnitOfWork): any {
    const tx = activeKnexTransaction(uow)
    if (tx !== undefined) return tx
    // NO SILENT FALLBACK. A dead-letter write that lands outside the batch's
    // transaction is the failure this store exists to avoid: it commits on its
    // own, a crash lands between it and the projection it accounts for, and the
    // read model is permanently wrong with nothing to read as the cause. A
    // handler's accessor may fall back — whether a seam is transactional is a
    // deployment decision — but this one may not.
    if (uow !== undefined) {
      throw new Error(
        "@kronos-ts/knex: this unit of work carries no knex transaction, so the " +
          "dead-letter write would commit outside the batch it accounts for. Build the " +
          "processor's unitOfWork with `knexUnitOfWork(next, knex)`.",
      )
    }
    // No unit of work at all — lifecycle and admin paths, which are honestly
    // outside any transaction.
    return knex
  }

  function rowToLetter(row: any): DeadLetter {
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
  ) {
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
      processing_started: null as string | null,
    }
  }

  async function sequenceRows(k: any, group: string, seqId: string): Promise<any[]> {
    return k(table)
      .where({ processing_group: group, sequence_identifier: seqId })
      .orderBy("sequence_index", "asc")
  }

  async function distinctSequences(k: any, group: string): Promise<string[]> {
    const rows = await k(table).where({ processing_group: group }).distinct("sequence_identifier")
    return rows.map((r: any) => r.sequence_identifier)
  }

  const queue: SequencedDeadLetterQueue = {
    async enqueue(group, letter, uow) {
      const k = getKnex(uow)
      const existing = await sequenceRows(k, group, letter.sequenceIdentifier)
      if (existing.length === 0) {
        if ((await distinctSequences(k, group)).length >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
      } else if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing.length === 0 ? 0 : existing[existing.length - 1].sequence_index + 1
      await k(table).insert(letterToRow(group, letter, idx, newId(group)))
    },

    async enqueueIfPresent(group, sequenceIdentifier, letterSupplier, uow) {
      const k = getKnex(uow)
      const existing = await sequenceRows(k, group, sequenceIdentifier)
      if (existing.length === 0) return false
      if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing[existing.length - 1].sequence_index + 1
      await k(table).insert(letterToRow(group, letterSupplier(), idx, newId(group)))
      return true
    },

    async evict(group, _sequenceIdentifier, letter, uow) {
      const k = getKnex(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await k(table).where({ processing_group: group, dead_letter_id: id }).delete()
    },

    async requeue(group, letter, update, uow) {
      const k = getKnex(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiag } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics ? { ...baseDiag, ...update.diagnostics } : baseDiag
      await k(table)
        .where({ processing_group: group, dead_letter_id: id })
        .update({
          cause_type: cause.name,
          cause_message: cause.message,
          diagnostics: JSON.stringify(diagnostics),
          last_touched: String(Date.now()),
        })
    },

    async contains(group, sequenceIdentifier, uow) {
      const k = getKnex(uow)
      const row = await k(table)
        .where({ processing_group: group, sequence_identifier: sequenceIdentifier })
        .first()
      return row != null
    },

    async deadLetterSequence(group, sequenceIdentifier, uow) {
      const k = getKnex(uow)
      return (await sequenceRows(k, group, sequenceIdentifier)).map(rowToLetter)
    },

    async sequenceIdentifiers(group, uow) {
      return distinctSequences(getKnex(uow), group)
    },

    async process(group, sequenceFilter, processingTask, uow) {
      const k = getKnex(uow)
      const candidates = (await distinctSequences(k, group)).filter(sequenceFilter)
      if (candidates.length === 0) return false

      // Pick the oldest sequence by its head letter's lastTouched, skipping
      // sequences under an unexpired processing lease (multi-node safety).
      const cutoff = Date.now() - claimDurationMs
      let chosen: string | undefined
      let oldest = Infinity
      for (const seqId of candidates) {
        const rows = await sequenceRows(k, group, seqId)
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
      const headRows = await sequenceRows(k, group, chosen)
      await k(table)
        .where({ processing_group: group, dead_letter_id: headRows[0].dead_letter_id })
        .update({ processing_started: String(Date.now()) })

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
        const remaining = await sequenceRows(k, group, chosen)
        if (remaining.length > 0 && remaining[0].processing_started != null) {
          await k(table)
            .where({ processing_group: group, dead_letter_id: remaining[0].dead_letter_id })
            .update({ processing_started: null })
        }
      }
    },

    async size(group, uow) {
      const k = getKnex(uow)
      const rows = await k(table).where({ processing_group: group }).select("dead_letter_id")
      return rows.length
    },

    async amountOfSequences(group, uow) {
      return (await distinctSequences(getKnex(uow), group)).length
    },

    async clear(group, uow) {
      const k = getKnex(uow)
      await k(table).where({ processing_group: group }).delete()
    },

    async isFull(group, sequenceIdentifier, uow) {
      const k = getKnex(uow)
      const rows = await sequenceRows(k, group, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(k, group)).length >= maxSequences
    },
  }

  return queue
}
