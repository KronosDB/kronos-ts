import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "@kronos-ts/messaging"
import { getActiveTransaction, DeadLetterQueueOverflowError } from "@kronos-ts/messaging"
import type { KnexTransaction } from "./knex-transaction-manager.js"
import type { KnexQueryable } from "./knex-token-store.js"

/**
 * Persistent {@link SequencedDeadLetterQueue} backed by Knex — mirrors the
 * Drizzle reference implementation, translated to Knex's query builder style.
 *
 * Like {@link knexTokenStore} it reads the active transaction via
 * `getActiveTransaction()`, so enqueue/evict/requeue commit in the **same
 * transaction as the token update** — a crash cannot advance the processor's
 * token while losing the parked letter.
 *
 * The table is shared across processors and partitioned by `processingGroup`.
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
export interface KnexDeadLetterQueueConfig {
  /** Processing group (the processor name) this queue serves. */
  processingGroup: string
  /** Table name. Default: "kronos_dead_letters". */
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

/**
 * Creates a {@link SequencedDeadLetterQueue} backed by Knex.
 *
 * Participates in the active transaction via `getActiveTransaction()`.
 *
 * ```typescript
 * import { knexDeadLetterQueue } from "@kronos-ts/knex"
 *
 * const dlq = knexDeadLetterQueue(knex, { processingGroup: "my-processor" })
 * ```
 */
export function knexDeadLetterQueue(
  knex: KnexQueryable,
  config: KnexDeadLetterQueueConfig,
): SequencedDeadLetterQueue {
  const { processingGroup } = config
  const table = config.tableName ?? "kronos_dead_letters"
  const maxSequences = config.maxSequences ?? 1024
  const maxSequenceSize = config.maxSequenceSize ?? 1024
  const claimDurationMs = config.claimDurationMs ?? 30000

  function getKnex(): KnexQueryable {
    return getActiveTransaction<KnexTransaction>() ?? knex
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

  function letterToRow(letter: DeadLetter, sequenceIndex: number, deadLetterId: string) {
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
      processing_started: null as string | null,
    }
  }

  async function sequenceRows(k: KnexQueryable, seqId: string): Promise<any[]> {
    return k(table)
      .where({ processing_group: processingGroup, sequence_identifier: seqId })
      .orderBy("sequence_index", "asc")
  }

  async function distinctSequences(k: KnexQueryable): Promise<string[]> {
    const rows = await k(table)
      .where({ processing_group: processingGroup })
      .distinct("sequence_identifier")
    return rows.map((r: any) => r.sequence_identifier)
  }

  return {
    async enqueue(letter) {
      const k = getKnex()
      const existing = await sequenceRows(k, letter.sequenceIdentifier)
      if (existing.length === 0) {
        if ((await distinctSequences(k)).length >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
      } else if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing.length === 0 ? 0 : existing[existing.length - 1].sequence_index + 1
      await k(table).insert(letterToRow(letter, idx, newId(processingGroup)))
    },

    async enqueueIfPresent(sequenceIdentifier, letterSupplier) {
      const k = getKnex()
      const existing = await sequenceRows(k, sequenceIdentifier)
      if (existing.length === 0) return false
      if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = existing[existing.length - 1].sequence_index + 1
      await k(table).insert(letterToRow(letterSupplier(), idx, newId(processingGroup)))
      return true
    },

    async evict(_sequenceIdentifier, letter) {
      const k = getKnex()
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await k(table)
        .where({ processing_group: processingGroup, dead_letter_id: id })
        .delete()
    },

    async requeue(letter, update) {
      const k = getKnex()
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiag } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics ? { ...baseDiag, ...update.diagnostics } : baseDiag
      await k(table)
        .where({ processing_group: processingGroup, dead_letter_id: id })
        .update({
          cause_type: cause.name,
          cause_message: cause.message,
          diagnostics: JSON.stringify(diagnostics),
          last_touched: String(Date.now()),
        })
    },

    async contains(sequenceIdentifier) {
      const k = getKnex()
      const row = await k(table)
        .where({ processing_group: processingGroup, sequence_identifier: sequenceIdentifier })
        .first()
      return row != null
    },

    async deadLetterSequence(sequenceIdentifier) {
      const k = getKnex()
      return (await sequenceRows(k, sequenceIdentifier)).map(rowToLetter)
    },

    async sequenceIdentifiers() {
      return distinctSequences(getKnex())
    },

    async process(sequenceFilter, processingTask) {
      const k = getKnex()
      const candidates = (await distinctSequences(k)).filter(sequenceFilter)
      if (candidates.length === 0) return false

      // Pick the oldest sequence by its head letter's lastTouched, skipping
      // sequences under an unexpired processing lease (multi-node safety).
      const cutoff = Date.now() - claimDurationMs
      let chosen: string | undefined
      let oldest = Infinity
      for (const seqId of candidates) {
        const rows = await sequenceRows(k, seqId)
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
      const headRows = await sequenceRows(k, chosen)
      await k(table)
        .where({ processing_group: processingGroup, dead_letter_id: headRows[0].dead_letter_id })
        .update({ processing_started: String(Date.now()) })

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
        const remaining = await sequenceRows(k, chosen)
        if (remaining.length > 0 && remaining[0].processing_started != null) {
          await k(table)
            .where({ processing_group: processingGroup, dead_letter_id: remaining[0].dead_letter_id })
            .update({ processing_started: null })
        }
      }
    },

    async size() {
      const k = getKnex()
      const rows = await k(table)
        .where({ processing_group: processingGroup })
        .select("dead_letter_id")
      return rows.length
    },

    async amountOfSequences() {
      return (await distinctSequences(getKnex())).length
    },

    async clear() {
      const k = getKnex()
      await k(table)
        .where({ processing_group: processingGroup })
        .delete()
    },

    async isFull(sequenceIdentifier) {
      const k = getKnex()
      const rows = await sequenceRows(k, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(k)).length >= maxSequences
    },
  }
}
