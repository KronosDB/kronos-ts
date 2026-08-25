import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "@kronos-ts/core"
import { DeadLetterQueueOverflowError, type UnitOfWork } from "@kronos-ts/core"
import type { TypeormManager, TypeormUnitOfWork } from "./typeorm-transaction.js"
import { activeTypeormTransaction } from "./typeorm-transaction.js"

/**
 * Persistent {@link SequencedDeadLetterQueue} backed by TypeORM — a faithful
 * translation of the Drizzle reference implementation to TypeORM's raw-SQL
 * query style (`manager.query(sql, params)`), mirroring {@link typeormTokenStore}.
 *
 * Like {@link typeormTokenStore} it reads the active transaction via
 * the unit of work handed to each method, so enqueue/evict/requeue commit in the **same
 * transaction as the token update** — a crash cannot advance the processor's
 * token while losing the parked letter.
 *
 * The table is shared across processors and partitioned by `group`.
 * Per-sequence FIFO order is held by a monotonic `sequence_index`; a
 * `processing_started` lease column makes `process()` safe across multiple
 * nodes (Axon parity).
 *
 * Expected table (`kronos_dead_letters`). Users may define this entity:
 *
 * ```typescript
 * @Entity("kronos_dead_letters")
 * export class KronosDeadLetterEntry {
 *   @PrimaryColumn({ name: "dead_letter_id" }) deadLetterId: string
 *   @Column({ name: "processing_group" }) processingGroup: string
 *   @Column({ name: "sequence_identifier" }) sequenceIdentifier: string
 *   @Column({ name: "sequence_index" }) sequenceIndex: number
 *   @Column({ type: "text" }) message: string
 *   @Column({ name: "cause_type", nullable: true }) causeType: string | null
 *   @Column({ name: "cause_message", type: "text", nullable: true }) causeMessage: string | null
 *   @Column({ type: "text" }) diagnostics: string
 *   @Column({ name: "enqueued_at" }) enqueuedAt: string
 *   @Column({ name: "last_touched" }) lastTouched: string
 *   @Column({ name: "processing_started", nullable: true }) processingStarted: string | null
 * }
 * ```
 *
 * Column set:
 * - `dead_letter_id`     (PK)        — opaque persistent row id.
 * - `processing_group`               — the processor name; partitions the table.
 * - `sequence_identifier`            — the per-sequence FIFO key.
 * - `sequence_index`     (int)       — monotonic ordering within a sequence.
 * - `message`            (text)      — the {@link EventMessage} serialized as JSON.
 * - `cause_type`         (nullable)  — `Error.name` of the failure cause.
 * - `cause_message`      (text, null)— `Error.message` of the failure cause.
 * - `diagnostics`        (text)      — diagnostics map serialized as JSON.
 * - `enqueued_at`                    — epoch-ms timestamp as a string.
 * - `last_touched`                   — epoch-ms timestamp as a string.
 * - `processing_started` (nullable)  — epoch-ms lease timestamp as a string.
 */
/** Tuning only — everything required is a positional argument. */
export type TypeormDeadLetterQueueOptions = {
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
export const TYPEORM_DEAD_LETTER_TABLE = "kronos_dead_letters"

let idCounter = 0
function newId(group: string): string {
  // Unique within the table: time + per-process counter + group.
  idCounter += 1
  return `${group}:${Date.now()}:${idCounter}`
}

/**
 * Creates a {@link SequencedDeadLetterQueue} backed by TypeORM.
 *
 * Uses raw SQL queries via the EntityManager for maximum compatibility.
 * Participates in the unit of work's transaction, read off the trailing
 * `uow` parameter every method now takes.
 *
 * ```typescript
 * import { typeormDeadLetterQueue } from "@kronos-ts/typeorm"
 *
 * const dlq = typeormDeadLetterQueue(dataSource.manager)
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
export function typeormDeadLetterQueue(
  manager: TypeormManager,
  options: TypeormDeadLetterQueueOptions = {},
): SequencedDeadLetterQueue<UnitOfWork & TypeormUnitOfWork> {
  const table = TYPEORM_DEAD_LETTER_TABLE
  const maxSequences = options.maxSequences ?? 1024
  const maxSequenceSize = options.maxSequenceSize ?? 1024
  const claimDurationMs = options.claimDurationMs ?? 30000

  /**
   * The writer for one call: the unit of work's adapter transaction when it
   * has one, else the plain handle. Passing no unit of work — lifecycle and
   * admin paths — writes outside any transaction.
   */
  function getManager(uow?: UnitOfWork): any {
    return activeTypeormTransaction(uow) ?? manager
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

  async function sequenceRows(m: any, group: string, seqId: string): Promise<any[]> {
    return m.query(
      `SELECT dead_letter_id, processing_group, sequence_identifier, sequence_index,
              message, cause_type, cause_message, diagnostics,
              enqueued_at, last_touched, processing_started
       FROM ${table}
       WHERE processing_group = $1 AND sequence_identifier = $2
       ORDER BY sequence_index ASC`,
      [group, seqId],
    )
  }

  async function distinctSequences(m: any, group: string): Promise<string[]> {
    const rows = await m.query(
      `SELECT DISTINCT sequence_identifier FROM ${table} WHERE processing_group = $1`,
      [group],
    )
    return rows.map((r: any) => r.sequence_identifier)
  }

  async function insertLetter(
    m: any,
    group: string,
    letter: DeadLetter,
    sequenceIndex: number,
    deadLetterId: string,
  ): Promise<void> {
    const { [DL_ID]: _omit, ...diagnostics } = letter.diagnostics as Record<string, unknown>
    await m.query(
      `INSERT INTO ${table}
         (dead_letter_id, processing_group, sequence_identifier, sequence_index,
          message, cause_type, cause_message, diagnostics,
          enqueued_at, last_touched, processing_started)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL)`,
      [
        deadLetterId,
        group,
        letter.sequenceIdentifier,
        sequenceIndex,
        JSON.stringify(letter.message),
        letter.cause.name,
        letter.cause.message,
        JSON.stringify(diagnostics),
        String(letter.enqueuedAt),
        String(letter.lastTouched),
      ],
    )
  }

  const queue: SequencedDeadLetterQueue = {
    async enqueue(group, letter, uow) {
      const m = getManager(uow)
      const existing = await sequenceRows(m, group, letter.sequenceIdentifier)
      if (existing.length === 0) {
        if ((await distinctSequences(m, group)).length >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
      } else if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx =
        existing.length === 0 ? 0 : Number(existing[existing.length - 1].sequence_index) + 1
      await insertLetter(m, group, letter, idx, newId(group))
    },

    async enqueueIfPresent(group, sequenceIdentifier, letterSupplier, uow) {
      const m = getManager(uow)
      const existing = await sequenceRows(m, group, sequenceIdentifier)
      if (existing.length === 0) return false
      if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const idx = Number(existing[existing.length - 1].sequence_index) + 1
      await insertLetter(m, group, letterSupplier(), idx, newId(group))
      return true
    },

    async evict(group, _sequenceIdentifier, letter, uow) {
      const m = getManager(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await m.query(`DELETE FROM ${table} WHERE processing_group = $1 AND dead_letter_id = $2`, [
        group,
        id,
      ])
    },

    async requeue(group, letter, update, uow) {
      const m = getManager(uow)
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiag } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics ? { ...baseDiag, ...update.diagnostics } : baseDiag
      await m.query(
        `UPDATE ${table}
         SET cause_type = $3, cause_message = $4, diagnostics = $5, last_touched = $6
         WHERE processing_group = $1 AND dead_letter_id = $2`,
        [group, id, cause.name, cause.message, JSON.stringify(diagnostics), String(Date.now())],
      )
    },

    async contains(group, sequenceIdentifier, uow) {
      const m = getManager(uow)
      const rows = await m.query(
        `SELECT dead_letter_id FROM ${table}
         WHERE processing_group = $1 AND sequence_identifier = $2
         LIMIT 1`,
        [group, sequenceIdentifier],
      )
      return rows.length > 0
    },

    async deadLetterSequence(group, sequenceIdentifier, uow) {
      const m = getManager(uow)
      return (await sequenceRows(m, group, sequenceIdentifier)).map(rowToLetter)
    },

    async sequenceIdentifiers(group, uow) {
      return distinctSequences(getManager(uow), group)
    },

    async process(group, sequenceFilter, processingTask, uow) {
      const m = getManager(uow)
      const candidates = (await distinctSequences(m, group)).filter(sequenceFilter)
      if (candidates.length === 0) return false

      // Pick the oldest sequence by its head letter's lastTouched, skipping
      // sequences under an unexpired processing lease (multi-node safety).
      const cutoff = Date.now() - claimDurationMs
      let chosen: string | undefined
      let oldest = Infinity
      for (const seqId of candidates) {
        const rows = await sequenceRows(m, group, seqId)
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
      const headRows = await sequenceRows(m, group, chosen)
      await m.query(
        `UPDATE ${table} SET processing_started = $3
         WHERE processing_group = $1 AND dead_letter_id = $2`,
        [group, headRows[0].dead_letter_id, String(Date.now())],
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
        const remaining = await sequenceRows(m, group, chosen)
        if (remaining.length > 0 && remaining[0].processing_started != null) {
          await m.query(
            `UPDATE ${table} SET processing_started = NULL
             WHERE processing_group = $1 AND dead_letter_id = $2`,
            [group, remaining[0].dead_letter_id],
          )
        }
      }
    },

    async size(group, uow) {
      const m = getManager(uow)
      const rows = await m.query(
        `SELECT dead_letter_id FROM ${table} WHERE processing_group = $1`,
        [group],
      )
      return rows.length
    },

    async amountOfSequences(group, uow) {
      return (await distinctSequences(getManager(uow), group)).length
    },

    async clear(group, uow) {
      const m = getManager(uow)
      await m.query(`DELETE FROM ${table} WHERE processing_group = $1`, [group])
    },

    async isFull(group, sequenceIdentifier, uow) {
      const m = getManager(uow)
      const rows = await sequenceRows(m, group, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(m, group)).length >= maxSequences
    },
  }

  return queue
}
