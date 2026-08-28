/**
 * postgresDeadLetterQueue — a {@link SequencedDeadLetterQueue} over raw SQL.
 * No ORM required.
 *
 * The rows are the SAME rows every other persistence family writes
 * (`kronos_dead_letters`, see `./schema.js`). Per-sequence FIFO order is held by
 * a monotonic `sequence_index`; a `processing_started` lease column makes
 * `process()` safe across multiple nodes.
 *
 * PRINCIPLE: like `postgresTokenStore`, every write goes through
 * {@link activePostgresTransaction}, so enqueue/evict/requeue commit in the SAME
 * postgres transaction as the token update. A crash cannot advance a
 * processor's token while losing the letter it parked.
 *
 * The table is shared across processors and partitioned by `processingGroup`,
 * which every method takes as its FIRST argument — the same way a token store
 * takes `processorName`. One queue object is one table, and which partition a
 * call touches is a property of the CALLER, not of the constructor.
 */

import type {
  DeadLetter,
  EnqueueDecision,
  SequencedDeadLetterQueue,
  UnitOfWork,
} from "@kronos-ts/core"
import { DeadLetterQueueOverflowError } from "@kronos-ts/core"
import type { QueryRow } from "./adapter.js"
import type { PostgresResource } from "./postgres-pool.js"
import { activePostgresTransaction } from "./postgres-transaction.js"
import type { PostgresUnitOfWork } from "./postgres-transaction.js"

/** Tuning only — everything required is a positional argument. */
export type PostgresDeadLetterQueueOptions = {
  /** Maximum number of sequences. Default: 1024 (Axon parity). */
  readonly maxSequences?: number
  /** Maximum letters per sequence. Default: 1024 (Axon parity). */
  readonly maxSequenceSize?: number
  /** Lease duration for in-flight processing, ms. Default: 30000 (Axon parity). */
  readonly claimDurationMs?: number
}

/** Reserved diagnostics key carrying the persistent row id across read → evict/requeue. */
const DL_ID = "__dlqId"

let idCounter = 0
function newId(group: string): string {
  // Unique within the table: time + per-process counter + group.
  idCounter += 1
  return `${group}:${Date.now()}:${idCounter}`
}

/** The one operation both a pool and a transaction answer. */
type SqlHandle = {
  query<R extends QueryRow = QueryRow>(sql: string, params?: unknown[]): Promise<R[]>
}

type LetterRow = QueryRow & {
  dead_letter_id: string
  sequence_identifier: string
  sequence_index: number | string
  message: string
  cause_type: string | null
  cause_message: string | null
  diagnostics: string
  enqueued_at: string
  last_touched: string
  processing_started: string | null
}

const COLUMNS =
  "dead_letter_id, sequence_identifier, sequence_index, message, cause_type, cause_message, " +
  "diagnostics, enqueued_at, last_touched, processing_started"

export function postgresDeadLetterQueue(
  pg: PostgresResource,
  options: PostgresDeadLetterQueueOptions = {},
): SequencedDeadLetterQueue<UnitOfWork & PostgresUnitOfWork> {
  const table = pg.tables.deadLetters
  const maxSequences = options.maxSequences ?? 1024
  const maxSequenceSize = options.maxSequenceSize ?? 1024
  const claimDurationMs = options.claimDurationMs ?? 30000

  /** The writer for one call: the unit of work's transaction, else the pool. */
  function sql(uow?: UnitOfWork): SqlHandle {
    return activePostgresTransaction(uow) ?? pg
  }

  function rowToLetter(row: LetterRow): DeadLetter {
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

  function insertParams(
    group: string,
    letter: DeadLetter,
    sequenceIndex: number,
    deadLetterId: string,
  ): unknown[] {
    const { [DL_ID]: _omit, ...diagnostics } = letter.diagnostics as Record<string, unknown>
    return [
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
    ]
  }

  const INSERT = `INSERT INTO ${table}
      (dead_letter_id, processing_group, sequence_identifier, sequence_index, message,
       cause_type, cause_message, diagnostics, enqueued_at, last_touched, processing_started)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL)`

  async function sequenceRows(
    handle: SqlHandle,
    group: string,
    seqId: string,
  ): Promise<LetterRow[]> {
    return handle.query<LetterRow>(
      `SELECT ${COLUMNS} FROM ${table}
        WHERE processing_group = $1 AND sequence_identifier = $2
        ORDER BY sequence_index ASC`,
      [group, seqId],
    )
  }

  async function distinctSequences(handle: SqlHandle, group: string): Promise<string[]> {
    const rows = await handle.query<{ sequence_identifier: string }>(
      `SELECT DISTINCT sequence_identifier FROM ${table} WHERE processing_group = $1`,
      [group],
    )
    return rows.map((r) => r.sequence_identifier)
  }

  const queue: SequencedDeadLetterQueue = {
    async enqueue(group, letter, uow) {
      const handle = sql(uow)
      const existing = await sequenceRows(handle, group, letter.sequenceIdentifier)
      if (existing.length === 0) {
        if ((await distinctSequences(handle, group)).length >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
      } else if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const index =
        existing.length === 0 ? 0 : Number(existing[existing.length - 1]!.sequence_index) + 1
      await handle.query(INSERT, insertParams(group, letter, index, newId(group)))
    },

    async enqueueIfPresent(group, sequenceIdentifier, letterSupplier, uow) {
      const handle = sql(uow)
      const existing = await sequenceRows(handle, group, sequenceIdentifier)
      if (existing.length === 0) return false
      if (existing.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const index = Number(existing[existing.length - 1]!.sequence_index) + 1
      await handle.query(INSERT, insertParams(group, letterSupplier(), index, newId(group)))
      return true
    },

    async evict(group, _sequenceIdentifier, letter, uow) {
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      await sql(uow).query(
        `DELETE FROM ${table} WHERE processing_group = $1 AND dead_letter_id = $2`,
        [group, id],
      )
    },

    async requeue(group, letter, update, uow) {
      const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
      if (typeof id !== "string") return
      const { [DL_ID]: _omit, ...baseDiagnostics } = letter.diagnostics as Record<string, unknown>
      const cause = update?.cause ?? letter.cause
      const diagnostics = update?.diagnostics
        ? { ...baseDiagnostics, ...update.diagnostics }
        : baseDiagnostics
      await sql(uow).query(
        `UPDATE ${table}
            SET cause_type = $3, cause_message = $4, diagnostics = $5, last_touched = $6
          WHERE processing_group = $1 AND dead_letter_id = $2`,
        [group, id, cause.name, cause.message, JSON.stringify(diagnostics), String(Date.now())],
      )
    },

    async contains(group, sequenceIdentifier, uow) {
      const rows = await sql(uow).query(
        `SELECT 1 FROM ${table}
          WHERE processing_group = $1 AND sequence_identifier = $2 LIMIT 1`,
        [group, sequenceIdentifier],
      )
      return rows.length > 0
    },

    async deadLetterSequence(group, sequenceIdentifier, uow) {
      return (await sequenceRows(sql(uow), group, sequenceIdentifier)).map(rowToLetter)
    },

    async sequenceIdentifiers(group, uow) {
      return distinctSequences(sql(uow), group)
    },

    async process(group, sequenceFilter, processingTask, uow) {
      const handle = sql(uow)
      const candidates = (await distinctSequences(handle, group)).filter(sequenceFilter)
      if (candidates.length === 0) return false

      // Pick the oldest sequence by its head letter's lastTouched, skipping
      // sequences under an unexpired processing lease (multi-node safety).
      const cutoff = Date.now() - claimDurationMs
      let chosen: string | undefined
      let oldest = Number.POSITIVE_INFINITY
      for (const seqId of candidates) {
        const rows = await sequenceRows(handle, group, seqId)
        const head = rows[0]
        if (head === undefined) continue
        const leased = head.processing_started != null && Number(head.processing_started) > cutoff
        if (leased) continue
        if (Number(head.last_touched) < oldest) {
          oldest = Number(head.last_touched)
          chosen = seqId
        }
      }
      if (chosen === undefined) return false

      // Claim the sequence head's lease for the duration of this pass.
      const headRows = await sequenceRows(handle, group, chosen)
      await handle.query(
        `UPDATE ${table} SET processing_started = $3
          WHERE processing_group = $1 AND dead_letter_id = $2`,
        [group, headRows[0]!.dead_letter_id, String(Date.now())],
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
        const remaining = await sequenceRows(handle, group, chosen)
        const head = remaining[0]
        if (head !== undefined && head.processing_started != null) {
          await handle.query(
            `UPDATE ${table} SET processing_started = NULL
              WHERE processing_group = $1 AND dead_letter_id = $2`,
            [group, head.dead_letter_id],
          )
        }
      }
    },

    async size(group, uow) {
      const rows = await sql(uow).query<{ count: string | number }>(
        `SELECT count(*)::bigint AS count FROM ${table} WHERE processing_group = $1`,
        [group],
      )
      return Number(rows[0]?.count ?? 0)
    },

    async amountOfSequences(group, uow) {
      return (await distinctSequences(sql(uow), group)).length
    },

    async clear(group, uow) {
      await sql(uow).query(`DELETE FROM ${table} WHERE processing_group = $1`, [group])
    },

    async isFull(group, sequenceIdentifier, uow) {
      const handle = sql(uow)
      const rows = await sequenceRows(handle, group, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(handle, group)).length >= maxSequences
    },
  }

  return queue
}
