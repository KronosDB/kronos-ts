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
 * {@link sharedPostgresTransaction}, so enqueue/evict/requeue commit in the SAME
 * postgres transaction as the token update. A crash cannot advance a
 * processor's token while losing the letter it parked.
 *
 * The one exception is the `process()` lease. Claiming a lane is its own short
 * committed statement on the pool, so other replayers see it at once and the
 * replay's transaction takes no xid before its handlers have run.
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
import { sharedPostgresTransaction } from "./postgres-transaction.js"

/** Tuning only — everything required is a positional argument. */
export type PostgresDeadLetterQueueOptions = {
  /** Maximum number of sequences. Default: 1024 (Axon parity). */
  readonly maxSequences?: number
  /** Maximum letters per sequence. Default: 1024 (Axon parity). */
  readonly maxSequenceSize?: number
  /**
   * Lease duration for in-flight processing, ms. Default: 30000 (Axon parity).
   *
   * Must exceed the longest replay a handler can take. A lane stays claimed
   * for this long; past it another replayer may claim the same lane and replay
   * its letters again, while the first is still running.
   */
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
): SequencedDeadLetterQueue<UnitOfWork> {
  const table = pg.tables.deadLetters
  const maxSequences = options.maxSequences ?? 1024
  const maxSequenceSize = options.maxSequenceSize ?? 1024
  const claimDurationMs = options.claimDurationMs ?? 30000

  /** The writer for one call: the unit of work's transaction, else the pool. */
  async function sql(uow?: UnitOfWork): Promise<SqlHandle> {
    // OPENS the transaction if the unit of work is a postgres one that nobody
    // has written through yet. The postgres transaction is LAZY — begun by the
    // first writer — and in a batch whose handler only read, or wrote through
    // another client, this store IS the first writer. Observing instead of
    // opening threw on every such batch.
    const tx = await sharedPostgresTransaction(uow)
    if (tx !== undefined) return tx
    // NO SILENT FALLBACK. A dead-letter write that lands outside the batch's
    // transaction is the failure this store exists to avoid: it commits on its
    // own, a crash lands between it and the projection it accounts for, and the
    // read model is permanently wrong with nothing to read as the cause. A
    // handler's accessor may fall back — whether a seam is transactional is a
    // deployment decision — but this one may not.
    if (uow !== undefined) {
      throw new Error(
        "@kronos-ts/postgres: this unit of work carries no postgres transaction, so the " +
          "dead-letter write would commit outside the batch it accounts for. Build the " +
          "processor's unitOfWork with `postgresUnitOfWork(next, pg)`.",
      )
    }
    // No unit of work at all — lifecycle and admin paths, which are honestly
    // outside any transaction.
    return pg
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

  /** The first letter of every lane in a group — what age and lease are read from. */
  async function laneHeads(group: string): Promise<LetterRow[]> {
    return pg.query<LetterRow>(
      `SELECT DISTINCT ON (sequence_identifier) ${COLUMNS} FROM ${table}
        WHERE processing_group = $1
        ORDER BY sequence_identifier, sequence_index ASC`,
      [group],
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
      const handle = await sql(uow)
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
      const handle = await sql(uow)
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
      await (await sql(uow)).query(
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
      await (await sql(uow)).query(
        `UPDATE ${table}
            SET cause_type = $3, cause_message = $4, diagnostics = $5, last_touched = $6
          WHERE processing_group = $1 AND dead_letter_id = $2`,
        [group, id, cause.name, cause.message, JSON.stringify(diagnostics), String(Date.now())],
      )
    },

    async contains(group, sequenceIdentifier, uow) {
      const rows = await (await sql(uow)).query(
        `SELECT 1 FROM ${table}
          WHERE processing_group = $1 AND sequence_identifier = $2 LIMIT 1`,
        [group, sequenceIdentifier],
      )
      return rows.length > 0
    },

    async deadLetterSequence(group, sequenceIdentifier, uow) {
      return (await sequenceRows(await sql(uow), group, sequenceIdentifier)).map(rowToLetter)
    },

    async sequenceIdentifiers(group, uow) {
      return distinctSequences(await sql(uow), group)
    },

    async process(group, sequenceFilter, processingTask, uow) {
      // CANDIDATE SELECTION AND THE CLAIM RUN ON THE POOL, never on the unit of
      // work's transaction. A lease written through the replay's own
      // transaction is invisible to every other replayer until that
      // transaction ends — and is cleared before it does — so it would
      // protect nothing; and the write would give the transaction an xid
      // before the handlers run, which holds back the event store's gap-free
      // tail (`transaction_id < pg_snapshot_xmin(...)`) for every streaming
      // processor until the replay is over.
      const now = Date.now()
      const lease = String(now)
      const cutoff = now - claimDurationMs

      // The filter sees each lane's head letter as read here. It is called
      // again below, on the head as it is once the lane is claimed.
      const candidates = (await laneHeads(group))
        .filter((head) => head.processing_started == null || Number(head.processing_started) <= cutoff)
        .filter((head) => sequenceFilter(head.sequence_identifier, rowToLetter(head)))
        // The oldest lane first, by its head letter's lastTouched.
        .sort((a, b) => Number(a.last_touched) - Number(b.last_touched))

      /**
       * Hand a lane back. Only while the lease is still OURS: if it expired and
       * another replayer took the lane, its lease value differs and this
       * matches nothing. Best-effort by design — the caller is already on a
       * failure path or has no use for the lane, and an unreleased lease merely
       * expires after `claimDurationMs`.
       */
      async function releaseLane(id: string): Promise<void> {
        try {
          await pg.query(
            `UPDATE ${table} SET processing_started = NULL
              WHERE processing_group = $1 AND dead_letter_id = $2 AND processing_started = $3`,
            [group, id, lease],
          )
        } catch {
          // See above: the lease expires on its own.
        }
      }

      let headId: string | undefined
      let chosen: string | undefined
      let rows: LetterRow[] = []
      for (const candidate of candidates) {
        // One statement, atomic: it takes the lane only if nobody holds a live
        // lease, and SKIP LOCKED means it never waits on a row another
        // statement has. Zero rows is "someone else has this lane" — try the
        // next one. The selection above can be stale by the time this runs;
        // this is the check that counts.
        const claimed = await pg.query<{ dead_letter_id: string }>(
          `UPDATE ${table} SET processing_started = $3
            WHERE dead_letter_id = (
              SELECT dead_letter_id FROM ${table}
               WHERE processing_group = $1 AND dead_letter_id = $2
                 AND (processing_started IS NULL OR processing_started::bigint <= $4::bigint)
                 FOR UPDATE SKIP LOCKED)
            RETURNING dead_letter_id`,
          [group, candidate.dead_letter_id, lease, String(cutoff)],
        )
        if (claimed.length === 0) continue

        // The lane's rows are read AFTER the claim. Read before it, a replayer
        // that lost the race would hold a stale copy of letters the winner is
        // about to evict, and replay them again.
        let fresh: LetterRow[]
        let passes: boolean
        try {
          fresh = await sequenceRows(pg, group, candidate.sequence_identifier)
          // The selection's head can be stale: another replayer may have
          // requeued the letter, with new diagnostics, between that read and
          // this claim. Judge the head as it is now.
          passes =
            fresh[0] !== undefined &&
            sequenceFilter(candidate.sequence_identifier, rowToLetter(fresh[0]))
        } catch (err) {
          await releaseLane(candidate.dead_letter_id)
          throw err
        }
        if (!passes) {
          // Not ours to replay after all. Hand the lane back — on the pool, so
          // the next replayer sees it at once — and take the next candidate.
          await releaseLane(candidate.dead_letter_id)
          continue
        }
        headId = candidate.dead_letter_id
        chosen = candidate.sequence_identifier
        rows = fresh
        break
      }
      if (headId === undefined || chosen === undefined) return false
      const claimedId = headId
      const release = () => releaseLane(claimedId)

      let handle: SqlHandle
      try {
        // Opens the replay transaction (if the unit of work carries one). Opening
        // is not writing: it assigns no xid until the first write below.
        handle = await sql(uow)
      } catch (err) {
        await release()
        throw err
      }

      // A unit of work that fails — a handler's write, the deferred writes
      // below, the commit itself — is rolled back, and THEN this runs. It has
      // to run after the rollback: the replay transaction can hold row locks
      // on this lane, and a release that waited on them from inside `process`
      // would wait on a rollback that only happens once `process` returns. The
      // transaction's own rollback handler was registered when it opened just
      // above, so it runs first.
      uow?.onError(release)

      try {
        // THE REPLAY TRANSACTION WRITES NOTHING UNTIL EVERY HANDLER HAS RUN.
        // The walk only collects what it will do. A write issued between
        // handlers would give the transaction an xid while a handler is still
        // running — see the claim above for what that costs — so the evictions
        // and the requeue are applied together once the walk is over, through
        // the unit of work, and commit or roll back with the handlers' own
        // writes.
        const evicted: string[] = []
        let failed: { letter: DeadLetter; decision: EnqueueDecision } | undefined
        for (const row of rows) {
          const letter = rowToLetter(row)
          const decision: EnqueueDecision = await processingTask(letter)
          if (decision.shouldEnqueue) {
            failed = { letter, decision }
            break
          }
          evicted.push(row.dead_letter_id)
        }

        if (evicted.length > 0) {
          await handle.query(
            `DELETE FROM ${table} WHERE processing_group = $1 AND dead_letter_id = ANY($2::text[])`,
            [group, evicted],
          )
        }
        if (failed !== undefined) {
          await queue.requeue(
            group,
            failed.letter,
            { cause: failed.decision.cause, diagnostics: failed.decision.diagnostics },
            uow,
          )
        }
        // A head that survives the pass (it was the letter that failed) is
        // released in the same transaction. An evicted head took its lease with it.
        if (evicted.length === 0) {
          await handle.query(
            `UPDATE ${table} SET processing_started = NULL
              WHERE processing_group = $1 AND dead_letter_id = $2 AND processing_started = $3`,
            [group, headId, lease],
          )
        }
        return true
      } catch (err) {
        // Without a unit of work there is no rollback to wait for, and every
        // write above already committed on its own: release now. With one,
        // `onError` above does it.
        if (uow === undefined) await release()
        throw err
      }
    },

    async size(group, uow) {
      const rows = await (await sql(uow)).query<{ count: string | number }>(
        `SELECT count(*)::bigint AS count FROM ${table} WHERE processing_group = $1`,
        [group],
      )
      return Number(rows[0]?.count ?? 0)
    },

    async amountOfSequences(group, uow) {
      return (await distinctSequences(await sql(uow), group)).length
    },

    async clear(group, uow) {
      await (await sql(uow)).query(`DELETE FROM ${table} WHERE processing_group = $1`, [group])
    },

    async isFull(group, sequenceIdentifier, uow) {
      const handle = await sql(uow)
      const rows = await sequenceRows(handle, group, sequenceIdentifier)
      if (rows.length > 0) return rows.length >= maxSequenceSize
      return (await distinctSequences(handle, group)).length >= maxSequences
    },
  }

  return queue
}
