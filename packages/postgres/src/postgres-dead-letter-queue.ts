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
 * The exception is the `process()` lease. Claiming a letter is its own short
 * committed statement on the pool, so other replayers see it at once. Each
 * letter is replayed in a unit of work of its own: the handler writes, the
 * letter's eviction and the commit are one transaction, which takes no xid
 * until the handler has finished. A letter that fails rolls its transaction
 * back and is requeued on the pool.
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
   * Must exceed the longest replay of ONE letter, handlers and commit
   * included — not of a whole lane. `process()` claims each letter just before
   * the letter ahead of it commits its eviction, so every letter is replayed
   * under a lease taken moments earlier. A lease older than this is treated as
   * abandoned: another replayer may claim the letter and replay it again while
   * the first replay is still running.
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

/**
 * Thrown inside a letter's unit of work when its replay failed, to make the
 * unit of work roll back. Private: it carries the decision out to `process`,
 * which catches it, and it never reaches a caller.
 */
class LetterFailed extends Error {
  constructor(readonly decision: EnqueueDecision) {
    super("dead letter replay failed")
    this.name = "LetterFailed"
  }
}

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

  /** How many letters a lane holds and the index of its last, without reading any of them. */
  async function laneStats(
    handle: SqlHandle,
    group: string,
    seqId: string,
  ): Promise<{ size: number; lastIndex: number | undefined }> {
    const rows = await handle.query<{ size: string | number; last_index: string | number | null }>(
      `SELECT count(*)::bigint AS size, max(sequence_index) AS last_index FROM ${table}
        WHERE processing_group = $1 AND sequence_identifier = $2`,
      [group, seqId],
    )
    const row = rows[0]
    return {
      size: Number(row?.size ?? 0),
      lastIndex: row?.last_index == null ? undefined : Number(row.last_index),
    }
  }

  /** How many lanes a group holds. */
  async function laneCount(handle: SqlHandle, group: string): Promise<number> {
    const rows = await handle.query<{ count: string | number }>(
      `SELECT count(DISTINCT sequence_identifier)::bigint AS count FROM ${table}
        WHERE processing_group = $1`,
      [group],
    )
    return Number(rows[0]?.count ?? 0)
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

  /**
   * Rewrite a parked letter's cause and diagnostics and stamp `lastTouched`.
   * With `lease`, the letter's lease is cleared in the same statement — but
   * only while it is still that value, so a lease another replayer has taken
   * since is left alone.
   */
  async function requeueOn(
    handle: SqlHandle,
    group: string,
    letter: DeadLetter,
    update?: Partial<Pick<DeadLetter, "cause" | "diagnostics">>,
    lease?: string,
  ): Promise<void> {
    const id = (letter.diagnostics as Record<string, unknown>)[DL_ID]
    if (typeof id !== "string") return
    const { [DL_ID]: _omit, ...baseDiagnostics } = letter.diagnostics as Record<string, unknown>
    const cause = update?.cause ?? letter.cause
    const diagnostics = update?.diagnostics
      ? { ...baseDiagnostics, ...update.diagnostics }
      : baseDiagnostics
    await handle.query(
      `UPDATE ${table}
          SET cause_type = $3, cause_message = $4, diagnostics = $5, last_touched = $6,
              processing_started = CASE WHEN processing_started = $7 THEN NULL ELSE processing_started END
        WHERE processing_group = $1 AND dead_letter_id = $2`,
      [
        group,
        id,
        cause.name,
        cause.message,
        JSON.stringify(diagnostics),
        String(Date.now()),
        lease ?? null,
      ],
    )
  }

  const queue: SequencedDeadLetterQueue = {
    async enqueue(group, letter, uow) {
      const handle = await sql(uow)
      const lane = await laneStats(handle, group, letter.sequenceIdentifier)
      if (lane.size === 0) {
        if ((await laneCount(handle, group)) >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
      } else if (lane.size >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const index = lane.lastIndex === undefined ? 0 : lane.lastIndex + 1
      await handle.query(INSERT, insertParams(group, letter, index, newId(group)))
    },

    async enqueueIfPresent(group, sequenceIdentifier, letterSupplier, uow) {
      const handle = await sql(uow)
      const lane = await laneStats(handle, group, sequenceIdentifier)
      if (lane.size === 0) return false
      if (lane.size >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      const index = (lane.lastIndex ?? -1) + 1
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
      await requeueOn(await sql(uow), group, letter, update)
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

    async process(group, sequenceFilter, processingTask, unitOfWork) {
      // CANDIDATE SELECTION AND EVERY CLAIM RUN ON THE POOL, never on a unit of
      // work's transaction. A lease written through the replay's own
      // transaction is invisible to every other replayer until that
      // transaction ends, so it would protect nothing; and the write would give
      // the transaction an xid before the handler has run, which holds back the
      // event store's gap-free tail (`transaction_id < pg_snapshot_xmin(...)`)
      // for every streaming processor until the replay is over.
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

      /** A letter's lease, as the one this call took. */
      type Held = { readonly id: string; readonly lease: string }

      /**
       * Take a letter's lease: one statement, atomic. It takes the letter only
       * if nobody holds a live lease on it, and SKIP LOCKED means it never
       * waits on a row another statement has. False is "someone else has it,
       * or it is gone".
       */
      async function claim(id: string, leaseValue: string): Promise<boolean> {
        const claimed = await pg.query<{ dead_letter_id: string }>(
          `UPDATE ${table} SET processing_started = $3
            WHERE dead_letter_id = (
              SELECT dead_letter_id FROM ${table}
               WHERE processing_group = $1 AND dead_letter_id = $2
                 AND (processing_started IS NULL OR processing_started::bigint <= $4::bigint)
                 FOR UPDATE SKIP LOCKED)
            RETURNING dead_letter_id`,
          [group, id, leaseValue, String(Number(leaseValue) - claimDurationMs)],
        )
        return claimed.length > 0
      }

      /**
       * Hand a letter back. Only while the lease is still OURS: if it expired
       * and another replayer took the letter, its lease value differs and this
       * matches nothing. Best-effort by design — the caller is already on a
       * failure path or has no use for the letter, and an unreleased lease
       * merely expires after `claimDurationMs`.
       */
      async function release(held: Held | undefined): Promise<void> {
        if (held === undefined) return
        try {
          await pg.query(
            `UPDATE ${table} SET processing_started = NULL
              WHERE processing_group = $1 AND dead_letter_id = $2 AND processing_started = $3`,
            [group, held.id, held.lease],
          )
        } catch {
          // See above: the lease expires on its own.
        }
      }

      let first: Held | undefined
      let rows: LetterRow[] = []
      for (const candidate of candidates) {
        // The selection above can be stale by the time this runs; this is the
        // check that counts. Zero rows is "someone else has this lane" — try
        // the next one.
        if (!(await claim(candidate.dead_letter_id, lease))) continue
        const taken: Held = { id: candidate.dead_letter_id, lease }

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
          await release(taken)
          throw err
        }
        if (!passes) {
          // Not ours to replay after all. Hand the lane back — on the pool, so
          // the next replayer sees it at once — and take the next candidate.
          await release(taken)
          continue
        }
        first = taken
        rows = fresh
        break
      }
      if (first === undefined) return false

      // THE WALK: head to tail, one unit of work per letter.
      let held: Held = first
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i]!
        const following = rows[i + 1]
        const letter = rowToLetter(row)
        // The lease taken on the letter behind this one, if any.
        let next = undefined as Held | undefined

        const uow = unitOfWork()
        try {
          await uow.execute(async () => {
            // THE HANDLER RUNS FIRST. Everything this queue writes in the
            // letter's transaction comes after it, so a handler that waits on
            // something slow does so in a transaction that holds no xid from
            // the queue.
            const decision = await processingTask(letter, uow)
            // A failing letter must leave nothing of its replay behind. The
            // decision is a return value, so it cannot make the unit of work
            // fail by itself: throwing is what rolls the transaction back, and
            // `process` catches it below.
            if (decision.shouldEnqueue) throw new LetterFailed(decision)

            // HAND-OVER-HAND. Take the next letter's lease before this one's
            // eviction commits, so the lane is never unclaimed between the two
            // and each lease only has to outlast one letter's replay.
            if (following !== undefined) {
              const taken: Held = { id: following.dead_letter_id, lease: String(Date.now()) }
              if (await claim(taken.id, taken.lease)) next = taken
            }

            // The eviction goes through THIS unit of work, so it commits with
            // the handler's writes or not at all.
            await queue.evict(group, row.sequence_identifier, letter, uow)
          })
        } catch (err) {
          // `execute` has rolled the transaction back by the time it rejects,
          // so the releases below do not wait on rows it held.
          if (err instanceof LetterFailed) {
            try {
              // Outside the rolled-back unit of work, on the pool: the letter
              // keeps its place, takes the new cause and diagnostics, and is
              // handed back in one statement.
              await requeueOn(
                pg,
                group,
                letter,
                { cause: err.decision.cause, diagnostics: err.decision.diagnostics },
                held.lease,
              )
            } catch (requeueError) {
              await release(held)
              throw requeueError
            }
            return true
          }
          await release(held)
          await release(next)
          throw err
        }

        // The letter is evicted, and its lease went with its row. The lane is
        // now held through the next letter's lease — or not at all, if that
        // could not be taken. Then another replayer has the rest.
        if (next === undefined) return true
        held = next
      }
      return true
    },

    async size(group, uow) {
      const rows = await (await sql(uow)).query<{ count: string | number }>(
        `SELECT count(*)::bigint AS count FROM ${table} WHERE processing_group = $1`,
        [group],
      )
      return Number(rows[0]?.count ?? 0)
    },

    async amountOfSequences(group, uow) {
      return laneCount(await sql(uow), group)
    },

    async clear(group, uow) {
      await (await sql(uow)).query(`DELETE FROM ${table} WHERE processing_group = $1`, [group])
    },

    async isFull(group, sequenceIdentifier, uow) {
      const handle = await sql(uow)
      const lane = await laneStats(handle, group, sequenceIdentifier)
      if (lane.size > 0) return lane.size >= maxSequenceSize
      return (await laneCount(handle, group)) >= maxSequences
    },
  }

  return queue
}
