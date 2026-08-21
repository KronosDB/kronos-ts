/**
 * postgresEventStore — full EventStorageEngine + EventBus implementation.
 *
 * Plan 12-04 delivered: source, appendEvents, append.
 * Plan 12-05 adds: open (gap-free tailing via xid8 + pg_snapshot_xmin),
 *   getHeadPosition, firstToken, latestToken, publish, subscribe.
 *
 * Append path:
 *   1. open transaction at READ COMMITTED
 *   2. acquireWriteLocks for the query's tags (not event types) so that
 *      two writers on the SAME tag serialize, while disjoint-tag
 *      writers run in parallel
 *   3. Conflict check: SELECT count(*) WHERE sequence_position > marker AND query
 *   4. If conflict count > 0 → throw AppendConditionError (code KR001)
 *   5. INSERT events returning sequence_position for the ConsistencyMarker
 *   6. commit() → COMMIT; afterCommit() → marker; rollback() → fire-and-forget
 *      ROLLBACK (synchronous void per the framework contract)
 *
 * Streaming path (open):
 *   - Watermark query: (transaction_id, sequence_position) > ($xid, $pos)
 *     AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
 *   - Wake-up via LISTEN/NOTIFY on `kronos_events_${tables.events}` channel
 *   - Fallback to 250ms polling if LISTEN is not supported
 *
 * The conflict check + INSERT run as direct parameterised SQL inside the
 * append transaction (see checkAndInsert), not via a stored procedure.
 */

import type {
  EventStorageEngine,
  AppendTransaction,
  EventStore,
} from "@kronos-ts/core"
import { markerAt } from "@kronos-ts/core"
import type { ConsistencyMarker } from "@kronos-ts/core"
import type { SourcingCondition } from "@kronos-ts/core"
import type { SourcingResult } from "@kronos-ts/core"
import type { AppendCondition } from "@kronos-ts/core"
import type {
  EventMessage,
  EventCriteria,
  MessageStream,
  SequencedEvent,
  StreamingCondition,
  TrackingToken,
  UnitOfWork,
} from "@kronos-ts/core"
import {
  messageStream,
  globalSequenceToken,
  gapAwareToken,
  isGapAwareToken,
  unwrapToken,
  compileQuery,
  FIRST_TOKEN,
} from "@kronos-ts/core"
import { qualifiedNameToString, qualifiedNameFromString } from "@kronos-ts/core"
import type { PostgresAdapter, PostgresAdapterTransaction } from "./adapter.js"
import { IsolationLevel } from "./adapter.js"
import { acquireWriteLocks, type LockTarget } from "./advisory-locks.js"
import { buildCriteriaWhere, encodeTag } from "./criteria-sql.js"
import { AppendConditionError, isDcbViolation, KRONOS_DCB_VIOLATION_SQLSTATE } from "./errors.js"
import type { PostgresResource } from "./postgres-pool.js"
import { sharedPostgresTransaction } from "./postgres-transaction.js"
import { decodeEvent, type EventRow, EVENT_COLUMNS } from "./event-row.js"

// Minimal TagResolver structural shape — the real slot is declared in the
// core; we accept anything compatible. Serializer uses the canonical type.
export type TagResolver = (event: EventMessage) => ReadonlyArray<{ key: string; value: string }>

export type PostgresEventStoreConfig = {
  readonly tagResolver: TagResolver
}

/**
 * The DCB event store — and NOTHING BUT. `pg` carries both the client and the
 * table names, so the only thing left to say is how tags are read off an event.
 *
 * IT HAS NEVER HEARD OF SNAPSHOTS. There is no `serializer` in its config any
 * more and no snapshot branch in its `source`: the base contract is complete
 * for event sourcing, and a host that wants a cache over the fold WRAPS this —
 * `postgresSnapshottingEventStore(postgresEventStore(pg, { tagResolver }), pg,
 * { serializer })` — which is the one place the snapshots table is mentioned
 * and the one place a serializer is needed.
 */
export function postgresEventStore(
  pg: PostgresResource,
  config: PostgresEventStoreConfig,
): EventStore {
  const adapter: PostgresAdapter = pg
  const { tagResolver } = config
  const tables = pg.tables

  // Push-based subscriber registry (EventBus.subscribe contract)
  const eventSubscribers = new Set<(events: ReadonlyArray<EventMessage>) => Promise<void>>()

  // LISTEN/NOTIFY channel name for wake-up of tailing streams (D-12.14)
  const notifyChannel = `kronos_events_${tables.events}`

  function eventTypeOf(e: EventMessage): string {
    return qualifiedNameToString(e.name)
  }

  /**
   * Extract lock targets from the append condition's query — the writer locks on
   * what it is READING (the query's tags), not just what it is writing. This
   * ensures two writers on the same tag serialize (one blocks until the other
   * commits), while writers on disjoint tags run in parallel.
   *
   * For `any-tag` or empty criteria, returns an empty array so only the
   * global-intent S-lock is acquired (acquireWriteLocks handles the empty case).
   */
  function lockTargetsForCondition(condition: AppendCondition | undefined): LockTarget[] {
    if (!condition) return []
    return extractCriteriaTags(compileQuery(condition.query)).map((tag) => ({ type: "", tag }))
  }

  function extractCriteriaTags(criteria: EventCriteria): string[] {
    switch (criteria.kind) {
      case "tags":
        return criteria.tags.map((t) => encodeTag(t.key, t.value))
      case "any-tag":
        // any-tag covers all tags — use global-intent only (empty list)
        return []
      case "type-restricted":
        return extractCriteriaTags(criteria.inner)
      case "either":
        return criteria.criteria.flatMap((c) => extractCriteriaTags(c))
    }
  }

  function encodedTagsOf(e: EventMessage): string[] {
    return tagResolver(e).map((t) => encodeTag(t.key, t.value))
  }

  /**
   * Check for DCB conflict and INSERT events, within the caller's transaction.
   * Returns the (position, xid) of the last inserted row.
   *
   * Uses parameterised SQL (no dynamic SQL), so each query is prepared once
   * and has no $N rebinding complexity.
   */
  async function checkAndInsert(
    tx: PostgresAdapterTransaction,
    events: ReadonlyArray<EventMessage>,
    condition: AppendCondition | undefined,
  ): Promise<{ position: bigint; xid: string }> {
    // --- Conflict check ---
    if (condition) {
      const markerPos = condition.marker.position
      const built = buildCriteriaWhere(compileQuery(condition.query), 2) // $1 = markerPos
      const sql = `SELECT count(*)::bigint AS cnt FROM ${tables.events}
                   WHERE sequence_position > $1 AND (${built.where})`
      const rows = await tx.query<{ cnt: string | number }>(sql, [markerPos, ...built.params])
      const cnt = BigInt(rows[0]?.cnt ?? 0)
      if (cnt > 0n) {
        // Throw with the KR001 code so isDcbViolation() can identify it
        const err = new AppendConditionError(
          `Append condition violated: ${cnt} conflicting event(s) after position ${markerPos}`,
        )
        ;(err as unknown as { code: string }).code = KRONOS_DCB_VIOLATION_SQLSTATE
        throw err
      }
    }

    // --- Insert events ---
    let lastPosition = -1n
    let lastXid = ""

    for (const e of events) {
      const encodedTags = encodedTagsOf(e)
      const type = eventTypeOf(e)
      const payload = e.payload ?? {}
      const metadata = e.metadata ?? {}

      const rows = await tx.query<{ sequence_position: string; transaction_id: string }>(
        `INSERT INTO ${tables.events} (event_id, type, tags, payload, metadata, version, timestamp)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING sequence_position, transaction_id`,
        [
          e.identifier,
          type,
          encodedTags,
          JSON.stringify(payload),
          JSON.stringify(metadata),
          e.version,
          e.timestamp,
        ],
      )
      const row = rows[0]
      if (!row) throw new Error("INSERT returned no rows")
      lastPosition = BigInt(row.sequence_position)
      lastXid = row.transaction_id
    }

    if (lastPosition < 0n) {
      throw new Error("no events were inserted")
    }
    return { position: lastPosition, xid: lastXid }
  }

  /** The plain read: the query, plus the head the marker falls back to. */
  async function sourcePlain(condition: SourcingCondition): Promise<SourcingResult> {
    const start = condition.start ?? 0n
    const built = buildCriteriaWhere(compileQuery(condition.query), 2) // $1 = start
    const sql = `
        SELECT ${EVENT_COLUMNS}
        FROM ${tables.events}
        WHERE sequence_position >= $1 AND (${built.where})
        ORDER BY sequence_position ASC
      `
    const rows = await adapter.query<EventRow>(sql, [start, ...built.params])

    const events: EventMessage[] = rows.map((r) => decodeEvent(r))
    const headRow = await adapter.queryOne<{ head: string | null }>(
      `SELECT MAX(sequence_position)::text AS head FROM ${tables.events}`,
    )
    const head = headRow?.head ? BigInt(headRow.head) : -1n
    const lastPos = rows.length > 0 ? BigInt(rows[rows.length - 1]!.sequence_position) : -1n
    const marker = rows.length > 0 ? markerAt(lastPos) : markerAt(head)
    return { events, marker }
  }

  return {
    // A CONDITION CARRYING A SNAPSHOT KEY IS IGNORED HERE, and that is correct
    // rather than incomplete: an unwrapped store replays in full, which is a
    // slower load and never a wrong one. Serving the key is what the wrapper is
    // for, and the compiler makes sure a state that needs one gets one.
    source: sourcePlain,

    async appendEvents(
      events: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<AppendTransaction> {
      // Two-phase: open tx, acquire locks, run conflict check + INSERT, hold
      // tx open until commit(). We bridge adapter.transaction() (which owns
      // the full lifecycle) with a deferred that the AppendTransaction controls.
      const targets = lockTargetsForCondition(condition)

      let resolveOuter!: (v: { position: bigint; xid: string }) => void
      let rejectOuter!: (e: unknown) => void
      const outer = new Promise<{ position: bigint; xid: string }>((res, rej) => {
        resolveOuter = res
        rejectOuter = rej
      })

      let txReady!: () => void
      const txStaged = new Promise<void>((res) => {
        txReady = res
      })

      let resolveTxControl!: (cmd: "commit" | "rollback") => void
      const txControl = new Promise<"commit" | "rollback">((res) => {
        resolveTxControl = res
      })

      // Kick off the transaction in the background.
      const txPromise = adapter
        .transaction(IsolationLevel.READ_COMMITTED, async (tx) => {
          await acquireWriteLocks(tx, targets)
          let captured: { position: bigint; xid: string }
          try {
            captured = await checkAndInsert(tx, events, condition)
          } catch (err) {
            if (isDcbViolation(err)) {
              // Already an AppendConditionError with KR001 code
              throw err
            }
            if ((err as { code?: string }).code === "23505") {
              throw AppendConditionError.fromConflictCount(0, condition?.marker.position ?? -1n)
            }
            throw err
          }
          txReady()
          const cmd = await txControl
          if (cmd === "rollback") {
            throw new Error("__kronos_rollback__")
          }
          return captured
        })
        .then(
          (v) => resolveOuter(v),
          (e) => {
            if (e instanceof Error && e.message === "__kronos_rollback__") {
              rejectOuter(new Error("rolled back"))
            } else {
              rejectOuter(e)
            }
          },
        )
      void txPromise

      // Wait until the SP has executed so that errors (DCB violation) surface
      // BEFORE the AppendTransaction handle is returned.
      await Promise.race([
        txStaged,
        outer.catch(() => {
          return
        }),
      ])

      // Surface any already-rejected error immediately.
      let alreadyFailed = false
      outer.catch(() => {
        alreadyFailed = true
      })
      await Promise.resolve()
      if (alreadyFailed) {
        await outer
      }

      let committed = false
      const transaction: AppendTransaction = {
        async commit() {
          committed = true
          resolveTxControl("commit")
          await outer
          // Wake up tailing streams + notify push-based subscribers after commit
          await adapter.query(`NOTIFY ${notifyChannel}`)
          for (const sub of eventSubscribers) {
            try { await sub(events) } catch { /* ignore subscriber errors */ }
          }
        },
        async afterCommit() {
          const result = await outer
          return markerAt(result.position)
        },
        rollback() {
          if (committed) return
          resolveTxControl("rollback")
          outer.catch(() => {
            /* swallow — rollback path is expected to reject outer */
          })
        },
      }
      return transaction
    },

    async append(
      events: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
      uow?: UnitOfWork,
    ): Promise<ConsistencyMarker> {
      // Two paths: join the unit of work's tx (writes commit atomically with
      // everything else in it — scheduler inserts, token store, future
      // outbox), opening it lazily on first request; or open our own
      // short-lived tx when no unit of work was handed in. The shared path
      // defers NOTIFY + subscriber dispatch to AFTER_COMMIT because the tx
      // hasn't actually committed yet when checkAndInsert returns.
      const targets = lockTargetsForCondition(condition)
      const shared = await sharedPostgresTransaction(uow)

      const notifyAndFanout = async () => {
        await adapter.query(`NOTIFY ${notifyChannel}`)
        for (const sub of eventSubscribers) {
          try { await sub(events) } catch { /* ignore subscriber errors */ }
        }
      }

      const runAppend = async (tx: PostgresAdapterTransaction): Promise<ConsistencyMarker> => {
        await acquireWriteLocks(tx, targets)
        const captured = await checkAndInsert(tx, events, condition)
        return markerAt(captured.position)
      }

      const translateError = (err: unknown): never => {
        if (isDcbViolation(err)) throw err
        if ((err as { code?: string }).code === "23505") {
          throw AppendConditionError.fromConflictCount(0, condition?.marker.position ?? -1n)
        }
        throw err
      }

      if (shared !== undefined) {
        let marker: ConsistencyMarker
        try {
          marker = await runAppend(shared)
        } catch (err) {
          translateError(err)
        }
        uow!.onAfterCommit(notifyAndFanout)
        return marker!
      }

      let marker: ConsistencyMarker
      try {
        marker = await adapter.transaction(IsolationLevel.READ_COMMITTED, runAppend)
      } catch (err) {
        translateError(err)
      }
      await notifyAndFanout()
      return marker!
    },

    async getHeadPosition(): Promise<bigint> {
      const row = await adapter.queryOne<{ head: string | null }>(
        `SELECT COALESCE(MAX(sequence_position), 0)::text AS head FROM ${tables.events}`,
      )
      return row?.head ? BigInt(row.head) : 0n
    },

    async firstToken(): Promise<TrackingToken> {
      return FIRST_TOKEN
    },

    async latestToken(): Promise<TrackingToken> {
      const row = await adapter.queryOne<{ head: string | null }>(
        `SELECT COALESCE(MAX(sequence_position), 0)::text AS head FROM ${tables.events}`,
      )
      const head = row?.head ? BigInt(row.head) : 0n
      return globalSequenceToken(head)
    },

    async publish(events: ReadonlyArray<EventMessage>, uow?: UnitOfWork): Promise<void> {
      // publish = append without condition; same shared/own tx split as append().
      const targets: LockTarget[] = []
      const shared = await sharedPostgresTransaction(uow)

      const notifyAndFanout = async () => {
        await adapter.query(`NOTIFY ${notifyChannel}`)
        for (const sub of eventSubscribers) {
          try { await sub(events) } catch { /* ignore subscriber errors */ }
        }
      }

      const runPublish = async (tx: PostgresAdapterTransaction): Promise<void> => {
        await acquireWriteLocks(tx, targets)
        await checkAndInsert(tx, events, undefined)
      }

      if (shared !== undefined) {
        try {
          await runPublish(shared)
        } catch (err) {
          if ((err as { code?: string }).code === "23505") {
            throw AppendConditionError.fromConflictCount(0, -1n)
          }
          throw err
        }
        uow!.onAfterCommit(notifyAndFanout)
        return
      }

      try {
        await adapter.transaction(IsolationLevel.READ_COMMITTED, runPublish)
      } catch (err) {
        if ((err as { code?: string }).code === "23505") {
          throw AppendConditionError.fromConflictCount(0, -1n)
        }
        throw err
      }
      await notifyAndFanout()
    },

    subscribe(
      handler: (events: ReadonlyArray<EventMessage>) => Promise<void>,
    ): () => void {
      eventSubscribers.add(handler)
      return () => {
        eventSubscribers.delete(handler)
      }
    },

    open(condition: StreamingCondition): MessageStream<SequencedEvent> {
      // Resume the (xid8, position) tuple cursor exactly when the caller hands
      // back a gap-aware token (the durable cursor minted by this engine). A
      // bare position (fresh start or an explicit reset) seeds xid8 = '0', a
      // sentinel below any real xid8 (pg_current_xact_id() never returns < 3),
      // so the first fetch uses the position-only catch-up branch.
      const resume = condition.token ? unwrapToken(condition.token) : undefined
      const gapResume = resume && isGapAwareToken(resume) ? resume : undefined
      let cursorPosition = gapResume ? gapResume.sequence : condition.position
      let cursorXid = gapResume ? gapResume.gapKey : "0"
      const criteria = condition.query ? compileQuery(condition.query) : undefined
      let closed = false
      let onAvailable: (() => void) | null = null
      const buffer: SequencedEvent[] = []
      let polling = false
      let listenSub: { unlisten: () => Promise<void> } | undefined

      async function fetchBatch(limit = 100): Promise<void> {
        if (closed) return
        // When we have a real xid cursor, use the (xid8, position) tuple comparison
        // for gap-free ordering. On initial fetch (cursorXid = "0") we don't yet
        // have a real xid, so fall back to a plain sequence_position > $cursor filter —
        // the pg_snapshot_xmin watermark still applies to exclude in-flight transactions.
        let sql: string
        let queryParams: unknown[]

        if (cursorXid === "0") {
          // Initial fetch (no tuple cursor yet): position-only filter for all
          // committed events strictly after cursorPosition. Only reached on a
          // fresh start (position 0) or an explicit reset — once the first row
          // is read the cursor switches to the gap-free (xid8, position) tuple
          // branch below, which is what a live processor resumes from (it
          // persists the gap-aware token, never a bare position).
          // $1 = position, criteria starts at $2
          const builtInitial = criteria
            ? buildCriteriaWhere(criteria, 2)
            : { where: "true", params: [] as unknown[], nextParamIndex: 2 }
          const limitParam = builtInitial.nextParamIndex
          sql = `
            SELECT sequence_position::text AS sequence_position,
                   transaction_id::text AS transaction_id,
                   event_id, type, tags, payload, metadata, version, timestamp
            FROM ${tables.events}
            WHERE sequence_position > $1::bigint
              AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
              AND (${builtInitial.where})
            ORDER BY ${tables.events}.transaction_id ASC, ${tables.events}.sequence_position ASC
            LIMIT $${limitParam}
          `
          queryParams = [String(cursorPosition), ...builtInitial.params, limit]
        } else {
          // Subsequent fetch: (xid8, position) tuple comparison for gap-free ordering.
          // $1 = xid, $2 = position, criteria starts at $3
          const builtTuple = criteria
            ? buildCriteriaWhere(criteria, 3)
            : { where: "true", params: [] as unknown[], nextParamIndex: 3 }
          const limitParam = builtTuple.nextParamIndex
          sql = `
            SELECT sequence_position::text AS sequence_position,
                   transaction_id::text AS transaction_id,
                   event_id, type, tags, payload, metadata, version, timestamp
            FROM ${tables.events}
            WHERE (transaction_id, sequence_position) > ($1::xid8, $2::bigint)
              AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
              AND (${builtTuple.where})
            ORDER BY ${tables.events}.transaction_id ASC, ${tables.events}.sequence_position ASC
            LIMIT $${limitParam}
          `
          queryParams = [cursorXid, String(cursorPosition), ...builtTuple.params, limit]
        }

        const rows = await adapter.query<{
          sequence_position: string
          transaction_id: string
          event_id: string
          type: string
          tags: string[]
          payload: unknown
          metadata: unknown
          version: string
          timestamp: string | number
        }>(sql, queryParams)

        for (const r of rows) {
          const event = decodeEvent(r)
          const seq = BigInt(r.sequence_position)
          // The durable cursor positioned AFTER this event: the (xid8, position)
          // tuple. Persisting it lets a reopened stream resume the gap-free
          // tuple comparison instead of a lossy position-only filter that would
          // skip an event with a lower sequence_position but higher xid8.
          buffer.push({ sequence: seq, event, token: gapAwareToken(seq, r.transaction_id) })
          cursorXid = r.transaction_id
          cursorPosition = seq
        }
      }

      async function pump(): Promise<void> {
        if (polling || closed) return
        polling = true
        try {
          await fetchBatch()
          if (buffer.length > 0 && onAvailable) onAvailable()
        } finally {
          polling = false
        }
      }

      // Start polling immediately so we don't miss events that were committed
      // before the LISTEN subscription is established. The poll interval is
      // replaced by NOTIFY-driven pumps once LISTEN is up.
      let pollInterval: ReturnType<typeof setInterval> | undefined = setInterval(() => {
        if (closed) {
          clearInterval(pollInterval)
          pollInterval = undefined
          return
        }
        void pump()
      }, 250)

      // Wake-up via LISTEN/NOTIFY — supplements polling with instant delivery
      void adapter
        .listen(notifyChannel, () => {
          void pump()
        })
        .then((sub) => {
          listenSub = sub
          // Keep polling as a safety net even with LISTEN active.
          // The 250ms interval is cheap (no-op when no new events).
        })
        .catch(() => {
          // LISTEN not supported — polling fallback already running above
        })

      // Also run an immediate fetch to pick up any pre-existing events
      void pump()

      return messageStream<SequencedEvent>({
        next() {
          return buffer.shift()
        },
        peek() {
          return buffer[0]
        },
        hasNextAvailable() {
          return buffer.length > 0
        },
        setCallback(cb: () => void) {
          onAvailable = cb
        },
        isCompleted() {
          return closed
        },
        error() {
          return undefined
        },
        close() {
          closed = true
          onAvailable = null
          if (pollInterval) {
            clearInterval(pollInterval)
            pollInterval = undefined
          }
          if (listenSub) void listenSub.unlisten()
        },
      })
    },
  }
}
