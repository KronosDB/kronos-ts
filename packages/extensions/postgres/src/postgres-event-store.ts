/**
 * createPostgresEventStore — Plan 12-04 implementation of EventStorageEngine
 * source/appendEvents/append. The StreamableEventSource methods (open,
 * firstToken, latestToken, getHeadPosition, publish, subscribe) land in
 * Plan 12-05.
 *
 * Append path:
 *   1. open transaction at READ COMMITTED
 *   2. acquireWriteLocks for the criteria tags (not event types) so that
 *      two writers on the SAME criteria tag serialize, while disjoint-tag
 *      writers run in parallel
 *   3. Conflict check: SELECT count(*) WHERE sequence_position > marker AND criteria
 *   4. If conflict count > 0 → throw AppendConditionError (code KR001)
 *   5. INSERT events returning sequence_position for the ConsistencyMarker
 *   6. commit() → COMMIT; afterCommit() → marker; rollback() → fire-and-forget
 *      ROLLBACK (synchronous void per the framework contract)
 *
 * Note on the stored procedure (buildAppendStoredProcedureDDL): The SP is
 * registered in schema.ts and available on the DB, but this plan uses
 * direct parameterised SQL for the conflict check + INSERT rather than
 * calling the SP. The SP's dynamic-SQL approach has complex $N-rebinding
 * requirements (criteria_params JSONB → USING binding) that are cleaner to
 * handle in TypeScript. Plan 06's review may revisit whether the SP
 * provides a meaningful benefit.
 */

import type {
  EventStorageEngine,
  AppendTransaction,
} from "@kronos-ts/eventsourcing"
import { markerAt } from "@kronos-ts/eventsourcing"
import type { ConsistencyMarker } from "@kronos-ts/eventsourcing"
import type { SourcingCondition } from "@kronos-ts/eventsourcing"
import type { SourcingResult } from "@kronos-ts/eventsourcing"
import type { AppendCondition } from "@kronos-ts/eventsourcing"
import type { EventMessage, EventCriteria } from "@kronos-ts/messaging"
import { qualifiedNameToString, qualifiedNameFromString } from "@kronos-ts/common"
import type { PostgresAdapter, PostgresAdapterTransaction } from "./adapter.js"
import { IsolationLevel } from "./adapter.js"
import { acquireWriteLocks, type LockTarget } from "./advisory-locks.js"
import { buildCriteriaWhere, encodeTag } from "./criteria-sql.js"
import { AppendConditionError, isDcbViolation, KRONOS_DCB_VIOLATION_SQLSTATE } from "./errors.js"
import { type TableNames, DEFAULT_TABLE_NAMES } from "./schema.js"

// Minimal Serializer / TagResolver structural shapes — the real slots are
// declared in the core; we accept anything compatible.
export interface Serializer {
  serialize(value: unknown): Uint8Array
  deserialize<T = unknown>(bytes: Uint8Array): T
}

export interface TagResolver {
  resolve(event: EventMessage): ReadonlyArray<{ key: string; value: string }>
}

export interface PostgresEventStoreConfig {
  readonly adapter: PostgresAdapter
  readonly serializer: Serializer
  readonly tagResolver: TagResolver
  readonly tableNames?: TableNames
}

// Partial — Plan 05 will augment this with the StreamableEventSource members.
export type PartialEventStorageEngine = Pick<
  EventStorageEngine,
  "source" | "appendEvents" | "append"
>

export function createPostgresEventStore(
  config: PostgresEventStoreConfig,
): PartialEventStorageEngine {
  const { adapter, tagResolver } = config
  const tables = config.tableNames ?? DEFAULT_TABLE_NAMES

  function eventTypeOf(e: EventMessage): string {
    return qualifiedNameToString(e.name)
  }

  /**
   * Extract lock targets from the criteria — the writer locks on what it is
   * READING (the criteria tags), not just what it is writing. This ensures
   * two writers on the same criteria tag serialize (one blocks until the
   * other commits), while writers on disjoint criteria tags run in parallel.
   *
   * For `any-tag` or empty criteria, returns an empty array so only the
   * global-intent S-lock is acquired (acquireWriteLocks handles the empty case).
   */
  function lockTargetsForCondition(condition: AppendCondition | undefined): LockTarget[] {
    if (!condition) return []
    return extractCriteriaTags(condition.criteria).map((tag) => ({ type: "", tag }))
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
    return tagResolver.resolve(e).map((t) => encodeTag(t.key, t.value))
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
      const built = buildCriteriaWhere(condition.criteria, 2) // $1 = markerPos
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
        `INSERT INTO ${tables.events} (event_id, type, tags, payload, metadata)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING sequence_position, transaction_id`,
        [
          e.identifier,
          type,
          encodedTags,
          JSON.stringify(payload),
          JSON.stringify(metadata),
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

  return {
    async source(condition: SourcingCondition): Promise<SourcingResult> {
      const start = condition.start ?? 0n
      const built = buildCriteriaWhere(condition.criteria, 2) // $1 = start
      const sql = `
        SELECT sequence_position, type, tags, payload, metadata
        FROM ${tables.events}
        WHERE sequence_position >= $1 AND (${built.where})
        ORDER BY sequence_position ASC
      `
      const rows = await adapter.query<{
        sequence_position: string
        type: string
        tags: string[]
        payload: unknown
        metadata: unknown
      }>(sql, [start, ...built.params])

      const events: EventMessage[] = rows.map((r) => decodeEvent(r))
      const headRow = await adapter.queryOne<{ head: string | null }>(
        `SELECT MAX(sequence_position)::text AS head FROM ${tables.events}`,
      )
      const head = headRow?.head ? BigInt(headRow.head) : -1n
      const lastPos = rows.length > 0 ? BigInt(rows[rows.length - 1]!.sequence_position) : -1n
      const marker = rows.length > 0 ? markerAt(lastPos) : markerAt(head)
      return { events, marker }
    },

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
    ): Promise<ConsistencyMarker> {
      // Convenience: appendEvents + commit + afterCommit in one shot.
      const targets = lockTargetsForCondition(condition)
      try {
        return await adapter.transaction(IsolationLevel.READ_COMMITTED, async (tx) => {
          await acquireWriteLocks(tx, targets)
          const captured = await checkAndInsert(tx, events, condition)
          return markerAt(captured.position)
        })
      } catch (err) {
        if (isDcbViolation(err)) {
          // Re-throw AppendConditionError directly (already has correct type)
          throw err
        }
        if ((err as { code?: string }).code === "23505") {
          throw AppendConditionError.fromConflictCount(0, condition?.marker.position ?? -1n)
        }
        throw err
      }
    },
  }
}

function decodeEvent(row: {
  type: string
  tags: string[]
  payload: unknown
  metadata: unknown
  sequence_position: string
}): EventMessage {
  const qn = qualifiedNameFromString(row.type)
  const tags = row.tags.map((t) => {
    const sep = t.indexOf("")
    return sep >= 0
      ? { key: t.slice(0, sep), value: t.slice(sep + 1) }
      : { key: t, value: "" }
  })
  return {
    name: qn,
    tags,
    payload: row.payload,
    metadata: row.metadata,
  } as unknown as EventMessage
}
