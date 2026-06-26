/**
 * createPostgresEventScheduler — durable {@link EventScheduler} backed by
 * the kronos_scheduled_events table, plus a polling worker that fires
 * due schedules into the event store.
 *
 * # Schedule path (caller-driven, inside a UoW)
 *
 *   schedule(event, at)
 *     → requires INVOCATION phase
 *     → captures tags via the configured TagResolver at schedule-time
 *     → INSERT row (schedule_id = event.identifier, status='pending')
 *     → joins the active UoW transaction via getOrBeginActiveTransaction;
 *       if the UoW rolls back, the schedule is never persisted
 *     → returns { id: schedule_id } as the cancellation token
 *
 * # Cancel path
 *
 *   cancel(token)
 *     → SELECT … FOR UPDATE to lock the row + read prior status
 *     → branch:
 *         status='pending'   → UPDATE status='cancelled'  → { kind: 'cancelled' }
 *         status='appended'  → no UPDATE                  → { kind: 'already-appended' }
 *         status='cancelled' → no UPDATE                  → { kind: 'not-found' }
 *         no row             → no UPDATE                  → { kind: 'not-found' }
 *     → cancel inside a UoW joins the active tx; outside, opens its own
 *       adapter.transaction so the SELECT-FOR-UPDATE + UPDATE land atomically
 *
 * # Worker path (background)
 *
 *   start() spins a setInterval that, per tick, runs inside a fresh UoW:
 *     1. force the lazy pg tx open via getOrBeginActiveTransaction
 *     2. SELECT … WHERE status='pending' AND fire_at <= now()
 *        ORDER BY fire_at LIMIT $batchSize FOR UPDATE SKIP LOCKED
 *        — the SKIP LOCKED keeps multiple worker instances safe
 *     3. for each row: reconstruct EventMessage, call eventStore.append
 *        (which joins the same UoW tx), UPDATE status='appended'
 *     4. UoW COMMIT → all appends + status flips land atomically
 *
 *   If the worker process dies mid-tick before COMMIT, the rows stay
 *   'pending' and get re-picked on the next tick — at-least-once delivery.
 *   The schedule_id is reused as event.identifier so the events table's
 *   UNIQUE constraint dedupes any spurious double-append (e.g., on the
 *   rare race where a previous COMMIT succeeded but the status UPDATE
 *   failed afterwards — though here they share a tx, so this is mainly
 *   a defensive note).
 *
 * # Multi-node safety
 *
 *   FOR UPDATE SKIP LOCKED is the locking primitive. Two worker processes
 *   polling the same table will hand non-overlapping batches of rows to
 *   their respective ticks; neither blocks the other. No leader election
 *   or distributed lock is needed.
 */

import { qualifiedNameToString, qualifiedNameFromString } from "@kronos-ts/common"
import type { EventMessage } from "@kronos-ts/messaging"
import type {
  EventScheduler,
  ScheduleToken,
  CancelResult,
  UoWRunner,
} from "@kronos-ts/messaging"
import { getOrBeginActiveTransaction } from "@kronos-ts/messaging"
// Deep-path import: requireInvocationPhase is intentionally not in the
// messaging barrel — it's the framework-internal mutator guard consumed
// by append/send/emitUpdate, and now schedule(). Same access pattern.
import { requireInvocationPhase } from "@kronos-ts/messaging/processing-state"
import type { EventStore } from "@kronos-ts/eventsourcing"
import { IsolationLevel } from "./adapter.js"
import type { PostgresAdapter, PostgresAdapterTransaction } from "./adapter.js"
import { encodeTag } from "./criteria-sql.js"
import { type TableNames, DEFAULT_TABLE_NAMES } from "./schema.js"
import type { TagResolver } from "./postgres-event-store.js"

export interface PostgresEventSchedulerConfig {
  readonly adapter: PostgresAdapter
  readonly eventStore: EventStore
  /**
   * The composed UoW runner the worker uses per tick — must be wrapped
   * with {@link lazyTransactionalUnitOfWorkFactory} (or equivalent) so
   * the worker's UoW sees the postgres tx. Typically the resolved
   * `unitOfWorkFactory` slot.
   */
  readonly uowFactory: UoWRunner
  readonly tagResolver: TagResolver
  readonly tableNames?: TableNames
  /**
   * Worker poll interval. Defaults to 1000ms — a compromise between
   * fire-latency and DB chatter. Production users wanting tighter
   * latency should lower this, ideally combined with LISTEN/NOTIFY
   * wake-up (not yet wired here).
   */
  readonly pollIntervalMs?: number
  /** Max rows the worker processes per tick. Defaults to 50. */
  readonly batchSize?: number
}

export interface PostgresEventScheduler extends EventScheduler {
  /** Begin background polling. Idempotent. */
  start(): Promise<void>
  /** Stop polling. Resolves once the in-flight tick (if any) has settled. */
  stop(): Promise<void>
}

interface ScheduleRow {
  schedule_id: string
  type: string
  tags: string[]
  payload: unknown
  metadata: unknown
  version: string
  timestamp: string | number
  [key: string]: unknown
}

export function createPostgresEventScheduler(
  config: PostgresEventSchedulerConfig,
): PostgresEventScheduler {
  const { adapter, eventStore, uowFactory, tagResolver } = config
  const tables = config.tableNames ?? DEFAULT_TABLE_NAMES
  const pollIntervalMs = config.pollIntervalMs ?? 1000
  const batchSize = config.batchSize ?? 50

  async function insertSchedule(
    tx: PostgresAdapterTransaction,
    event: EventMessage,
    at: Date,
  ): Promise<string> {
    const scheduleId = event.identifier
    const encodedTags = tagResolver
      .resolve(event)
      .map((t) => encodeTag(t.key, t.value))
    await tx.query(
      `INSERT INTO ${tables.scheduled}
         (schedule_id, fire_at, status, type, tags, payload, metadata, version, timestamp)
       VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8)`,
      [
        scheduleId,
        at.toISOString(),
        qualifiedNameToString(event.name),
        encodedTags,
        JSON.stringify(event.payload ?? {}),
        JSON.stringify(event.metadata ?? {}),
        event.version,
        event.timestamp,
      ],
    )
    return scheduleId
  }

  async function cancelOnTx(
    tx: PostgresAdapterTransaction,
    scheduleId: string,
  ): Promise<CancelResult> {
    const rows = await tx.query<{ status: string }>(
      `SELECT status FROM ${tables.scheduled} WHERE schedule_id = $1 FOR UPDATE`,
      [scheduleId],
    )
    const row = rows[0]
    if (!row) return { kind: "not-found" }
    if (row.status === "appended") return { kind: "already-appended" }
    if (row.status === "cancelled") return { kind: "not-found" }

    await tx.query(
      `UPDATE ${tables.scheduled} SET status = 'cancelled' WHERE schedule_id = $1`,
      [scheduleId],
    )
    return { kind: "cancelled" }
  }

  function decodeJsonbValue(v: unknown): unknown {
    if (typeof v === "string") {
      try {
        return JSON.parse(v)
      } catch {
        return v
      }
    }
    return v ?? {}
  }

  function decodeTags(encoded: string[]): EventMessage["tags"] {
    return encoded.map((t) => {
      // U+001F unit separator — matches encodeTag in criteria-sql.ts
      const sep = t.indexOf("")
      return sep >= 0
        ? { key: t.slice(0, sep), value: t.slice(sep + 1) }
        : { key: t, value: "" }
    })
  }

  function reconstructEvent(row: ScheduleRow): EventMessage {
    return {
      kind: "event",
      identifier: row.schedule_id,
      name: qualifiedNameFromString(row.type),
      payload: decodeJsonbValue(row.payload),
      metadata: decodeJsonbValue(row.metadata) as EventMessage["metadata"],
      timestamp: Number(row.timestamp),
      version: row.version,
      tags: decodeTags(row.tags),
    }
  }

  // Worker state
  let running = false
  let timer: ReturnType<typeof setTimeout> | undefined
  let activeTick: Promise<void> | undefined

  async function tick(): Promise<void> {
    try {
      await uowFactory(undefined, async () => {
        const tx = await getOrBeginActiveTransaction<PostgresAdapterTransaction>()
        if (!tx) {
          // No lazy tx factory installed on the UoW — the scheduler was
          // configured with a uowFactory that doesn't wrap the postgres tx
          // manager. Without a shared tx the worker can't atomically
          // append-and-mark, so refuse to fire rather than risk
          // partial-state. This is a misconfiguration; surface loudly.
          throw new Error(
            "postgresEventScheduler worker requires a uowFactory wrapped with lazyTransactionalUnitOfWorkFactory + postgresTransactionManager",
          )
        }

        const rows = await tx.query<ScheduleRow>(
          `SELECT schedule_id, type, tags, payload, metadata, version, timestamp
           FROM ${tables.scheduled}
           WHERE status = 'pending' AND fire_at <= now()
           ORDER BY fire_at
           LIMIT $1
           FOR UPDATE SKIP LOCKED`,
          [batchSize],
        )
        if (rows.length === 0) return

        for (const row of rows) {
          const event = reconstructEvent(row)
          // eventStore.append joins our shared UoW tx via
          // getOrBeginActiveTransaction (postgres-event-store refactor).
          // event_id = schedule_id, so re-fires after a crash dedupe via
          // the events table's UNIQUE(event_id) constraint.
          await eventStore.append([event])
          await tx.query(
            `UPDATE ${tables.scheduled} SET status = 'appended' WHERE schedule_id = $1`,
            [row.schedule_id],
          )
        }
      })
    } catch (err) {
      // A failed tick leaves rows 'pending' (the UoW rolls back the whole
      // batch). The next tick re-tries. Log so operators see persistent
      // failures rather than a silent hang.
      console.warn("postgresEventScheduler: worker tick failed:", err)
    }
  }

  function scheduleNextTick(): void {
    if (!running) return
    timer = setTimeout(() => {
      activeTick = tick().finally(() => {
        activeTick = undefined
        scheduleNextTick()
      })
    }, pollIntervalMs)
  }

  return {
    async schedule(event: EventMessage, at: Date): Promise<ScheduleToken> {
      requireInvocationPhase()
      const shared = await getOrBeginActiveTransaction<PostgresAdapterTransaction>()
      if (shared === undefined) {
        // schedule() must be transactional with the caller's UoW so a
        // rolled-back command does not leak schedules. Refuse rather
        // than open a side-channel tx the caller can't roll back.
        throw new Error(
          "postgresEventScheduler.schedule requires a UoW with a postgres transaction (configure lazyTransactionalUnitOfWorkFactory + postgresTransactionManager)",
        )
      }
      const id = await insertSchedule(shared, event, at)
      return { id }
    },

    async cancel(token: ScheduleToken): Promise<CancelResult> {
      const shared = await getOrBeginActiveTransaction<PostgresAdapterTransaction>()
      if (shared !== undefined) {
        return cancelOnTx(shared, token.id)
      }
      return adapter.transaction(IsolationLevel.READ_COMMITTED, (tx) =>
        cancelOnTx(tx, token.id),
      )
    },

    async start(): Promise<void> {
      if (running) return
      running = true
      scheduleNextTick()
    },

    async stop(): Promise<void> {
      running = false
      if (timer !== undefined) {
        clearTimeout(timer)
        timer = undefined
      }
      if (activeTick !== undefined) {
        try {
          await activeTick
        } catch {
          // The tick logs its own failures; stop() returns successfully
          // either way so callers can shut down deterministically.
        }
      }
    },
  }
}
