// ---------------------------------------------------------------------------
// POSTGRES SNAPSHOTS — the capability tier, and the only place in this package
// that knows `kronos_snapshots` exists.
//
// ONE WRAPPER OWNS BOTH HALVES, which is what makes this family's story simple.
// The WRITE is a row upsert. The READ is a single fused statement — the cache
// lookup, the start position derived from it, the event query from there, and
// the head, in ONE round trip. They used to live apart: a `postgresSnapshotStore`
// held the write, and `postgresEventStore` grew a snapshot branch to hold the
// read, and a host had to wire two objects and hand both the SAME serializer or
// get bytes one side could not decode. Now there is one object, one serializer,
// one line of wiring, and the base event store has never heard of any of it.
//
// "NATIVE" WAS NEVER A FEATURE — IT IS JUST OWNING THE QUERY. A wrapper that
// only held a seam could not do better than two round trips: fetch the entry,
// then source after its position. This one holds the CONNECTION, so it can say
// the whole thing once. That is the entire difference between this family and
// the in-memory, KronosDB and Axon Server ones, and it is a difference in what
// the wrapper can REACH, not in what the capability MEANS.
// ---------------------------------------------------------------------------

import type {
  EventMessage,
  EventStore,
  Serializer,
  Snapshot,
  SnapshotStoreCapability,
  SourcingCondition,
  SourcingResult,
  UnitOfWork,
} from "@kronos-ts/core"
import { compileQuery, markerAt, withoutSnapshotKey } from "@kronos-ts/core"
import { buildCriteriaWhere } from "./criteria-sql.js"
import { decodeEvent, type EventRow, EVENT_COLUMNS } from "./event-row.js"
import type { PostgresResource } from "./postgres-pool.js"
import { sharedPostgresTransaction } from "./postgres-transaction.js"
import { decodeSnapshot, encodeSnapshot } from "./snapshot-codec.js"

export type PostgresSnapshottingEventStoreConfig = {
  /**
   * How a state's fold becomes bytes, and back. Required, not defaulted: the
   * `payload` column is BYTEA and what goes into it is the application's
   * choice — `jsonSerializer()` for the common case, something else when the
   * cached shape is expensive to encode.
   *
   * ONE SERIALIZER, ONE OBJECT. It used to be possible to hand the writing side
   * and the reading side different ones, because they were different functions;
   * that class of mistake does not exist any more.
   */
  readonly serializer: Serializer
}

/**
 * Add the snapshotting capability to a postgres-backed log.
 *
 * ```ts
 * const eventStore = postgresSnapshottingEventStore(
 *   postgresEventStore(pg, { tagResolver }),
 *   pg,
 *   { serializer: jsonSerializer() },
 * )
 * ```
 *
 * ADDITIVE, NOT COLLAPSING. It returns `E & SnapshotStoreCapability` — the store you
 * passed in, plus the write — so nothing the inner store carried is laundered
 * on the way through and capabilities stack in either order. Wrapping an
 * upcasting store leaves an upcasting store; wrapping this in an upcasting
 * store leaves a snapshot-capable store. A wrapper typed
 * `(EventStore) => SnapshotCapableEventStore` would have erased whichever
 * capability it was not itself adding, and a compile-time demand built on
 * erased capabilities rejects configurations that genuinely work.
 *
 * `pg` MUST BE THE SAME RESOURCE the wrapped store reads events from. The fused
 * statement joins `kronos_snapshots` against `kronos_events` in one plan; two
 * different databases cannot be joined and would not be a fusion.
 *
 * COMPOSE UPCASTING OUTERMOST — `upcastingEventStore(postgresSnapshottingEventStore(…), upcast)`.
 * The snapshot layer decides WHICH events are read; the upcast layer decides
 * what each of them MEANS, and the fused read below bypasses the store it wraps
 * for the snapshot path, so an upcaster placed INSIDE this wrapper would not see
 * the events this statement returns. Outermost is both the documented order and
 * the only one that is correct here.
 */
export function postgresSnapshottingEventStore<E extends EventStore>(
  next: E,
  pg: PostgresResource,
  config: PostgresSnapshottingEventStoreConfig,
): E & SnapshotStoreCapability {
  const adapter = pg
  const { serializer } = config
  const tables = pg.tables

  /**
   * THE FUSED READ — four CTEs and a LEFT JOIN off a one-row anchor.
   *
   *   snap   the cache entry, or nothing
   *   bound  where the event scan starts: `snap.position + 1`, floored by any
   *          `start` the condition independently asked for, so the two
   *          narrowings compose instead of fighting
   *   head   `MAX(sequence_position)` — always exactly one row, which is what
   *          guarantees the outer SELECT returns a row even when the state has
   *          no events and no snapshot
   *   ev     the event query itself, from `bound`
   *
   * The snapshot's columns ride along on every event row. That is a few bytes
   * repeated per row and it buys the whole fusion; the alternative — a UNION
   * with the snapshot as a distinct arm — would have to reconcile BYTEA against
   * JSONB in one result column and is worse in every way. `payload` comes back
   * as `encode(payload, 'base64')` for the same reason: a BYTEA cannot ride
   * beside the event columns without a cast, so it is asked for as text and
   * decoded by the shared codec.
   *
   * FITNESS IS STILL CLIENT-SIDE, deliberately. The server could compare shapes
   * in SQL, but the shape a fold starts from is a fact about the running code,
   * not about the database — so the repository asks it once, for every backend,
   * and a mismatch costs one extra round trip for a full replay.
   */
  async function sourceFused(
    condition: SourcingCondition,
    key: NonNullable<SourcingCondition["snapshot"]>,
  ): Promise<SourcingResult> {
    // $1 = the cache key, $2 = the condition's own start floor
    const start = condition.start ?? 0n
    const built = buildCriteriaWhere(compileQuery(condition.query), 3)
    const sql = `
        WITH snap AS (
          SELECT position, payload, metadata
            FROM ${tables.snapshots}
           WHERE key = $1
        ),
        bound AS (
          SELECT GREATEST(
                   COALESCE((SELECT position + 1 FROM snap), 0::bigint),
                   $2::bigint
                 ) AS start_at
        ),
        head AS (
          SELECT COALESCE(MAX(sequence_position), -1)::bigint AS head FROM ${tables.events}
        ),
        ev AS (
          SELECT ${EVENT_COLUMNS}
            FROM ${tables.events}
           WHERE sequence_position >= (SELECT start_at FROM bound)
             AND (${built.where})
        )
        SELECT ev.sequence_position, ev.event_id, ev.type, ev.tags,
               ev.payload, ev.metadata, ev.version, ev.timestamp,
               head.head::text                              AS head,
               (SELECT position::text FROM snap)            AS snap_position,
               (SELECT encode(payload, 'base64') FROM snap) AS snap_payload,
               (SELECT metadata FROM snap)                  AS snap_metadata
          FROM head LEFT JOIN ev ON true
         ORDER BY ev.sequence_position ASC NULLS LAST
      `
    const rows = await adapter.query<
      Partial<EventRow> & {
        head: string | null
        snap_position: string | null
        snap_payload: string | null
        snap_metadata: unknown
      }
    >(sql, [key.key, start, ...built.params])

    // The anchor guarantees at least one row; an empty `ev` shows up as that
    // row with every event column NULL.
    const anchor = rows[0]
    const head = anchor?.head ? BigInt(anchor.head) : -1n
    const eventRows = rows.filter((r) => r.event_id != null) as EventRow[]
    const events: EventMessage[] = eventRows.map((r) => decodeEvent(r))

    const snapshot =
      anchor && anchor.snap_position != null && anchor.snap_payload != null
        ? decodeSnapshot(serializer, key.key, {
            position: anchor.snap_position,
            payload: anchor.snap_payload,
            metadata: anchor.snap_metadata,
          })
        : undefined

    const lastPos = eventRows.length > 0
      ? BigInt(eventRows[eventRows.length - 1]!.sequence_position)
      : -1n
    const marker = eventRows.length > 0 ? markerAt(lastPos) : markerAt(head)

    return { events, marker, ...(snapshot !== undefined ? { snapshot } : {}) }
  }

  return {
    ...next,

    async storeSnapshot(key: string, snapshot: Snapshot, uow?: UnitOfWork): Promise<void> {
      const { payload, metadata } = encodeSnapshot(serializer, key, snapshot)
      // Hand the write the task's transaction when there is one, so a cache
      // entry cannot become visible for a fold whose events rolled back.
      // Without a unit of work it is its own statement, which is the common
      // case: the repository writes fire-and-forget, outside the task.
      const shared = await sharedPostgresTransaction(uow)
      const run = shared ?? adapter
      await run.query(
        `INSERT INTO ${tables.snapshots}
           (key, position, payload, metadata, recorded_at)
         VALUES ($1, $2::bigint, $3, $4::jsonb, now())
         ON CONFLICT (key) DO UPDATE
           SET position    = EXCLUDED.position,
               payload     = EXCLUDED.payload,
               metadata    = EXCLUDED.metadata,
               recorded_at = EXCLUDED.recorded_at`,
        [key, String(snapshot.position), payload, JSON.stringify(metadata)],
      )
    },

    async source(condition: SourcingCondition): Promise<SourcingResult> {
      // No key, no fusion, and the store below serves the read exactly as it
      // always did — which is every `ctx.source(query)` and every state without
      // a policy. The strategy is CONSUMED here when there is one, so the
      // wrapped store never sees a key it might try to serve twice.
      return condition.snapshot !== undefined
        ? sourceFused(withoutSnapshotKey(condition), condition.snapshot)
        : next.source(condition)
    },
    // The spread of a generic is opaque to the checker, so the shape it
    // produces is asserted rather than inferred; the type probe is what makes
    // the assertion honest.
  } as E & SnapshotStoreCapability
}
