/**
 * Postgres event-store schema DDL builders + idempotent bootstrap.
 *
 * Plan 12-02 deliverable. Locks the table shape used by:
 *   - the append SP (Plan 04) — INSERT … RETURNING sequence_position, transaction_id
 *   - the streaming query (Plan 05) — WHERE (transaction_id, sequence_position) > $bookmark
 *                                       AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
 *   - the sourcing query (Plan 04) — WHERE tags @> $required AND type IN (...)
 *   - the snapshot store (Plan 05) — INSERT … ON CONFLICT (state_name, state_id) DO UPDATE
 *
 * Minimum Postgres version: 14 (xid8 + pg_snapshot_xmin require PG14, per D-12.13).
 * Tag storage: `text[]` with GIN(fastupdate=off) — `@>` contains-all semantics in SQL.
 * Payload storage: events use JSONB (queryable metadata, compressed); snapshots use BYTEA
 *   (Serializer returns Uint8Array; no JSONB roundtrip cost).
 */

export interface TableNames {
  readonly events: string
  readonly snapshots: string
  readonly scheduled: string
}

export const DEFAULT_TABLE_NAMES: TableNames = {
  events: "kronos_events",
  snapshots: "kronos_snapshots",
  scheduled: "kronos_scheduled_events",
}

/**
 * Session-scoped advisory lock key for the schema bootstrap.
 *
 * Value `-89001n` matches kraken-tech's `ensureInstalled()` key, so an
 * operator running both libraries against the same database experiences
 * serialised bootstraps instead of duplicate-key races. Held with
 * `pg_advisory_lock` (session-scoped, NOT xact-scoped) because some
 * Postgres versions implicitly commit DDL — xact-scoped locks would
 * release mid-bootstrap.
 */
export const KRONOS_SCHEMA_LOCK_KEY: bigint = -89001n

export function buildEventsTableDDL(tables: TableNames): string {
  // event_id is sourced from EventMessage.identifier (UUID v7 per quick 260511-mks).
  // UNIQUE auto-creates a btree; v7's time-ordered prefix keeps it compact under
  // append load (a v4 random UUID would fragment the leaf pages over time).
  return `CREATE TABLE IF NOT EXISTS ${tables.events} (
  sequence_position  BIGSERIAL PRIMARY KEY,
  event_id           UUID NOT NULL UNIQUE,
  transaction_id     xid8 NOT NULL DEFAULT pg_current_xact_id(),
  type               TEXT COLLATE "C" NOT NULL,
  tags               TEXT[] NOT NULL DEFAULT '{}',
  payload            JSONB NOT NULL,
  metadata           JSONB NOT NULL DEFAULT '{}',
  recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (
  autovacuum_freeze_min_age   = 10000000,
  autovacuum_freeze_table_age = 100000000,
  FILLFACTOR = 100
);`
}

export function buildEventsIndexesDDL(tables: TableNames): string {
  return `CREATE UNIQUE INDEX IF NOT EXISTS ${tables.events}_type_pos_idx
  ON ${tables.events} (type COLLATE "C", sequence_position DESC);

CREATE INDEX IF NOT EXISTS ${tables.events}_tags_gin
  ON ${tables.events} USING GIN(tags) WITH (fastupdate = off);`
}

export function buildSnapshotsTableDDL(tables: TableNames): string {
  return `CREATE TABLE IF NOT EXISTS ${tables.snapshots} (
  state_name   TEXT COLLATE "C" NOT NULL,
  state_id     TEXT NOT NULL,
  position     BIGINT NOT NULL,
  payload      BYTEA NOT NULL,
  metadata     JSONB NOT NULL DEFAULT '{}',
  recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (state_name, state_id)
);`
}

/**
 * Scheduled-events table — holds events parked for future append.
 *
 * # Row lifecycle (tombstone model)
 *
 *   INSERT (status='pending')   ← schedule() inside a UoW
 *      ├── UPDATE → 'appended'  ← worker fires the schedule; row stays as tombstone
 *      └── UPDATE → 'cancelled' ← cancel(token) succeeds; row stays as tombstone
 *
 * Tombstones (rather than DELETE-on-fire) give cancel() three distinct
 * outcomes — `cancelled` / `already-appended` / `not-found` — by inspecting
 * the row's terminal status. The events table already grows unboundedly,
 * so a parallel tombstone table is no worse from a retention perspective.
 *
 * # Schedule id = event id
 *
 * `schedule_id` is the same UUID as the eventual `event_id` written to the
 * events table at fire-time. One UUID identifies the schedule pre-fire and
 * the event post-fire, so callers tracking the materialised event can
 * correlate back to the original schedule without an extra column.
 *
 * # Payload columns
 *
 * The whole EventMessage shape is captured inline (event_id, type, tags,
 * payload, metadata, version, message_timestamp) so the fire-time worker
 * can reconstruct it from a single row read. `message_timestamp` is the
 * EventMessage's authored timestamp (epoch ms) — distinct from
 * `created_at` (when the row was inserted) and `fire_at` (when it should
 * fire). At append-time, the worker MAY overwrite message_timestamp with
 * `now()` so consumers see the actual append time; that is an
 * implementation decision left to the scheduler.
 */
export function buildScheduledEventsTableDDL(tables: TableNames): string {
  return `CREATE TABLE IF NOT EXISTS ${tables.scheduled} (
  schedule_id        UUID PRIMARY KEY,
  fire_at            TIMESTAMPTZ NOT NULL,
  status             TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'appended', 'cancelled')),
  type               TEXT COLLATE "C" NOT NULL,
  tags               TEXT[] NOT NULL DEFAULT '{}',
  payload            JSONB NOT NULL,
  metadata           JSONB NOT NULL DEFAULT '{}',
  version            TEXT NOT NULL,
  message_timestamp  BIGINT NOT NULL,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);`
}

/**
 * Indexes for the scheduled-events table.
 *
 * The single critical index is the partial btree on `fire_at WHERE status =
 * 'pending'`. The worker's hot query — `SELECT … WHERE status = 'pending'
 * AND fire_at <= now() ORDER BY fire_at LIMIT n FOR UPDATE SKIP LOCKED` —
 * scans only pending rows, so a partial index keeps the hot path B-tree
 * tiny regardless of how many appended/cancelled tombstones accumulate.
 *
 * No index on `status` alone — every status query also filters by either
 * schedule_id (PK lookup) or fire_at (the partial index above).
 */
export function buildScheduledEventsIndexesDDL(tables: TableNames): string {
  return `CREATE INDEX IF NOT EXISTS ${tables.scheduled}_pending_fire_at_idx
  ON ${tables.scheduled} (fire_at)
  WHERE status = 'pending';`
}

/**
 * Minimal adapter contract bootstrapSchema needs. A subset of the full
 * PostgresAdapter interface authored in Plan 12-03 — structurally
 * compatible so Plan 12-04's adapter instance can be passed in directly.
 * Kept local to schema.ts to keep this plan independent of Plan 03.
 */
export interface SchemaBootstrapAdapter {
  query(sql: string, params?: unknown[]): Promise<unknown>
}

export interface BootstrapSchemaOptions {
  /** Override `kronos_events` / `kronos_snapshots`. */
  readonly tableNames?: TableNames
}

/**
 * Append-with-DCB-check stored procedure.
 *
 * Called from `createPostgresEventStore.appendEvents` (Plan 04 Task 3)
 * after advisory-lock acquisition. The SP is the single SQL statement
 * that BOTH performs the conflict check AND inserts the new events —
 * keeping them atomic without round-tripping a separate SELECT.
 *
 * Inputs (positional):
 *   $1  marker_position      bigint     — the AppendCondition.marker.position
 *   $2  has_condition        boolean    — false ⇒ skip the conflict check
 *   $3  criteria_where_sql   text       — Plan 04's criteria-sql builder output
 *   $4  criteria_params      jsonb      — parameter array for criteria_where_sql
 *   $5  event_ids            uuid[]     — N event identifiers (EventMessage.identifier, UUID v7)
 *   $6  event_types          text[]     — N event types (one per event)
 *   $7  event_tags           text[][]   — N tag arrays
 *   $8  event_payloads       jsonb[]    — N JSONB payloads
 *   $9  event_metadata       jsonb[]    — N metadata maps
 *
 * Returns: TABLE(out_position bigint, out_xid xid8) — one row per inserted event.
 *          The last row carries the consistency marker.
 *
 * On conflict: RAISE EXCEPTION USING ERRCODE = 'KR001' (D-12.12).
 * On duplicate event_id (UNIQUE violation): Postgres raises SQLSTATE 23505;
 * caller treats as idempotent no-op or surfaces per consumer policy.
 *
 * NOTE: dynamic-SQL is used (EXECUTE) because the criteria WHERE clause is
 * parameter-shaped and varies per call. SQL injection is mitigated by the
 * fact that criteria_where_sql is produced by buildCriteriaWhere — a typed
 * builder that NEVER concatenates user data into the WHERE string (all user
 * data flows through criteria_params as JSONB).
 */
export function buildAppendStoredProcedureDDL(tables: TableNames): string {
  return `CREATE OR REPLACE FUNCTION kronos_append_with_check(
  marker_position    bigint,
  has_condition      boolean,
  criteria_where_sql text,
  criteria_params    jsonb,
  event_ids          uuid[],
  event_types        text[],
  event_tags         text[][],
  event_payloads     jsonb[],
  event_metadata     jsonb[]
) RETURNS TABLE(out_position bigint, out_xid xid8) AS $$
DECLARE
  conflict_count bigint;
  i integer;
BEGIN
  IF has_condition THEN
    EXECUTE format(
      'SELECT count(*) FROM ${tables.events} WHERE sequence_position > $1 AND (%s)',
      criteria_where_sql
    )
    USING marker_position, criteria_params
    INTO conflict_count;

    IF conflict_count > 0 THEN
      RAISE EXCEPTION 'Append condition violated: % conflicting event(s) after position %',
        conflict_count, marker_position
        USING ERRCODE = 'KR001';
    END IF;
  END IF;

  FOR i IN 1 .. array_length(event_types, 1) LOOP
    -- event_id UNIQUE constraint surfaces duplicates as SQLSTATE 23505 (D-12.12);
    -- caller (Plan 04 Task 3) maps that to AppendConditionError or idempotent skip.
    INSERT INTO ${tables.events} (event_id, type, tags, payload, metadata)
    VALUES (event_ids[i], event_types[i], event_tags[i:i][1], event_payloads[i], event_metadata[i])
    RETURNING sequence_position, transaction_id INTO out_position, out_xid;
    RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql;`
}

/**
 * Idempotently create the event-store schema.
 *
 * Holds a session-scoped advisory lock (KRONOS_SCHEMA_LOCK_KEY) for the
 * duration so concurrent bootstraps (e.g. two app instances starting
 * simultaneously) serialise instead of racing on CREATE TABLE.
 *
 * The lock is RELEASED in a finally block — partial-DDL-then-throw must
 * NEVER leak a session lock that would block all subsequent bootstraps
 * on the same connection.
 *
 * The append stored procedure is applied as part of bootstrap so that
 * the SP is always up-to-date with the schema version.
 */
export async function bootstrapSchema(
  adapter: SchemaBootstrapAdapter,
  options: BootstrapSchemaOptions = {},
): Promise<void> {
  const tables = options.tableNames ?? DEFAULT_TABLE_NAMES

  // Acquire migration lock. Session-scoped is intentional: some Postgres
  // versions implicitly commit DDL, which would release an xact-scoped
  // lock mid-bootstrap.
  await adapter.query(`SELECT pg_advisory_lock($1)`, [KRONOS_SCHEMA_LOCK_KEY])

  try {
    await adapter.query(buildEventsTableDDL(tables))
    await adapter.query(buildEventsIndexesDDL(tables))
    await adapter.query(buildSnapshotsTableDDL(tables))
    await adapter.query(buildScheduledEventsTableDDL(tables))
    await adapter.query(buildScheduledEventsIndexesDDL(tables))
    await adapter.query(buildAppendStoredProcedureDDL(tables))
  } finally {
    // Release even on partial-DDL failure. The error (if any) propagates
    // to the caller after the lock has been freed.
    await adapter.query(`SELECT pg_advisory_unlock($1)`, [KRONOS_SCHEMA_LOCK_KEY])
  }
}
