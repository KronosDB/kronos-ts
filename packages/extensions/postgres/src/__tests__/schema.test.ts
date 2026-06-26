import { describe, it, expect } from "bun:test"
import {
  DEFAULT_TABLE_NAMES,
  KRONOS_SCHEMA_LOCK_KEY,
  buildEventsTableDDL,
  buildEventsIndexesDDL,
  buildSnapshotsTableDDL,
  buildScheduledEventsTableDDL,
  buildScheduledEventsIndexesDDL,
  bootstrapSchema,
  type SchemaBootstrapAdapter,
} from "../schema.js"

describe("DEFAULT_TABLE_NAMES", () => {
  it("uses the kronos_events / kronos_snapshots / kronos_scheduled_events defaults", () => {
    expect(DEFAULT_TABLE_NAMES.events).toBe("kronos_events")
    expect(DEFAULT_TABLE_NAMES.snapshots).toBe("kronos_snapshots")
    expect(DEFAULT_TABLE_NAMES.scheduled).toBe("kronos_scheduled_events")
  })
})

describe("KRONOS_SCHEMA_LOCK_KEY", () => {
  it("matches the kraken-tech migration lock key (-89001n) for cross-library bootstrap safety", () => {
    expect(KRONOS_SCHEMA_LOCK_KEY).toBe(-89001n)
  })
})

describe("buildEventsTableDDL", () => {
  it("declares xid8 transaction_id with pg_current_xact_id default (D-12.14)", () => {
    const ddl = buildEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toContain("transaction_id")
    expect(ddl).toMatch(/transaction_id\s+xid8\s+NOT NULL DEFAULT pg_current_xact_id\(\)/)
  })

  it("declares event_id UUID NOT NULL UNIQUE for at-least-once idempotency", () => {
    // event_id sources from EventMessage.identifier (UUID v7 per quick 260511-mks).
    // UNIQUE column-level constraint auto-creates a btree; v7's time-ordered
    // prefix keeps inserts at the right edge so the btree stays compact.
    const ddl = buildEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/event_id\s+UUID\s+NOT NULL\s+UNIQUE/)
    // Positional: event_id must appear before transaction_id (identifier column near top)
    const eventIdIdx = ddl.indexOf("event_id")
    const txnIdIdx = ddl.indexOf("transaction_id")
    expect(eventIdIdx).toBeGreaterThan(-1)
    expect(eventIdIdx).toBeLessThan(txnIdIdx)
  })

  it("declares BIGSERIAL sequence_position primary key", () => {
    const ddl = buildEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/sequence_position\s+BIGSERIAL\s+PRIMARY KEY/)
  })

  it("declares tags TEXT[] NOT NULL DEFAULT empty", () => {
    const ddl = buildEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/tags\s+TEXT\[\]\s+NOT NULL DEFAULT '\{\}'/)
  })

  it("declares payload as JSONB (not BYTEA — D-12 Claude's Discretion locked JSONB for query-ability)", () => {
    const ddl = buildEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/payload\s+JSONB\s+NOT NULL/)
    // Negative: must not silently flip to BYTEA on the payload column
    expect(ddl).not.toMatch(/payload\s+BYTEA/)
  })

  it("substitutes the events table name parameter", () => {
    const ddl = buildEventsTableDDL({ events: "my_events", snapshots: "ignored", scheduled: "ignored" })
    expect(ddl).toContain("CREATE TABLE IF NOT EXISTS my_events")
    expect(ddl).not.toContain("kronos_events")
  })

  it("sets FILLFACTOR=100 + freeze-age storage params for append-only optimisation", () => {
    const ddl = buildEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/FILLFACTOR\s*=\s*100/)
    expect(ddl).toMatch(/autovacuum_freeze_min_age/)
  })

  it("persists version + timestamp so source()/open() reconstruct the full EventMessage", () => {
    // timestamp is the EventMessage's authored timestamp (epoch ms).
    const ddl = buildEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/version\s+TEXT\s+NOT NULL/)
    expect(ddl).toMatch(/timestamp\s+BIGINT\s+NOT NULL/)
    // recorded_at was removed — the events table carries only the authored timestamp.
    expect(ddl).not.toMatch(/recorded_at/)
  })
})

describe("buildEventsIndexesDDL", () => {
  it("creates a unique (type, sequence_position DESC) index for type-filtered tailing", () => {
    const ddl = buildEventsIndexesDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/CREATE UNIQUE INDEX IF NOT EXISTS kronos_events_type_pos_idx/)
    expect(ddl).toMatch(/\(type COLLATE "C", sequence_position DESC\)/)
  })

  it("creates a GIN index on tags with fastupdate=off (D-12 Claude's Discretion locked)", () => {
    const ddl = buildEventsIndexesDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/USING GIN\(tags\)\s+WITH \(fastupdate = off\)/)
  })

  it("substitutes the events table name", () => {
    const ddl = buildEventsIndexesDDL({ events: "my_events", snapshots: "ignored", scheduled: "ignored" })
    expect(ddl).toMatch(/ON my_events/)
  })
})

describe("buildSnapshotsTableDDL", () => {
  it("uses BYTEA for the snapshot payload (Serializer returns Uint8Array; no JSONB roundtrip)", () => {
    const ddl = buildSnapshotsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/payload\s+BYTEA\s+NOT NULL/)
  })

  it("declares a composite primary key on (state_name, state_id) for upsert semantics", () => {
    const ddl = buildSnapshotsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/PRIMARY KEY \(state_name, state_id\)/)
  })
})

describe("buildScheduledEventsTableDDL", () => {
  it("uses schedule_id UUID as the primary key (same UUID as the eventual event_id)", () => {
    const ddl = buildScheduledEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/schedule_id\s+UUID\s+PRIMARY KEY/)
  })

  it("declares fire_at TIMESTAMPTZ — wall-clock target for the worker", () => {
    const ddl = buildScheduledEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/fire_at\s+TIMESTAMPTZ\s+NOT NULL/)
  })

  it("declares a CHECK constraint enumerating the three valid statuses", () => {
    // pending / appended / cancelled — tombstone model. CHECK keeps the column
    // honest at the DB level so an out-of-spec UPDATE can't silently corrupt state.
    const ddl = buildScheduledEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/status\s+TEXT\s+NOT NULL\s+DEFAULT 'pending'/)
    expect(ddl).toMatch(/CHECK \(status IN \('pending', 'appended', 'cancelled'\)\)/)
  })

  it("captures the full EventMessage shape (type, tags, payload, metadata, version, timestamp)", () => {
    const ddl = buildScheduledEventsTableDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/type\s+TEXT/)
    expect(ddl).toMatch(/tags\s+TEXT\[\]/)
    expect(ddl).toMatch(/payload\s+JSONB/)
    expect(ddl).toMatch(/metadata\s+JSONB/)
    expect(ddl).toMatch(/version\s+TEXT/)
    expect(ddl).toMatch(/timestamp\s+BIGINT/)
  })

  it("substitutes the scheduled table name parameter", () => {
    const ddl = buildScheduledEventsTableDDL({
      events: "ignored",
      snapshots: "ignored",
      scheduled: "my_sched",
    })
    expect(ddl).toContain("CREATE TABLE IF NOT EXISTS my_sched")
    expect(ddl).not.toContain("kronos_scheduled_events")
  })
})

describe("buildScheduledEventsIndexesDDL", () => {
  it("creates a partial btree on fire_at WHERE status = 'pending' for the hot polling path", () => {
    // The worker's hot query scans only pending rows; a partial index keeps
    // the B-tree tiny no matter how many appended/cancelled tombstones pile up.
    const ddl = buildScheduledEventsIndexesDDL(DEFAULT_TABLE_NAMES)
    expect(ddl).toMatch(/CREATE INDEX IF NOT EXISTS kronos_scheduled_events_pending_fire_at_idx/)
    expect(ddl).toMatch(/ON kronos_scheduled_events \(fire_at\)/)
    expect(ddl).toMatch(/WHERE status = 'pending'/)
  })
})

function makeMockAdapter(): { adapter: SchemaBootstrapAdapter; calls: string[]; throwOn?: RegExp } {
  const state: { adapter: SchemaBootstrapAdapter; calls: string[]; throwOn?: RegExp } = {
    calls: [],
    adapter: undefined as unknown as SchemaBootstrapAdapter,
  }
  state.adapter = {
    async query(sql: string, _params?: unknown[]) {
      state.calls.push(sql)
      if (state.throwOn && state.throwOn.test(sql)) {
        throw new Error("simulated DDL failure")
      }
      return []
    },
  }
  return state
}

describe("bootstrapSchema", () => {
  it("acquires session-scoped advisory lock BEFORE issuing any CREATE TABLE", async () => {
    const mock = makeMockAdapter()
    await bootstrapSchema(mock.adapter)
    const lockIdx = mock.calls.findIndex((c) => c.includes("pg_advisory_lock"))
    const firstDDLIdx = mock.calls.findIndex((c) => c.includes("CREATE TABLE"))
    expect(lockIdx).toBeGreaterThanOrEqual(0)
    expect(firstDDLIdx).toBeGreaterThan(lockIdx)
  })

  it("releases the advisory lock after DDL completes", async () => {
    const mock = makeMockAdapter()
    await bootstrapSchema(mock.adapter)
    const unlockIdx = mock.calls.findIndex((c) => c.includes("pg_advisory_unlock"))
    const lastDDLIdx = mock.calls.map((c, i) => c.includes("CREATE") ? i : -1).filter((i) => i >= 0).pop() ?? -1
    expect(unlockIdx).toBeGreaterThan(lastDDLIdx)
  })

  it("issues DDL in order: events table, events indexes, snapshots table", async () => {
    const mock = makeMockAdapter()
    await bootstrapSchema(mock.adapter)
    const eventsTbl = mock.calls.findIndex((c) => c.includes("CREATE TABLE IF NOT EXISTS kronos_events"))
    const eventsIdx = mock.calls.findIndex((c) => c.includes("kronos_events_tags_gin"))
    const snapsTbl = mock.calls.findIndex((c) => c.includes("CREATE TABLE IF NOT EXISTS kronos_snapshots"))
    expect(eventsTbl).toBeGreaterThanOrEqual(0)
    expect(eventsIdx).toBeGreaterThan(eventsTbl)
    expect(snapsTbl).toBeGreaterThan(eventsIdx)
  })

  it("substitutes both table names", async () => {
    const mock = makeMockAdapter()
    await bootstrapSchema(mock.adapter, {
      tableNames: { events: "my_evt", snapshots: "my_snap", scheduled: "my_sched" },
    })
    expect(mock.calls.some((c) => c.includes("CREATE TABLE IF NOT EXISTS my_evt"))).toBe(true)
    expect(mock.calls.some((c) => c.includes("CREATE TABLE IF NOT EXISTS my_snap"))).toBe(true)
    expect(mock.calls.some((c) => c.includes("CREATE TABLE IF NOT EXISTS my_sched"))).toBe(true)
    expect(mock.calls.some((c) => c.includes("kronos_events"))).toBe(false)
    expect(mock.calls.some((c) => c.includes("kronos_snapshots"))).toBe(false)
    expect(mock.calls.some((c) => c.includes("kronos_scheduled_events"))).toBe(false)
  })

  it("releases the lock even when a DDL statement throws", async () => {
    const mock = makeMockAdapter()
    mock.throwOn = /tags_gin/
    let caught: Error | undefined
    try {
      await bootstrapSchema(mock.adapter)
    } catch (e) {
      caught = e as Error
    }
    expect(caught).toBeDefined()
    expect(caught?.message).toContain("simulated DDL failure")
    expect(mock.calls.some((c) => c.includes("pg_advisory_unlock"))).toBe(true)
  })

  it("is idempotent — running twice does not error", async () => {
    const mock = makeMockAdapter()
    await bootstrapSchema(mock.adapter)
    await bootstrapSchema(mock.adapter)
    // CREATE TABLE IF NOT EXISTS is idempotent at the SQL layer; this asserts
    // no client-side guard prevents re-execution.
    const tableCreates = mock.calls.filter((c) => c.includes("CREATE TABLE IF NOT EXISTS")).length
    expect(tableCreates).toBe(6) // 3 tables (events / snapshots / scheduled) × 2 runs
  })
})
