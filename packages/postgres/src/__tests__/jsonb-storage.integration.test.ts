/**
 * WHAT A JSONB COLUMN HOLDS.
 *
 * Every JSONB column the package writes holds a JSON object, so SQL such as
 * `payload->>'field'` reads it: an event's and a scheduled event's `payload`
 * and `metadata`, and a snapshot's `metadata`. postgres.js and Bun.SQL encode
 * a string bound to a JSONB parameter as a JSON string, which is how rows came
 * to hold a JSON string containing the JSON text. Those rows still read, and
 * one UPDATE per table rewrites them.
 *
 * The tests run over all three drivers, because each binds parameters itself.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import {
  generateIdentifier,
  polling,
  sourcingCondition,
  unitOfWork,
  type EventMessage,
  type TaggedEvent,
  type SerializedObject,
  type Serializer,
} from "@kronos-ts/core"
import type { PostgresAdapter } from "../adapter.js"
import { bunSqlAdapter } from "../adapters/bun-sql.js"
import { pgAdapter } from "../adapters/pg.js"
import { postgresAdapter } from "../adapters/postgres.js"
import { encodeTag } from "../criteria-sql.js"
import { postgresEventStore } from "../postgres-event-store.js"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresSchedulingEventStore } from "../postgres-scheduling-event-store.js"
import { postgresSnapshottingEventStore } from "../postgres-snapshotting-event-store.js"
import { postgresUnitOfWork } from "../postgres-transaction.js"
import { DEFAULT_TABLE_NAMES as T } from "../schema.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"

let pg: RunningPostgres

beforeAll(async () => {
  pg = await startPostgresContainer()
}, 60_000)

afterAll(async () => {
  await pg.stop()
}, 30_000)

/**
 * The statements a host runs once to rewrite rows stored as JSON strings.
 * `#>> '{}'` reads the string a JSONB string holds, and the cast parses it.
 */
const REWRITE_EVENTS = `
  UPDATE ${T.events}
     SET payload  = CASE WHEN jsonb_typeof(payload)  = 'string' THEN (payload  #>> '{}')::jsonb ELSE payload  END,
         metadata = CASE WHEN jsonb_typeof(metadata) = 'string' THEN (metadata #>> '{}')::jsonb ELSE metadata END
   WHERE jsonb_typeof(payload) = 'string' OR jsonb_typeof(metadata) = 'string'`
const REWRITE_SCHEDULED = `
  UPDATE ${T.scheduled}
     SET payload  = CASE WHEN jsonb_typeof(payload)  = 'string' THEN (payload  #>> '{}')::jsonb ELSE payload  END,
         metadata = CASE WHEN jsonb_typeof(metadata) = 'string' THEN (metadata #>> '{}')::jsonb ELSE metadata END
   WHERE jsonb_typeof(payload) = 'string' OR jsonb_typeof(metadata) = 'string'`
const REWRITE_SNAPSHOTS = `
  UPDATE ${T.snapshots}
     SET metadata = (metadata #>> '{}')::jsonb
   WHERE jsonb_typeof(metadata) = 'string'`

/** The serializer type a snapshot row was read with, so a test can see it. */
let deserializedTypes: string[] = []
const SERIALIZER: Serializer = {
  serialize(x: unknown, type: string, revision: string = ""): SerializedObject {
    return { type: `json:${type}`, revision, data: new TextEncoder().encode(JSON.stringify(x)) }
  },
  deserialize<V>(data: SerializedObject): V {
    deserializedTypes.push(data.type)
    return JSON.parse(new TextDecoder().decode(data.data)) as V
  },
  canConvert(): boolean {
    return true
  },
}

const drivers: ReadonlyArray<[string, () => PostgresAdapter]> = [
  ["pgAdapter", () => pgAdapter({ connectionString: pg.connectionString })],
  ["postgresAdapter", () => postgresAdapter({ connectionString: pg.connectionString })],
  ["bunSqlAdapter", () => bunSqlAdapter({ connectionString: pg.connectionString })],
]

describe.each(drivers)("JSONB columns over %s", (driver, make) => {
  let pool: PostgresResource
  let uow: ReturnType<typeof postgresUnitOfWork>
  let store: ReturnType<typeof wire>
  const stream = (name: string) => `${driver}-${name}`

  function wire() {
    return polling(
      postgresSchedulingEventStore(
        postgresSnapshottingEventStore(postgresEventStore(pool), pool, SERIALIZER),
        pool,
        uow,
      ),
      20,
    )
  }

  beforeAll(async () => {
    pool = postgresPool(make())
    await pool.start()
    uow = postgresUnitOfWork(unitOfWork, pool)
    store = wire()
  }, 60_000)

  afterAll(async () => {
    await store.stopScheduling()
    await pool.close()
  }, 30_000)

  function event(name: string, n: number): TaggedEvent {
    return {
      event: {
        kind: "event",
        identifier: generateIdentifier(),
        name: { namespace: "jsonb", name: "Happened" },
        payload: { n, text: `it's "quoted" \\ {braced} ${n}`, nested: { list: [1, null, "NULL"] } },
        metadata: { correlationId: `c-${n}` },
        timestamp: 1_700_000_000_000 + n,
        version: "1",
      } as EventMessage,
      tags: [{ key: "stream", value: stream(name) }],
    }
  }

  type Stored = { payload_type: string; metadata_type: string; text: string | null; correlation: string | null }

  /** What SQL sees in the JSONB columns of the row `id` of `table`. */
  async function stored(table: string, idColumn: string, id: string): Promise<Stored | null> {
    return pool.queryOne<Stored>(
      `SELECT jsonb_typeof(payload) AS payload_type, jsonb_typeof(metadata) AS metadata_type,
              payload->>'text' AS text, metadata->>'correlationId' AS correlation
         FROM ${table} WHERE ${idColumn} = $1::uuid`,
      [id],
    )
  }

  const asObjects = ({ event: e }: TaggedEvent): Stored => ({
    payload_type: "object",
    metadata_type: "object",
    text: (e.payload as { text: string }).text,
    correlation: (e.metadata as { correlationId: string }).correlationId,
  })

  /** Insert `e` into the events table the way rows were stored before: JSONB strings. */
  async function insertEventAsStrings({ event: e, tags }: TaggedEvent): Promise<void> {
    await pool.query(
      `INSERT INTO ${T.events} (event_id, type, tags, payload, metadata, version, timestamp)
       VALUES ($1::uuid, $2, $3::text[], to_jsonb($4::text), to_jsonb($5::text), $6, $7::bigint)`,
      [
        e.identifier,
        `${e.name.namespace}.${e.name.name}`,
        tags.map((t) => encodeTag(t.key, t.value)),
        JSON.stringify(e.payload),
        JSON.stringify(e.metadata),
        e.version,
        e.timestamp,
      ],
    )
  }

  async function insertScheduleAsStrings({ event: e, tags }: TaggedEvent, fireAt: Date): Promise<void> {
    await pool.query(
      `INSERT INTO ${T.scheduled} (schedule_id, fire_at, status, type, tags, payload, metadata, version, timestamp)
       VALUES ($1::uuid, $2::timestamptz, 'pending', $3, $4::text[], to_jsonb($5::text), to_jsonb($6::text), $7, $8::bigint)`,
      [
        e.identifier,
        fireAt.toISOString(),
        `${e.name.namespace}.${e.name.name}`,
        tags.map((t) => encodeTag(t.key, t.value)),
        JSON.stringify(e.payload),
        JSON.stringify(e.metadata),
        e.version,
        e.timestamp,
      ],
    )
  }

  async function insertSnapshotAsString(key: string, state: unknown, type: string): Promise<void> {
    await pool.query(
      `INSERT INTO ${T.snapshots} (key, position, payload, metadata)
       VALUES ($1, 0, convert_to($2::text, 'UTF8'), to_jsonb($3::text))`,
      [key, JSON.stringify(state), JSON.stringify({ __kr_serializer_type: type, __kr_serializer_revision: "" })],
    )
  }

  const read = (name: string) => store.source(sourcingCondition({ tags: { stream: stream(name) } }))

  async function eventually<V>(probe: () => Promise<V | undefined>): Promise<V> {
    const deadline = Date.now() + 15_000
    for (;;) {
      const value = await probe()
      if (value !== undefined) return value
      if (Date.now() > deadline) throw new Error("timed out")
      await new Promise((r) => setTimeout(r, 20))
    }
  }

  it("stores an appended event's payload and metadata as objects", async () => {
    const alone = event("append", 0)
    const together = [event("append", 1), event("append", 2), event("append", 3)]
    await store.append([alone])
    await store.append(together)

    for (const e of [alone, ...together]) {
      expect(await stored(T.events, "event_id", e.event.identifier)).toEqual(asObjects(e))
    }
    expect((await read("append")).events).toEqual([alone, ...together].map((e) => e.event))
  })

  it("stores a scheduled event's payload and metadata as objects", async () => {
    const e = event("schedule", 0)
    await uow().execute((u) => store.schedule(e, new Date(Date.now() + 3_600_000), u))

    expect(await stored(T.scheduled, "schedule_id", e.event.identifier)).toEqual(asObjects(e))
  })

  it("stores a snapshot's metadata as an object", async () => {
    const key = stream("snapshot")
    await store.storeSnapshot(key, { position: 7n, state: { total: 3 } })

    const row = await pool.queryOne<{ metadata_type: string; serializer_type: string | null }>(
      `SELECT jsonb_typeof(metadata) AS metadata_type, metadata->>'__kr_serializer_type' AS serializer_type
         FROM ${T.snapshots} WHERE key = $1`,
      [key],
    )
    expect(row).toEqual({ metadata_type: "object", serializer_type: `json:${key}` })
  })

  it("reads an event whose payload and metadata are stored as JSON strings", async () => {
    const old = event("old-event", 0)
    await insertEventAsStrings(old)
    expect(await stored(T.events, "event_id", old.event.identifier)).toEqual({
      payload_type: "string",
      metadata_type: "string",
      text: null,
      correlation: null,
    })

    expect((await read("old-event")).events).toEqual([old.event])
    // The read that leads with a snapshot decodes the same rows.
    const fused = await store.source({ query: { tags: { stream: stream("old-event") } }, snapshot: { key: "none" } })
    expect(fused.events).toEqual([old.event])
  })

  it("fires a schedule whose payload and metadata are stored as JSON strings", async () => {
    const old = event("old-schedule", 0)
    await insertScheduleAsStrings(old, new Date(Date.now() - 1_000))

    await store.startScheduling()
    const fired = await eventually(async () => (await read("old-schedule")).events[0])
    await store.stopScheduling()

    expect(fired).toEqual(old.event)
    // The event it became is stored as every appended event is.
    expect(await stored(T.events, "event_id", old.event.identifier)).toEqual(asObjects(old))
  })

  it("reads a snapshot whose metadata is stored as a JSON string", async () => {
    const key = stream("old-snapshot")
    await insertSnapshotAsString(key, { total: 9 }, "stored-type")

    deserializedTypes = []
    const result = await store.source({ query: { tags: { stream: stream("nothing") } }, snapshot: { key } })
    expect(result.snapshot).toEqual({ position: 0n, state: { total: 9 } })
    expect(deserializedTypes).toEqual(["stored-type"])
  })

  it("rewrites rows stored as JSON strings with one UPDATE per table", async () => {
    const oldEvent = event("rewrite", 0)
    const newEvent = event("rewrite", 1)
    const oldSchedule = event("rewrite-schedule", 0)
    const newSchedule = event("rewrite-schedule", 1)
    const later = new Date(Date.now() + 3_600_000)
    await insertEventAsStrings(oldEvent)
    await store.append([newEvent])
    await insertScheduleAsStrings(oldSchedule, later)
    await uow().execute((u) => store.schedule(newSchedule, later, u))
    await insertSnapshotAsString(stream("rewrite-old"), { total: 1 }, "stored-type")
    await store.storeSnapshot(stream("rewrite-new"), { position: 1n, state: { total: 2 } })

    await pool.query(REWRITE_EVENTS)
    await pool.query(REWRITE_SCHEDULED)
    await pool.query(REWRITE_SNAPSHOTS)

    for (const e of [oldEvent, newEvent]) {
      expect(await stored(T.events, "event_id", e.event.identifier)).toEqual(asObjects(e))
    }
    for (const e of [oldSchedule, newSchedule]) {
      expect(await stored(T.scheduled, "schedule_id", e.event.identifier)).toEqual(asObjects(e))
    }
    const snapshots = await pool.query<{ key: string; metadata_type: string; serializer_type: string }>(
      `SELECT key, jsonb_typeof(metadata) AS metadata_type, metadata->>'__kr_serializer_type' AS serializer_type
         FROM ${T.snapshots} WHERE key = ANY($1::text[]) ORDER BY key`,
      [[stream("rewrite-new"), stream("rewrite-old")]],
    )
    expect(snapshots).toEqual([
      { key: stream("rewrite-new"), metadata_type: "object", serializer_type: `json:${stream("rewrite-new")}` },
      { key: stream("rewrite-old"), metadata_type: "object", serializer_type: "stored-type" },
    ])

    // Nothing is left to rewrite, and the rows read as they did.
    const left = await pool.queryOne<{ n: string | number }>(
      `SELECT count(*) AS n FROM ${T.events} WHERE jsonb_typeof(payload) = 'string' OR jsonb_typeof(metadata) = 'string'`,
    )
    expect(Number(left?.n)).toBe(0)
    expect((await read("rewrite")).events).toEqual([oldEvent.event, newEvent.event])
  })
})
