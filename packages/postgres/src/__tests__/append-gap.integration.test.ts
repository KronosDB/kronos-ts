/**
 * COMMIT-ORDER GAPS AND THE APPEND CONDITION.
 *
 * Postgres hands out `sequence_position` at INSERT and makes the row visible at
 * COMMIT, so two transactions can commit out of position order. A read that
 * runs in between sees the later position and not the earlier one. The append
 * condition only looks ABOVE the read's marker, so the store must never hand
 * out a marker above an event the read could not see:
 *
 * 1. Writers of the same tag serialize — an unconditional append holds the
 *    tags it writes until it commits — so a visible event of a tag is never
 *    above an uncommitted one of that tag.
 * 2. A read that matches nothing is anchored below its own start, not at the
 *    log head: the head can sit above an uncommitted matching event.
 *
 * Each test holds one write transaction open to create the gap, then checks
 * that an append conditioned on a read that missed an event is refused.
 */
import assert from "node:assert/strict"
import { afterAll, beforeAll, beforeEach, describe, it } from "bun:test"
import {
  appendCondition,
  generateIdentifier,
  jsonSerializer,
  sourcingCondition,
  type EventMessage,
  type EventQuery,
} from "@kronos-ts/core"
import { pgAdapter } from "../adapters/pg.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"
import { DEFAULT_TABLE_NAMES } from "../schema.js"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresEventStore } from "../postgres-event-store.js"
import { postgresSnapshottingEventStore } from "../postgres-snapshotting-event-store.js"

let pg: RunningPostgres
let adapter: ReturnType<typeof pgAdapter>
let pool: PostgresResource
let store: ReturnType<typeof postgresEventStore>

function tagged(label: string, tag: { key: string; value: string }): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: { namespace: "gap", name: label },
    tags: [tag],
    payload: {},
    metadata: {},
    timestamp: Date.now(),
    version: "1",
  } as unknown as EventMessage
}

const A = { key: "gap", value: "a" }
const B = { key: "gap", value: "b" }
const onA: EventQuery = { tags: { gap: "a" } }
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const isConflict = (err: unknown) => /append condition violated/i.test((err as Error)?.message ?? "")

beforeAll(async () => {
  pg = await startPostgresContainer()
  adapter = pgAdapter({ connectionString: pg.connectionString })
  pool = postgresPool(adapter)
  await pool.start()
  store = postgresEventStore(pool)
}, 60_000)

afterAll(async () => {
  await pool.close()
  await pg.stop()
}, 30_000)

beforeEach(async () => {
  await adapter.query(`TRUNCATE TABLE ${DEFAULT_TABLE_NAMES.events}, ${DEFAULT_TABLE_NAMES.snapshots} RESTART IDENTITY`)
})

describe("commit-order gaps", () => {
  it("an append holds the tags it writes until it commits, so no read sees past an uncommitted event of that tag", async () => {
    const held = await store.appendEvents([tagged("First", A)])
    const second = store.append([tagged("Second", A)])
    let read
    try {
      await sleep(300)
      read = await store.source(sourcingCondition(onA))
    } finally {
      await held.commit()
    }
    await second
    await assert.rejects(store.append([tagged("Decision", A)], appendCondition(onA, read.marker)), isConflict)
  })

  it("a read that matches nothing is not anchored to the log head", async () => {
    const held = await store.appendEvents([tagged("First", A)])
    let read
    try {
      await store.append([tagged("Elsewhere", B)])
      read = await store.source(sourcingCondition(onA))
    } finally {
      await held.commit()
    }
    assert.equal(read.events.length, 0)
    await assert.rejects(store.append([tagged("Decision", A)], appendCondition(onA, read.marker)), isConflict)
  })

  it("a snapshotted read with nothing after the snapshot is not anchored to the log head", async () => {
    const snapshots = postgresSnapshottingEventStore(store, pool, { serializer: jsonSerializer() })
    const seeded = await store.append([tagged("Seed", A)])
    await snapshots.storeSnapshot("gap-v1:a", { state: { seen: 1 }, position: seeded.position })

    const held = await store.appendEvents([tagged("First", A)])
    let read
    try {
      await store.append([tagged("Elsewhere", B)])
      read = await snapshots.source(sourcingCondition(onA, undefined, { key: "gap-v1:a" }))
    } finally {
      await held.commit()
    }
    assert.equal(read.events.length, 0)
    assert.ok(read.snapshot, "the snapshot was served")
    await assert.rejects(store.append([tagged("Decision", A)], appendCondition(onA, read.marker)), isConflict)
  })
})
