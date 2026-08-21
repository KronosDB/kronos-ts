import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import { pgAdapter } from "../adapters/pg.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"
import { DEFAULT_TABLE_NAMES } from "../schema.js"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresEventStore } from "../postgres-event-store.js"
import { postgresSnapshottingEventStore } from "../postgres-snapshotting-event-store.js"
import { descriptorBasedTagResolver } from "@kronos-ts/core"
import type { Snapshot } from "@kronos-ts/core"
import type { Serializer, SerializedObject } from "@kronos-ts/core"

let pg: RunningPostgres
let adapter: ReturnType<typeof pgAdapter>
let snapshotCallCounts = { serialize: 0, deserialize: 0 }
const COUNTING_SERIALIZER: Serializer = {
  serialize(x: unknown, type: string, revision: string = ""): SerializedObject {
    snapshotCallCounts.serialize++
    return {
      type,
      revision,
      data: new TextEncoder().encode(JSON.stringify(x)),
    }
  },
  deserialize<T>(data: SerializedObject): T {
    snapshotCallCounts.deserialize++
    return JSON.parse(new TextDecoder().decode(data.data)) as T
  },
  canConvert(): boolean {
    return true
  },
}
let pool: PostgresResource
let store: ReturnType<typeof postgresSnapshottingEventStore<ReturnType<typeof postgresEventStore>>>

beforeAll(async () => {
  pg = await startPostgresContainer()
  adapter = pgAdapter({ connectionString: pg.connectionString })
  pool = postgresPool(adapter)
  await pool.start()
  // The serializer stays an EXPLICIT argument: the payload column is BYTEA and
  // what goes into it is the application's decision, not the store's.
  store = postgresSnapshottingEventStore(
    postgresEventStore(pool, { tagResolver: descriptorBasedTagResolver() }),
    pool,
    { serializer: COUNTING_SERIALIZER },
  )
}, 60_000)

afterAll(async () => {
  await pool.close()
  await pg.stop()
}, 30_000)

beforeEach(async () => {
  await adapter.query(`TRUNCATE TABLE ${DEFAULT_TABLE_NAMES.snapshots}`)
  snapshotCallCounts = { serialize: 0, deserialize: 0 }
})

const sampleSnapshot = (position: bigint): Snapshot => ({
  position,
  state: { kind: "Order", items: [{ sku: "A", qty: 2 }] },
})

/**
 * What is filed under `key`, read the only way there is: a read that ASKS for
 * it. There is no `load` on the capability, because reading a cached fold is
 * not a second call — it is the read you were already making, led by whatever
 * the wrapper found.
 */
async function readCached(key: string): Promise<Snapshot | undefined> {
  const result = await store.source({
    query: { tags: { nothing: "matches-this" } },
    snapshot: { key },
  })
  return result.snapshot
}

describe("postgresSnapshottingEventStore — one opaque key, one column", () => {
  it("stores and loads a cached fold roundtrip-stable", async () => {
    await store.storeSnapshot("order-v1:1", sampleSnapshot(42n))
    const loaded = await readCached("order-v1:1")
    expect(loaded).toBeDefined()
    expect(loaded!.position).toBe(42n)
    expect(loaded!.state).toEqual({ kind: "Order", items: [{ sku: "A", qty: 2 }] })
  })

  it("repeated store(same key) REPLACES via ON CONFLICT DO UPDATE", async () => {
    await store.storeSnapshot("order-v1:1", sampleSnapshot(1n))
    await store.storeSnapshot("order-v1:1", sampleSnapshot(2n))
    const loaded = await readCached("order-v1:1")
    expect(loaded!.position).toBe(2n)
  })

  it("load returns undefined for a key nothing was filed under", async () => {
    expect(await readCached("nope-v1:x")).toBeUndefined()
  })

  it("the single-column PK keeps different keys separate — nothing is parsed", async () => {
    await store.storeSnapshot("order-v1:1", sampleSnapshot(10n))
    await store.storeSnapshot("order-v1:2", sampleSnapshot(20n))
    await store.storeSnapshot("invoice-v1:1", sampleSnapshot(30n))
    expect((await readCached("order-v1:1"))!.position).toBe(10n)
    expect((await readCached("order-v1:2"))!.position).toBe(20n)
    expect((await readCached("invoice-v1:1"))!.position).toBe(30n)
  })

  it("RENAMING THE KEY orphans the old row rather than migrating it", async () => {
    await store.storeSnapshot("order-v1:1", sampleSnapshot(10n))
    // The fold's meaning changed, so the caller changed the key.
    expect(await readCached("order-v2:1")).toBeUndefined()
    // The old row is untouched — unreachable, not converted and not deleted.
    expect((await readCached("order-v1:1"))!.position).toBe(10n)
  })

  it("a key with punctuation, spaces or braces is stored verbatim", async () => {
    // `state()` composes `"<key>:<flattened id>"`, and a flattened composite id
    // is JSON — braces, quotes and colons and all. The column takes it as text.
    const key = 'course-v1:{"courseId":"cs 101","studentId":"s-1"}'
    await store.storeSnapshot(key, sampleSnapshot(7n))
    expect((await readCached(key))!.position).toBe(7n)
  })

  it("uses the injected Serializer for payload roundtrip", async () => {
    await store.storeSnapshot("order-v1:1", sampleSnapshot(1n))
    expect(snapshotCallCounts.serialize).toBeGreaterThan(0)
    await readCached("order-v1:1")
    expect(snapshotCallCounts.deserialize).toBeGreaterThan(0)
  })

  it("position roundtrips as bigint (no precision loss at 2^53+)", async () => {
    const huge = 9_007_199_254_740_993n // 2^53 + 1
    await store.storeSnapshot("order-v1:huge", sampleSnapshot(huge))
    expect((await readCached("order-v1:huge"))!.position).toBe(huge)
  })
})
