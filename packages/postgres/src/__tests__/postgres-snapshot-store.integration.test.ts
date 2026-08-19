import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import { pgAdapter } from "../adapters/pg.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"
import { DEFAULT_TABLE_NAMES } from "../schema.js"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresSnapshotStore } from "../postgres-snapshot-store.js"
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
let store: ReturnType<typeof postgresSnapshotStore>

beforeAll(async () => {
  pg = await startPostgresContainer()
  adapter = pgAdapter({ connectionString: pg.connectionString })
  pool = postgresPool(adapter)
  await pool.start()
  // The serializer stays an EXPLICIT argument: the payload column is BYTEA and
  // what goes into it is the application's decision, not the store's.
  store = postgresSnapshotStore(pool, { serializer: COUNTING_SERIALIZER })
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
  payload: { kind: "Order", items: [{ sku: "A", qty: 2 }] },
  timestamp: 1715472000_000,
  metadata: { causation: "test" },
})

describe("postgresSnapshotStore", () => {
  it("stores and loads a snapshot roundtrip-stable", async () => {
    await store.store("Order", "1", sampleSnapshot(42n))
    const loaded = await store.load("Order", "1")
    expect(loaded).toBeDefined()
    expect(loaded!.position).toBe(42n)
    expect(loaded!.payload).toEqual({ kind: "Order", items: [{ sku: "A", qty: 2 }] })
    expect(loaded!.timestamp).toBe(1715472000_000)
    expect(loaded!.metadata).toEqual({ causation: "test" })
  })

  it("repeated store(same entity) REPLACES via ON CONFLICT DO UPDATE", async () => {
    await store.store("Order", "1", sampleSnapshot(1n))
    await store.store("Order", "1", sampleSnapshot(2n))
    const loaded = await store.load("Order", "1")
    expect(loaded!.position).toBe(2n)
  })

  it("load returns undefined for missing entity", async () => {
    expect(await store.load("Nope", "x")).toBeUndefined()
  })

  it("deleteSnapshots removes the snapshot", async () => {
    await store.store("Order", "1", sampleSnapshot(7n))
    await store.deleteSnapshots("Order", "1")
    expect(await store.load("Order", "1")).toBeUndefined()
  })

  it("composite (state_name, state_id) PK keeps different states separate", async () => {
    await store.store("Order", "1", sampleSnapshot(10n))
    await store.store("Order", "2", sampleSnapshot(20n))
    await store.store("Invoice", "1", sampleSnapshot(30n))
    expect((await store.load("Order", "1"))!.position).toBe(10n)
    expect((await store.load("Order", "2"))!.position).toBe(20n)
    expect((await store.load("Invoice", "1"))!.position).toBe(30n)
  })

  it("uses the injected Serializer for payload roundtrip", async () => {
    await store.store("Order", "1", sampleSnapshot(1n))
    expect(snapshotCallCounts.serialize).toBeGreaterThan(0)
    await store.load("Order", "1")
    expect(snapshotCallCounts.deserialize).toBeGreaterThan(0)
  })

  it("position roundtrips as bigint (no precision loss at 2^53+)", async () => {
    const huge = 9_007_199_254_740_993n // 2^53 + 1
    await store.store("Order", "huge", sampleSnapshot(huge))
    expect((await store.load("Order", "huge"))!.position).toBe(huge)
  })
})
