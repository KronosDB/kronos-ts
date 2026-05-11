import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import { pgAdapter } from "../adapters/pg.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"
import { bootstrapSchema, DEFAULT_TABLE_NAMES } from "../schema.js"
import { createPostgresSnapshotStore } from "../postgres-snapshot-store.js"
import type { Snapshot } from "@kronos-ts/eventsourcing"

let pg: RunningPostgres
let adapter: ReturnType<typeof pgAdapter>
let snapshotCallCounts = { serialize: 0, deserialize: 0 }
const COUNTING_SERIALIZER = {
  serialize(x: unknown): Uint8Array {
    snapshotCallCounts.serialize++
    return new TextEncoder().encode(JSON.stringify(x))
  },
  deserialize<T>(b: Uint8Array): T {
    snapshotCallCounts.deserialize++
    return JSON.parse(new TextDecoder().decode(b)) as T
  },
}
let store: ReturnType<typeof createPostgresSnapshotStore>

beforeAll(async () => {
  pg = await startPostgresContainer()
  adapter = pgAdapter({ connectionString: pg.connectionString })
  await adapter.connect()
  await bootstrapSchema(adapter)
  store = createPostgresSnapshotStore({ adapter, serializer: COUNTING_SERIALIZER })
}, 60_000)

afterAll(async () => {
  await adapter.disconnect()
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

describe("createPostgresSnapshotStore", () => {
  it("stores and loads a snapshot roundtrip-stable", async () => {
    await store.store("Order", "1", sampleSnapshot(42n))
    const loaded = await store.load("Order", "1")
    expect(loaded).not.toBeNull()
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

  it("load returns null for missing entity", async () => {
    expect(await store.load("Nope", "x")).toBeNull()
  })

  it("deleteSnapshots removes the snapshot", async () => {
    await store.store("Order", "1", sampleSnapshot(7n))
    await store.deleteSnapshots("Order", "1")
    expect(await store.load("Order", "1")).toBeNull()
  })

  it("composite (entity_name, entity_id) PK keeps different entities separate", async () => {
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
