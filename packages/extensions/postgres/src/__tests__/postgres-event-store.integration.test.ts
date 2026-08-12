import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import { pgAdapter } from "../adapters/pg.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"
import { bootstrapSchema, DEFAULT_TABLE_NAMES } from "../schema.js"
import { postgresEventStore } from "../postgres-event-store.js"
import { AppendConditionError } from "../errors.js"
import { ORIGIN } from "@kronos-ts/eventsourcing"
import { generateIdentifier } from "@kronos-ts/common"
import type { EventMessage } from "@kronos-ts/messaging"

let pg: RunningPostgres
let adapter: ReturnType<typeof pgAdapter>
let store: ReturnType<typeof postgresEventStore>

const NOOP_SERIALIZER = {
  serialize: (x: unknown) => new TextEncoder().encode(JSON.stringify(x)),
  deserialize: (bytes: Uint8Array) => JSON.parse(new TextDecoder().decode(bytes)),
}
const NOOP_TAG_RESOLVER = { resolve: (e: EventMessage) => e.tags }

function makeEvent(type: string, tags: { key: string; value: string }[], payload: unknown = {}): EventMessage {
  // Note: QualifiedName uses 'name' (not 'localName') per packages/common/src/qualified-name.ts
  return {
    kind: "event",
    identifier: generateIdentifier(), // UUID v7 per quick 260511-mks
    name: { namespace: "test", name: type },
    tags,
    payload,
    metadata: {},
    timestamp: Date.now(),
    version: "1",
  }
}

beforeAll(async () => {
  pg = await startPostgresContainer()
  adapter = pgAdapter({ connectionString: pg.connectionString })
  await adapter.connect()
  await bootstrapSchema(adapter)
  store = postgresEventStore({
    adapter,
    serializer: NOOP_SERIALIZER,
    tagResolver: NOOP_TAG_RESOLVER,
    tableNames: DEFAULT_TABLE_NAMES,
  })
}, 60_000)

afterAll(async () => {
  await adapter.disconnect()
  await pg.stop()
}, 30_000)

beforeEach(async () => {
  // Clean slate between tests so positions are predictable.
  await adapter.query(`TRUNCATE TABLE ${DEFAULT_TABLE_NAMES.events} RESTART IDENTITY`)
})

describe("postgresEventStore — shape", () => {
  it("exposes source / appendEvents / append", () => {
    expect(typeof store.source).toBe("function")
    expect(typeof store.appendEvents).toBe("function")
    expect(typeof store.append).toBe("function")
  })
})

describe("append + source", () => {
  it("append([e1, e2]) returns a ConsistencyMarker whose position is the LAST inserted sequence_position", async () => {
    const marker = await store.append([
      makeEvent("OrderPlaced", [{ key: "order", value: "1" }]),
      makeEvent("OrderPlaced", [{ key: "order", value: "2" }]),
    ])
    // BIGSERIAL starts at 1; two inserts → positions 1, 2; marker = 2n.
    expect(marker.position).toBe(2n)
  })

  it("source({tags: [{order: 1}]}) returns only events containing the order:1 tag (contains-all)", async () => {
    await store.append([
      makeEvent("OrderPlaced", [{ key: "order", value: "1" }]),
      makeEvent("OrderPlaced", [{ key: "order", value: "2" }]),
      makeEvent("OrderPlaced", [{ key: "order", value: "1" }, { key: "customer", value: "X" }]),
    ])
    const result = await store.source({
      criteria: { kind: "tags", tags: [{ key: "order", value: "1" }] },
      start: 0n,
    })
    expect(result.events.length).toBe(2)
  })

  it("source({any-tag}) returns all tagged events", async () => {
    await store.append([
      makeEvent("OrderPlaced", [{ key: "order", value: "1" }]),
      makeEvent("OrderPlaced", []), // no tags
    ])
    const result = await store.source({ criteria: { kind: "any-tag" }, start: 0n })
    expect(result.events.length).toBe(1)
  })

  it("source() round-trips the full EventMessage — identifier, timestamp, version (engine parity)", async () => {
    // Guards the contract gap where postgres dropped identifier/timestamp/version on read
    // while the in-memory, axon-server, and kronosdb engines preserved them.
    const original = makeEvent("OrderPlaced", [{ key: "order", value: "RT" }], { amount: 42 })
    await store.append([original])

    const result = await store.source({
      criteria: { kind: "tags", tags: [{ key: "order", value: "RT" }] },
      start: 0n,
    })
    expect(result.events.length).toBe(1)
    const sourced = result.events[0]!
    expect(sourced.kind).toBe("event")
    expect(sourced.identifier).toBe(original.identifier)
    expect(sourced.timestamp).toBe(original.timestamp)
    expect(sourced.version).toBe(original.version)
    expect(sourced.payload).toEqual(original.payload)
    expect(sourced.name).toEqual(original.name)
  })
})

describe("appendEvents — two-phase transaction", () => {
  it("rollback() is synchronous void and discards staged events", async () => {
    const txn = await store.appendEvents([makeEvent("OrderPlaced", [{ key: "order", value: "RB" }])])
    const rollbackResult = txn.rollback()
    // Verifies SYNCHRONOUS void contract — no Promise returned.
    expect(rollbackResult).toBeUndefined()
    // Event must NOT be visible (uncommitted + rolled back).
    const result = await store.source({
      criteria: { kind: "tags", tags: [{ key: "order", value: "RB" }] },
      start: 0n,
    })
    expect(result.events.length).toBe(0)
  })

  it("commit() then afterCommit() yields the ConsistencyMarker", async () => {
    const txn = await store.appendEvents([
      makeEvent("OrderPlaced", [{ key: "order", value: "CM" }]),
    ])
    await txn.commit()
    const marker = await txn.afterCommit()
    expect(typeof marker.position).toBe("bigint")
    expect(marker.position).toBeGreaterThanOrEqual(1n)
  })
})

describe("DCB conflict detection", () => {
  it("throws AppendConditionError when a matching event was inserted after the marker", async () => {
    // 1. Initial state: source the entity → marker = ORIGIN
    const initial = await store.source({
      criteria: { kind: "tags", tags: [{ key: "order", value: "C1" }] },
      start: 0n,
    })
    const markerAtRead = initial.marker
    // 2. Concurrent writer inserts a conflicting event
    await store.append([makeEvent("OrderPlaced", [{ key: "order", value: "C1" }])])
    // 3. Our append with the stale marker MUST reject
    let caught: Error | undefined
    try {
      await store.append(
        [makeEvent("OrderShipped", [{ key: "order", value: "C1" }])],
        {
          criteria: { kind: "tags", tags: [{ key: "order", value: "C1" }] },
          marker: markerAtRead,
        },
      )
    } catch (e) {
      caught = e as Error
    }
    expect(caught).toBeDefined()
    expect(caught).toBeInstanceOf(AppendConditionError)
  })
})

describe("Concurrency — advisory-lock taxonomy", () => {
  it("two concurrent SAME-TAG writers race — exactly one wins (10 trials)", async () => {
    for (let trial = 0; trial < 10; trial++) {
      await adapter.query(`TRUNCATE TABLE ${DEFAULT_TABLE_NAMES.events} RESTART IDENTITY`)
      const m = ORIGIN
      const criteria = { kind: "tags" as const, tags: [{ key: "race", value: "T" }] }

      const results = await Promise.allSettled([
        store.append(
          [makeEvent("RaceA", [{ key: "race", value: "T" }])],
          { criteria, marker: m },
        ),
        store.append(
          [makeEvent("RaceB", [{ key: "race", value: "T" }])],
          { criteria, marker: m },
        ),
      ])
      const fulfilled = results.filter((r) => r.status === "fulfilled")
      const rejected = results.filter((r) => r.status === "rejected")
      expect(fulfilled.length).toBe(1)
      expect(rejected.length).toBe(1)
      expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(AppendConditionError)
    }
  }, 60_000)

  it("two concurrent DISJOINT-TAG writers BOTH succeed (lock taxonomy permits parallelism)", async () => {
    const mA = ORIGIN
    const mB = ORIGIN
    const results = await Promise.allSettled([
      store.append(
        [makeEvent("DisjointA", [{ key: "entity", value: "AAA" }])],
        { criteria: { kind: "tags", tags: [{ key: "entity", value: "AAA" }] }, marker: mA },
      ),
      store.append(
        [makeEvent("DisjointB", [{ key: "entity", value: "BBB" }])],
        { criteria: { kind: "tags", tags: [{ key: "entity", value: "BBB" }] }, marker: mB },
      ),
    ])
    expect(results.every((r) => r.status === "fulfilled")).toBe(true)
  })
})
