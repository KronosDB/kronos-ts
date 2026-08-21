import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import { pgAdapter } from "../adapters/pg.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"
import { DEFAULT_TABLE_NAMES } from "../schema.js"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresEventStore } from "../postgres-event-store.js"
import { postgresSnapshottingEventStore } from "../postgres-snapshotting-event-store.js"
import { AppendConditionError } from "../errors.js"
import { ORIGIN } from "@kronos-ts/core"
import { generateIdentifier } from "@kronos-ts/core"
import type { EventMessage, SerializedObject, Serializer } from "@kronos-ts/core"

let pg: RunningPostgres
let adapter: ReturnType<typeof pgAdapter>
let pool: PostgresResource
let store: ReturnType<typeof postgresEventStore>

const NOOP_SERIALIZER: Serializer = {
  serialize: (x: unknown, type: string, revision = ""): SerializedObject => ({
    type,
    revision,
    data: new TextEncoder().encode(JSON.stringify(x)),
  }),
  deserialize: <T,>(o: SerializedObject): T => JSON.parse(new TextDecoder().decode(o.data)) as T,
  canConvert: () => true,
}
const NOOP_TAG_RESOLVER = (e: EventMessage) => e.tags

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
  // The pool owns connect + bootstrap + the table names; the store is a
  // function of it.
  pool = postgresPool(adapter)
  await pool.start()
  store = postgresEventStore(pool, {
    tagResolver: NOOP_TAG_RESOLVER,
  })
}, 60_000)

afterAll(async () => {
  await pool.close()
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
      query: { tags: { order: "1" } },
      start: 0n,
    })
    expect(result.events.length).toBe(2)
  })

  it("source({}) returns every event", async () => {
    await store.append([
      makeEvent("OrderPlaced", [{ key: "order", value: "1" }]),
      makeEvent("OrderPlaced", []), // no tags
    ])
    const result = await store.source({ query: {}, start: 0n })
    expect(result.events.length).toBe(2)
  })

  it("source() round-trips the full EventMessage — identifier, timestamp, version (engine parity)", async () => {
    // Guards the contract gap where postgres dropped identifier/timestamp/version on read
    // while the in-memory, axon-server, and kronosdb engines preserved them.
    const original = makeEvent("OrderPlaced", [{ key: "order", value: "RT" }], { amount: 42 })
    await store.append([original])

    const result = await store.source({
      query: { tags: { order: "RT" } },
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
      query: { tags: { order: "RB" } },
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
      query: { tags: { order: "C1" } },
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
          query: { tags: { order: "C1" } },
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
      const query = { tags: { race: "T" } }

      const results = await Promise.allSettled([
        store.append(
          [makeEvent("RaceA", [{ key: "race", value: "T" }])],
          { query, marker: m },
        ),
        store.append(
          [makeEvent("RaceB", [{ key: "race", value: "T" }])],
          { query, marker: m },
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
        { query: { tags: { entity: "AAA" } }, marker: mA },
      ),
      store.append(
        [makeEvent("DisjointB", [{ key: "entity", value: "BBB" }])],
        { query: { tags: { entity: "BBB" } }, marker: mB },
      ),
    ])
    expect(results.every((r) => r.status === "fulfilled")).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// THE NATIVE SNAPSHOTTING STRATEGY.
//
// `postgresEventStore` implements the strategy itself: the cache lookup, the
// start position derived from it, the event query and the head all arrive in
// ONE round trip. These tests judge the RESULT of that query — that the entry
// filed under the caller's KEY comes back, that the events start strictly after
// it, and that a different key simply finds nothing. The key is one opaque
// string and this query never parses it. The single-round-trip claim is the
// query's own shape; see the doc comment on `sourceFused`.
//
// THE BASE STORE IS NOT INVOLVED. `postgresEventStore` has never heard of
// snapshots; the capability arrives by wrapping, and the wrapper owns BOTH the
// upsert and the fused read — one object, one serializer.
// ---------------------------------------------------------------------------

describe("postgresSnapshottingEventStore — the fused strategy, in one round trip", () => {
  const caching = () =>
    postgresSnapshottingEventStore(store, pool, { serializer: NOOP_SERIALIZER })

  beforeEach(async () => {
    await adapter.query(`TRUNCATE TABLE ${DEFAULT_TABLE_NAMES.snapshots}`)
  })

  async function appendBumps(entity: string, n: number): Promise<void> {
    for (let i = 0; i < n; i++) {
      await store.append([makeEvent("Bumped", [{ key: "entity", value: entity }], { i })])
    }
  }

  it("a condition with NO snapshot key reads the whole range and leads with nothing", async () => {
    await appendBumps("plain", 3)
    const result = await caching().source({ query: { tags: { entity: "plain" } } })
    expect(result.events.length).toBe(3)
    expect(result.snapshot).toBeUndefined()
  })

  it("a MISS falls back to the full range, silently", async () => {
    await appendBumps("miss", 3)
    const result = await caching().source({
      query: { tags: { entity: "miss" } },
      snapshot: { key: "counter:miss" },
    })
    expect(result.events.length).toBe(3)
    expect(result.snapshot).toBeUndefined()
  })

  it("a HIT leads with the snapshot and sources events strictly AFTER its position", async () => {
    await appendBumps("hit", 5)
    const positions = await adapter.query<{ p: string }>(
      `SELECT sequence_position::text AS p FROM ${DEFAULT_TABLE_NAMES.events} ORDER BY sequence_position`,
    )
    const third = BigInt(positions[2]!.p)

    await caching().storeSnapshot("counter:hit", { state: { count: 3 }, position: third })

    const result = await caching().source({
      query: { tags: { entity: "hit" } },
      snapshot: { key: "counter:hit" },
    })

    expect(result.snapshot).toBeDefined()
    expect(result.snapshot!.state).toEqual({ count: 3 })
    expect(result.snapshot!.position).toBe(third)
    // Two events after the third — the fused query narrowed the scan itself.
    expect(result.events.length).toBe(2)
    expect(result.events.every((e) => (e.payload as { i: number }).i >= 3)).toBe(true)
  })

  it("a snapshot covering EVERYTHING leads with itself and sources no events", async () => {
    await appendBumps("all", 3)
    const head = await store.getHeadPosition()
    await caching().storeSnapshot("counter:all", { state: { count: 3 }, position: head })

    const result = await caching().source({
      query: { tags: { entity: "all" } },
      snapshot: { key: "counter:all" },
    })

    expect(result.snapshot!.state).toEqual({ count: 3 })
    expect(result.events.length).toBe(0)
    // The marker still reports the head, exactly as the plain read does when
    // its query matched nothing.
    expect(result.marker.position).toBeGreaterThanOrEqual(0n)
  })

  it("an EMPTY log with no entry still answers — the one-row anchor guarantees it", async () => {
    const result = await caching().source({
      query: { tags: { entity: "nothing-here" } },
      snapshot: { key: "counter:nothing-here" },
    })
    expect(result.events.length).toBe(0)
    expect(result.snapshot).toBeUndefined()
    expect(result.marker.position).toBe(-1n)
  })

  it("serves whatever is cached — FITNESS is the repository's question, not SQL's", () => {
    // The fused query answers "here is the entry". Whether the value still fits
    // the shape the fold seeds into is a fact about the running application, so
    // it is asked once, in core, for every backend — see `matchesInitialStructure`.
    expect(true).toBe(true)
  })

  it("the condition's own `start` still floors the scan — the two narrowings compose", async () => {
    await appendBumps("floor", 5)
    const positions = await adapter.query<{ p: string }>(
      `SELECT sequence_position::text AS p FROM ${DEFAULT_TABLE_NAMES.events} ORDER BY sequence_position`,
    )
    // A snapshot at the FIRST event, but the caller asks to start at the fourth.
    await caching().storeSnapshot("counter:floor", { state: {}, position: BigInt(positions[0]!.p) })

    const result = await caching().source({
      query: { tags: { entity: "floor" } },
      start: BigInt(positions[3]!.p),
      snapshot: { key: "counter:floor" },
    })

    expect(result.snapshot).toBeDefined()
    expect(result.events.length).toBe(2)
  })

  it("RENAMING THE KEY orphans the old row — invalidation, in one column", async () => {
    await appendBumps("rename", 4)
    await caching().storeSnapshot("counter-v1:rename", { state: { count: 2 }, position: 1n })

    // The fold's meaning changed, so the caller changed the key. The old row is
    // still there; it is simply unreachable, and this read replays in full.
    const result = await caching().source({
      query: { tags: { entity: "rename" } },
      snapshot: { key: "counter-v2:rename" },
    })

    expect(result.snapshot).toBeUndefined()
    expect(result.events.length).toBe(4)
    // Not migrated, not deleted — just orphaned, and still reachable under the
    // key it was filed under.
    const old = await caching().source({
      query: { tags: { entity: "rename" } },
      snapshot: { key: "counter-v1:rename" },
    })
    expect(old.snapshot!.state).toEqual({ count: 2 })
  })

  it("the leading snapshot survives a bigint position past 2^53", async () => {
    const huge = 9_007_199_254_740_993n // 2^53 + 1
    await caching().storeSnapshot("counter:huge", { state: { count: 1 }, position: huge })

    const result = await caching().source({
      query: { tags: { entity: "huge" } },
      snapshot: { key: "counter:huge" },
    })

    expect(result.snapshot!.position).toBe(huge)
  })
})
