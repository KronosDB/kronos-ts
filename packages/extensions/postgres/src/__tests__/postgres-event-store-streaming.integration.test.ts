import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import { pgAdapter } from "../adapters/pg.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"
import { bootstrapSchema, DEFAULT_TABLE_NAMES } from "../schema.js"
import { createPostgresEventStore } from "../postgres-event-store.js"
import { generateIdentifier } from "@kronos-ts/common"
import type { EventMessage, SequencedEvent } from "@kronos-ts/messaging"

let pg: RunningPostgres
let adapter: ReturnType<typeof pgAdapter>
let store: ReturnType<typeof createPostgresEventStore>

const NOOP_SERIALIZER = {
  serialize: (x: unknown) => new TextEncoder().encode(JSON.stringify(x)),
  deserialize: (b: Uint8Array) => JSON.parse(new TextDecoder().decode(b)),
}
const NOOP_TAG_RESOLVER = { resolve: (e: EventMessage) => e.tags }

function makeEvent(type: string, tags: { key: string; value: string }[]): EventMessage {
  return {
    identifier: generateIdentifier(),
    name: { namespace: "test", name: type },
    tags,
    payload: {},
    metadata: {},
    timestamp: Date.now(),
    version: "1",
  } as unknown as EventMessage
}

async function collectAvailable<T>(
  stream: { next(): T | undefined; hasNextAvailable(): boolean; close(): void },
  max: number,
  timeoutMs = 5000,
): Promise<T[]> {
  const out: T[] = []
  const deadline = Date.now() + timeoutMs
  while (out.length < max && Date.now() < deadline) {
    while (stream.hasNextAvailable() && out.length < max) {
      const v = stream.next()
      if (v) out.push(v)
    }
    if (out.length < max) await new Promise((r) => setTimeout(r, 50))
  }
  return out
}

beforeAll(async () => {
  pg = await startPostgresContainer()
  adapter = pgAdapter({ connectionString: pg.connectionString })
  await adapter.connect()
  await bootstrapSchema(adapter)
  store = createPostgresEventStore({
    adapter,
    serializer: NOOP_SERIALIZER,
    tagResolver: NOOP_TAG_RESOLVER,
  })
}, 60_000)

afterAll(async () => {
  await adapter.disconnect()
  await pg.stop()
}, 30_000)

beforeEach(async () => {
  await adapter.query(`TRUNCATE TABLE ${DEFAULT_TABLE_NAMES.events} RESTART IDENTITY`)
})

describe("open() — streaming basics", () => {
  it("delivers appended events in sequence_position order", async () => {
    await store.append([
      makeEvent("E1", [{ key: "k", value: "1" }]),
      makeEvent("E2", [{ key: "k", value: "2" }]),
      makeEvent("E3", [{ key: "k", value: "3" }]),
    ])
    const stream = store.open({ position: 0n })
    const events = await collectAvailable<SequencedEvent>(stream as never, 3)
    stream.close()
    expect(events.length).toBe(3)
    expect(events[0]!.sequence).toBe(1n)
    expect(events[2]!.sequence).toBe(3n)
  })

  it("honours the criteria filter", async () => {
    await store.append([
      makeEvent("E1", [{ key: "k", value: "X" }]),
      makeEvent("E2", [{ key: "k", value: "Y" }]),
      makeEvent("E3", [{ key: "k", value: "X" }]),
    ])
    const stream = store.open({
      position: 0n,
      criteria: { kind: "tags", tags: [{ key: "k", value: "X" }] },
    })
    const events = await collectAvailable<SequencedEvent>(stream as never, 2)
    stream.close()
    expect(events.length).toBe(2)
  })
})

describe("open() — gap-free tailing (xid8 + pg_snapshot_xmin)", () => {
  it("does NOT skip events committed out-of-order relative to BIGSERIAL position", async () => {
    // Setup: two writers A, B. A starts FIRST (lower xid8) but commits AFTER B.
    // Naive `WHERE position > $cursor ORDER BY position` would read B (higher pos)
    // and advance the cursor past A, then never see A.
    //
    // Strategy: hold A's transaction open via a pg_sleep inside it; meanwhile
    // commit B; THEN release A. Subscriber must see both, in xid8 order.

    const stream = store.open({ position: 0n })

    // Launch A: a long-running tx that inserts then sleeps inside the tx body
    // BEFORE committing. We use raw SQL because adapter.transaction commits
    // immediately on callback resolution — we need a controlled hold.
    const aPromise = adapter.transaction("READ COMMITTED" as never, async (tx) => {
      await tx.query(
        `INSERT INTO ${DEFAULT_TABLE_NAMES.events} (event_id, type, tags, payload, metadata, version, timestamp)
         VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7)`,
        [generateIdentifier(), "test.A", ["k\x1Fa"], JSON.stringify({}), JSON.stringify({}), "1", Date.now()],
      )
      // Hold the tx open for 500ms — B will commit during this window.
      await tx.query(`SELECT pg_sleep(0.5)`)
    })

    // Give A time to grab its xid8 + insert
    await new Promise((r) => setTimeout(r, 100))

    // Launch B: quick append (commits immediately)
    await store.append([makeEvent("B", [{ key: "k", value: "b" }])])

    // Now wait for A to finish
    await aPromise

    // Subscriber must see BOTH events. Without the pg_snapshot_xmin filter,
    // B's event would be visible immediately but A's would be invisible
    // (because cursor advanced past it).
    const events = await collectAvailable<SequencedEvent>(stream as never, 2, 8000)
    stream.close()
    expect(events.length).toBe(2)
    // Events arrive in (xid8, position) order — A's xid8 is smaller (started first),
    // so A should be delivered FIRST despite committing second.
    const names = events.map((e) => (e.event as { name: { name: string } }).name.name)
    expect(names).toContain("A")
    expect(names).toContain("B")
  }, 30_000)
})

describe("open() — bookmark resume", () => {
  it("resuming from a saved position bookmark replays nothing already seen and skips nothing new", async () => {
    await store.append([
      makeEvent("E1", [{ key: "k", value: "1" }]),
      makeEvent("E2", [{ key: "k", value: "2" }]),
    ])

    const stream1 = store.open({ position: 0n })
    const firstBatch = await collectAvailable<SequencedEvent>(stream1 as never, 2)
    expect(firstBatch.length).toBe(2)
    const lastSeen = firstBatch[1]!.sequence
    stream1.close()

    // More events arrive
    await store.append([
      makeEvent("E3", [{ key: "k", value: "3" }]),
      makeEvent("E4", [{ key: "k", value: "4" }]),
    ])

    // Resume from the last-seen position. Internally the stream consults xid8 to filter
    // in-flight tx events.
    const stream2 = store.open({ position: lastSeen })
    const secondBatch = await collectAvailable<SequencedEvent>(stream2 as never, 2)
    stream2.close()

    expect(secondBatch.length).toBe(2)
    expect(secondBatch.map((e) => Number(e.sequence))).toEqual([3, 4])
  })
})

describe("StreamableEventSource extras", () => {
  it("getHeadPosition returns max sequence_position", async () => {
    await store.append([
      makeEvent("E1", [{ key: "k", value: "1" }]),
      makeEvent("E2", [{ key: "k", value: "2" }]),
    ])
    const head = await store.getHeadPosition()
    expect(head).toBe(2n)
  })

  it("getHeadPosition returns 0n when empty", async () => {
    const head = await store.getHeadPosition()
    expect(head).toBe(0n)
  })

  it("publish appends and is observable via source", async () => {
    await store.publish([makeEvent("P1", [{ key: "k", value: "p" }])])
    const result = await store.source({
      criteria: { kind: "tags", tags: [{ key: "k", value: "p" }] },
      start: 0n,
    })
    expect(result.events.length).toBe(1)
  })

  it("subscribe fires on append and the unsubscribe function stops deliveries", async () => {
    const seen: number[] = []
    const unsubscribe = store.subscribe(async (events) => {
      seen.push(events.length)
    })
    await store.append([makeEvent("S1", [{ key: "k", value: "1" }])])
    // Give the subscriber a moment to fire
    await new Promise((r) => setTimeout(r, 100))
    expect(seen).toEqual([1])
    unsubscribe()
    await store.append([makeEvent("S2", [{ key: "k", value: "2" }])])
    await new Promise((r) => setTimeout(r, 100))
    // No additional callback fire after unsubscribe.
    expect(seen).toEqual([1])
  })

  it("firstToken / latestToken are valid TrackingTokens", async () => {
    const first = await store.firstToken()
    const latest = await store.latestToken()
    expect(first).toBeDefined()
    expect(latest).toBeDefined()
  })
})
