/**
 * Concurrent-commit-gap regression test.
 *
 * Spirit-port of kraken-tech's concurrentCommitGap.tests.ts (per D-12.16 —
 * the one place a verbatim test port is genuinely useful, because the
 * gap-bug failure mode is subtle and a known-good test catches it directly).
 *
 * Scenario:
 *   1. Writer W1 BEGINs a transaction; its xid8 is assigned (X1).
 *   2. W1 INSERTs an event but does NOT commit yet.
 *   3. Writer W2 starts, INSERTs an event, and COMMITs. W2's xid8 is X2 > X1.
 *   4. A subscriber is tailing via open({ position: 0n }).
 *   5. WITHOUT the xid8 watermark, the subscriber sees W2's event (higher
 *      position), advances its cursor PAST W1's event, and NEVER sees W1.
 *   6. WITH the `transaction_id < pg_snapshot_xmin(pg_current_snapshot())`
 *      filter, W1's event is HIDDEN until W1 commits — at which point both
 *      events become visible and the subscriber delivers them in xid8 order.
 *
 * Run as 20 trials to catch flakiness — if the watermark is wrong, a single
 * trial might pass by luck (the held-tx timing is racy) but 20 trials reliably
 * exposes the gap.
 */

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

const HOLD_MS = 400
const TRIALS = 20

function makeEvent(label: string): EventMessage {
  return {
    identifier: generateIdentifier(),
    name: { namespace: "test", name: label },
    tags: [{ key: "label", value: label }],
    payload: {},
    metadata: {},
    timestamp: Date.now(),
    version: "1",
  } as unknown as EventMessage
}

async function collectAtLeast<T>(
  stream: { next(): T | undefined; hasNextAvailable(): boolean },
  n: number,
  timeoutMs: number,
): Promise<T[]> {
  const out: T[] = []
  const deadline = Date.now() + timeoutMs
  while (out.length < n && Date.now() < deadline) {
    while (stream.hasNextAvailable() && out.length < n) {
      const v = stream.next()
      if (v) out.push(v)
    }
    if (out.length < n) await new Promise((r) => setTimeout(r, 25))
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

describe("Concurrent-commit-gap regression (kraken-tech scenario)", () => {
  it(`subscriber sees BOTH events when the smaller-xid8 writer commits second (${TRIALS} trials)`, async () => {
    for (let trial = 0; trial < TRIALS; trial++) {
      // Start the stream BEFORE inserting anything — simulates a live tail subscriber.
      const stream = store.open({ position: 0n })

      // W1: long-running transaction. It inserts an event but holds the
      // transaction open via a pg_sleep BEFORE committing. Its xid8 (X1) is
      // assigned at the first statement and remains active until commit.
      //
      // A naive subscriber polling `WHERE sequence_position > $cursor` during
      // the hold window would see W2's committed row (higher position) and
      // advance the cursor past W1, permanently losing W1's event.
      //
      // The xid8 + pg_snapshot_xmin watermark prevents this: W1's row has
      // transaction_id = X1 which is still in the active snapshot, so the
      // watermark filter hides it. Only after W1 commits does X1 drop below
      // xmin, making both rows visible at once.
      const w1 = adapter.transaction("READ COMMITTED" as never, async (tx) => {
        await tx.query(
          `INSERT INTO ${DEFAULT_TABLE_NAMES.events} (event_id, type, tags, payload, metadata)
           VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)`,
          [generateIdentifier(), "test.W1", ["label\x1FW1"], JSON.stringify({}), JSON.stringify({})],
        )
        // Hold the tx open — W2 will commit during this sleep.
        await tx.query(`SELECT pg_sleep(${HOLD_MS / 1000})`)
      })

      // Give W1 time to grab its xid8 + insert before W2 races.
      await new Promise((r) => setTimeout(r, 50))

      // W2: quick commit. Its xid8 > W1's xid8 (started after W1).
      await store.append([makeEvent("W2")])

      // Wait for W1 to finish (commit must land before we collect).
      await w1

      // Collect at least 2 events with a 5s deadline. Without the xid8
      // watermark, a naive subscriber would time out here with only 1 event
      // (W2 only — W1 was "skipped" past before it committed).
      const events = await collectAtLeast<SequencedEvent>(stream as never, 2, 5000)
      stream.close()

      const labels = events.map(
        (e) => (e.event as { name: { name: string } }).name.name,
      )

      expect(labels, `trial ${trial}: expected both W1 and W2`).toContain("W1")
      expect(labels, `trial ${trial}: expected both W1 and W2`).toContain("W2")

      // Bonus assertion: W1 (smaller xid8, lower BIGSERIAL despite committing later)
      // must be delivered BEFORE W2 in the xid8-ordered stream. This verifies that
      // the (transaction_id, sequence_position) sort preserves causal ordering.
      expect(
        labels.indexOf("W1"),
        `trial ${trial}: W1 (smaller xid8) must arrive before W2`,
      ).toBeLessThan(labels.indexOf("W2"))

      // Clean up for next trial.
      await adapter.query(`TRUNCATE TABLE ${DEFAULT_TABLE_NAMES.events} RESTART IDENTITY`)
    }
  }, 120_000)
})
