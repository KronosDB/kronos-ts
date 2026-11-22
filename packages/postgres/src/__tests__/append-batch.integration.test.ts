/**
 * AN APPEND OF MANY EVENTS.
 *
 * The events of one append are inserted with one statement per 100 of them.
 * These tests hold that to what a one-event append guarantees: positions in
 * the order the events were given, the marker at the last of them, the same
 * stored rows, and nothing inserted when the append is refused. They run over
 * all three drivers, because each binds the statement's parameters itself.
 */
import assert from "node:assert/strict"
import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import {
  appendCondition,
  generateIdentifier,
  isConflict,
  sourcingCondition,
  type EventMessage,
} from "@kronos-ts/core"
import type { PostgresAdapter, PostgresAdapterTransaction, QueryRow } from "../adapter.js"
import { bunSqlAdapter } from "../adapters/bun-sql.js"
import { pgAdapter } from "../adapters/pg.js"
import { postgresAdapter } from "../adapters/postgres.js"
import { postgresEventStore } from "../postgres-event-store.js"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"

let pg: RunningPostgres

beforeAll(async () => {
  pg = await startPostgresContainer()
}, 60_000)

afterAll(async () => {
  await pg.stop()
}, 30_000)

function event(stream: string, n: number, extra: Partial<EventMessage> = {}): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: { namespace: "batch", name: "Happened" },
    tags: [{ key: "stream", value: stream }],
    payload: { n },
    metadata: {},
    timestamp: 1_700_000_000_000 + n,
    version: "1",
    ...extra,
  } as EventMessage
}

/** `inner`, recording the text of every INSERT into the events table. */
function recordingInserts(inner: PostgresAdapter): { adapter: PostgresAdapter; inserts: string[] } {
  const inserts: string[] = []
  const adapter: PostgresAdapter = {
    ...inner,
    transaction: (isolationLevel, fn) =>
      inner.transaction(isolationLevel, (tx) => {
        const recorded: PostgresAdapterTransaction = {
          unwrap: <T = unknown>() => tx.unwrap<T>(),
          query<R extends QueryRow = QueryRow>(sql: string, params?: unknown[]): Promise<R[]> {
            if (/INSERT INTO kronos_events/.test(sql)) inserts.push(sql)
            return tx.query<R>(sql, params)
          },
        }
        return fn(recorded)
      }),
  }
  return { adapter, inserts }
}

const drivers: ReadonlyArray<[string, () => PostgresAdapter]> = [
  ["pgAdapter", () => pgAdapter({ connectionString: pg.connectionString })],
  ["postgresAdapter", () => postgresAdapter({ connectionString: pg.connectionString })],
  ["bunSqlAdapter", () => bunSqlAdapter({ connectionString: pg.connectionString })],
]

describe.each(drivers)("an append of many events over %s", (driver, make) => {
  let pool: PostgresResource
  let inserts: string[]
  let store: ReturnType<typeof postgresEventStore>
  const stream = (name: string) => `${driver}-${name}`

  beforeAll(async () => {
    const recording = recordingInserts(make())
    inserts = recording.inserts
    pool = postgresPool(recording.adapter)
    await pool.start()
    store = postgresEventStore(pool)
  }, 60_000)

  afterAll(async () => {
    await pool.close()
  }, 30_000)

  async function countOf(name: string): Promise<number> {
    const read = await store.source(sourcingCondition({ tags: { stream: stream(name) } }))
    return read.events.length
  }

  it("inserts the events with one statement per 100", async () => {
    inserts.length = 0
    await store.append(Array.from({ length: 250 }, (_, n) => event(stream("statements"), n)))
    expect(inserts).toHaveLength(3)

    inserts.length = 0
    await store.append([event(stream("statements"), 250)])
    expect(inserts).toHaveLength(1)
    expect(await countOf("statements")).toBe(251)
  })

  it("hands out positions in the order the events were given, beside other writers", async () => {
    // Other writers draw positions from the same sequence while this runs.
    let appending = true
    const others = Array.from({ length: 4 }, async (_, writer) => {
      for (let round = 0; appending; round++) {
        await store.append([event(stream(`other-${writer}-${round}`), 0), event(stream(`other-${writer}-${round}`), 1)])
      }
    })
    const given = Array.from({ length: 1_050 }, (_, n) => event(stream("order"), n))
    let marker
    try {
      marker = await store.append(given)
    } finally {
      appending = false
      await Promise.all(others)
    }

    const read = await store.source(sourcingCondition({ tags: { stream: stream("order") } }))
    expect(read.events.map((e) => e.identifier)).toEqual(given.map((e) => e.identifier))
    // The marker is the last event's position: the read ends on the same one.
    expect(read.marker.position).toBe(marker.position)
  }, 60_000)

  it("stores each event as a one-event append does", async () => {
    const awkward = (n: number) =>
      event(stream("rows"), n, {
        name: { namespace: "batch", name: `Kind${n}` },
        tags: [
          { key: "stream", value: stream("rows") },
          { key: "note", value: `quote " backslash \\ brace } comma , ${n}` },
        ],
        payload: { n, text: `it's "quoted" \\ {braced}, with a\nnewline and ünïcödé`, nested: { list: [1, null, "NULL"] } },
        metadata: { correlationId: `c-${n}`, empty: "" },
        version: `${n}.0`,
      })
    const untagged = (n: number) => event(stream("rows"), n, { tags: [{ key: "stream", value: stream("rows") }], payload: {} })
    const alone = awkward(0)
    const together = [awkward(1), untagged(2), awkward(3)]

    await store.append([alone])
    await store.append(together)

    const read = await store.source(sourcingCondition({ tags: { stream: stream("rows") } }))
    expect(read.events).toEqual([alone, ...together])
  })

  it("inserts none of the events when the append condition is violated", async () => {
    const query = { tags: { stream: stream("refused") } }
    const before = await store.source(sourcingCondition(query))
    await store.append([event(stream("refused"), 0)])

    const refused = Array.from({ length: 150 }, (_, n) => event(stream("refused"), n + 1))
    await assert.rejects(store.append(refused, appendCondition(query, before.marker)), isConflict)
    expect(await countOf("refused")).toBe(1)
  })

  it("inserts none of the events when a later statement of the append fails", async () => {
    const existing = event(stream("duplicate"), 0)
    await store.append([existing])

    // The 150th event repeats an id already in the log, so the second
    // statement fails after the first inserted 100 rows.
    const batch = Array.from({ length: 200 }, (_, n) => event(stream("duplicate"), n + 1))
    batch[149] = { ...batch[149]!, identifier: existing.identifier }
    await assert.rejects(store.append(batch), isConflict)
    expect(await countOf("duplicate")).toBe(1)
  })
})
