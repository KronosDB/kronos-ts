/**
 * The KronosDB event store over the native protocol, against a real server.
 *
 * What the unit tests cannot say: that the bytes this client writes are the
 * bytes the server reads, and the other way round. Everything here goes through
 * the package's public functions — the same ones a host calls — and checks what
 * comes back, not what was sent.
 *
 * Needs a server that speaks the native protocol: KRONOSDB_SERVER_BIN or
 * KRONOSDB_IMAGE (see ../kronosdb-server.ts).
 */
import assert from "node:assert/strict"
import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { emptyMetadata, generateIdentifier, jsonSerializer, qn, qualifiedNameToString, type EventMessage } from "@kronos-ts/core"
import {
  ConcurrencyError,
  KronosDbRequestError,
  kronosDbConnection,
  kronosDbEventStore,
  kronosDbPositionAt,
  kronosDbSubscription,
  kronosDbTagsAt,
  kronosDbTail,
  type KronosDbConnectionHandle,
} from "@kronos-ts/kronosdb"
import { startKronosDb, type KronosDbServer } from "../kronosdb-server.js"

const run = generateIdentifier().slice(0, 8)

function fact(name: string, payload: Record<string, unknown>, tags: Array<[string, string]>, metadata: Record<string, unknown> = {}): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: qn("native-it", name),
    version: "2.1",
    payload,
    metadata,
    timestamp: Date.now(),
    tags: tags.map(([key, value]) => ({ key, value })),
  }
}

async function waitFor(probe: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (probe()) return
    await new Promise((r) => setTimeout(r, 10))
  }
  throw new Error("waitFor: never became true")
}

describe("KronosDB event store — native protocol", () => {
  let server: KronosDbServer
  let kdb: KronosDbConnectionHandle

  beforeAll(async () => {
    server = await startKronosDb()
    kdb = await kronosDbConnection({
      componentName: "native-it",
      host: server.host,
      port: server.port,
      serializer: jsonSerializer(),
    })
  }, 120_000)

  afterAll(async () => {
    await kdb?.close()
    await server?.stop()
  })

  it("appends and reads back an event whole — fields, metadata, tags and payload", async () => {
    const store = kronosDbEventStore(kdb, "default")
    const orderId = `o-${run}-1`
    const written = fact("OrderPlaced", { orderId, total: 12.5, note: "naïve café ✓" }, [["orderId", orderId], ["region", "eu"]], {
      actor: "alice",
      attempt: 3,
    })

    await store.append([written])
    const { events } = await store.source({ query: { tags: { orderId } } })

    expect(events).toHaveLength(1)
    const read = events[0]!
    expect(read.identifier).toBe(written.identifier)
    expect(qualifiedNameToString(read.name)).toBe("native-it.OrderPlaced")
    expect(read.version).toBe("2.1")
    expect(read.timestamp).toBe(written.timestamp)
    expect(read.payload).toEqual({ orderId, total: 12.5, note: "naïve café ✓" })
    // Metadata is text on the wire, as it always was.
    expect(read.metadata).toEqual({ actor: "alice", attempt: "3" })
    // Tags now come back on read events.
    expect(read.tags).toEqual([
      { key: "orderId", value: orderId },
      { key: "region", value: "eu" },
    ])
  })

  it("filters by type and by tag, and ORs the items of a query", async () => {
    const store = kronosDbEventStore(kdb, "default")
    const a = `a-${run}`
    const b = `b-${run}`
    await store.append([
      fact("Opened", { id: a }, [["acct", a]]),
      fact("Deposited", { id: a }, [["acct", a]]),
      fact("Opened", { id: b }, [["acct", b]]),
    ])

    const onlyOpenedA = await store.source({ query: { tags: { acct: a }, types: ["native-it.Opened"] } })
    expect(onlyOpenedA.events.map((e) => qualifiedNameToString(e.name))).toEqual(["native-it.Opened"])

    const either = await store.source({ query: [{ tags: { acct: a } }, { tags: { acct: b } }] })
    expect(either.events).toHaveLength(3)

    const none = await store.source({ query: { tags: { acct: `missing-${run}` } } })
    expect(none.events).toHaveLength(0)
  })

  it("answers a conditional append with a marker, and refuses a stale one with a ConcurrencyError", async () => {
    const store = kronosDbEventStore(kdb, "default")
    const id = `c-${run}`
    const query = { tags: { cond: id } }

    const first = await store.source({ query })
    await store.append([fact("Claimed", { id }, [["cond", id]])], { query, marker: first.marker })

    const refusal = await store
      .append([fact("Claimed", { id }, [["cond", id]])], { query, marker: first.marker })
      .then(() => undefined, (e) => e)
    expect(refusal).toBeInstanceOf(ConcurrencyError)
    expect(refusal.message).toMatch(/consistency condition violated: conflicting event at position \d+/)
    expect(refusal.transient).toBe(true)

    // The marker a read returns is the one a retry conditions on.
    const again = await store.source({ query })
    await store.append([fact("Claimed", { id: `${id}-2` }, [["cond", id]])], { query, marker: again.marker })
    expect((await store.source({ query })).events).toHaveLength(2)
  })

  it("reads big batches whole — more events than one frame, payloads that straddle reads", async () => {
    const store = kronosDbEventStore(kdb, "default")
    const id = `big-${run}`
    const filler = "x".repeat(4_000)
    const batch = Array.from({ length: 300 }, (_, i) => fact("Chunk", { id, i, filler }, [["big", id]]))
    // 300 × ~4 KB: one append frame over a megabyte, a read of many frames.
    await store.append(batch)

    const { events } = await store.source({ query: { tags: { big: id } } })
    expect(events).toHaveLength(300)
    expect(events.map((e) => (e.payload as { i: number }).i)).toEqual(batch.map((_, i) => i))
    expect((events[299]!.payload as { filler: string }).filler.length).toBe(4_000)
  })

  it("refuses an append larger than a frame, and the connection carries on", async () => {
    const store = kronosDbEventStore(kdb, "default")
    // Whatever the server allows a client in one frame (it has been 16 MiB, and 65 MiB since).
    const limit = (await kdb.connection.native.ready()).welcome.maxFrame
    const huge = fact("Huge", { blob: "y".repeat(limit + 1024) }, [["huge", run]])
    const refusal = await store.append([huge]).then(() => undefined, (e) => e)
    assert.ok(refusal instanceof Error)
    expect(refusal.message).toMatch(/exceeds the server's limit/)
    await store.append([fact("After", { ok: true }, [["huge", run]])])
    expect((await store.source({ query: { tags: { huge: run } } })).events).toHaveLength(1)
  })

  it("says what is wrong with a context that does not exist", async () => {
    const store = kronosDbEventStore(kdb, `no-such-context-${run}`)
    const refusal = await store.getHeadPosition().then(() => undefined, (e) => e)
    expect(refusal).toBeInstanceOf(KronosDbRequestError)
    expect(refusal.status).toBe("not_found")
    expect(refusal.message).toMatch(/context not found/)
  })

  it("keeps many appends in flight on the one connection", async () => {
    const store = kronosDbEventStore(kdb, "default")
    const id = `many-${run}`
    const markers = await Promise.all(
      Array.from({ length: 200 }, (_, i) => store.append([fact("Tick", { i }, [["many", id]])])),
    )
    expect(markers).toHaveLength(200)
    expect((await store.source({ query: { tags: { many: id } } })).events).toHaveLength(200)
  })

  it("reports the head, the tail, a position's tags and the position at a time", async () => {
    const store = kronosDbEventStore(kdb, "default")
    const id = `pos-${run}`
    const before = Date.now()
    const head = await store.getHeadPosition()
    await store.append([fact("Positioned", { id }, [["pos", id], ["kind", "probe"]])])

    expect(await store.getHeadPosition()).toBeGreaterThan(head)
    expect(await kronosDbTail(kdb, "default")).toBe(0n)
    expect(await kronosDbTagsAt(kdb, "default", head)).toEqual([
      { key: "pos", value: id },
      { key: "kind", value: "probe" },
    ])
    expect(await kronosDbPositionAt(kdb, "default", before - 60_000)).toBeDefined()
    expect(await kronosDbPositionAt(kdb, "default", Date.now() + 3_600_000)).toBeUndefined()
  })

  describe("subscriptions", () => {
    it("delivers history, then live events, in order with their positions", async () => {
      const store = kronosDbEventStore(kdb, "default")
      const id = `sub-${run}`
      const start = await store.getHeadPosition()
      await store.append([fact("Before", { n: 0 }, [["sub", id]]), fact("Before", { n: 1 }, [["sub", id]])])

      const stream = store.open({ position: start, query: { tags: { sub: id } } })
      const seen: number[] = []
      const positions: bigint[] = []
      const drain = () => {
        for (let item = stream.next(); item !== undefined; item = stream.next()) {
          seen.push((item.event.payload as { n: number }).n)
          positions.push(item.sequence)
        }
      }
      stream.setCallback(drain)
      await waitFor(() => (drain(), seen.length === 2))

      await store.append([fact("Live", { n: 2 }, [["sub", id]])])
      await waitFor(() => (drain(), seen.length === 3))
      stream.close()

      expect(seen).toEqual([0, 1, 2])
      expect(positions[0]! < positions[1]! && positions[1]! < positions[2]!).toBe(true)
      expect(positions[0]).toBeGreaterThanOrEqual(start)
    })

    it("grants credits as the consumer takes events, so a long backlog flows through", async () => {
      const store = kronosDbEventStore(kdb, "default")
      const id = `flow-${run}`
      const start = await store.getHeadPosition()
      // Several times the 500 initial credits.
      for (let batch = 0; batch < 4; batch++) {
        await store.append(Array.from({ length: 400 }, (_, i) => fact("Flow", { n: batch * 400 + i }, [["flow", id]])))
      }
      const stream = store.open({ position: start, query: { tags: { flow: id } } })
      const seen: number[] = []
      await waitFor(() => {
        for (let item = stream.next(); item !== undefined; item = stream.next()) seen.push((item.event.payload as { n: number }).n)
        return seen.length === 1600
      }, 20_000)
      stream.close()
      expect(seen).toEqual(Array.from({ length: 1600 }, (_, i) => i))
    })

    it("peek shows the next event without taking it", async () => {
      const store = kronosDbEventStore(kdb, "default")
      const id = `peek-${run}`
      const start = await store.getHeadPosition()
      await store.append([fact("Peeked", { n: 1 }, [["peek", id]])])
      const stream = store.open({ position: start, query: { tags: { peek: id } } })
      await waitFor(() => stream.hasNextAvailable())
      expect((stream.peek()?.event.payload as { n: number }).n).toBe(1)
      expect((stream.peek()?.event.payload as { n: number }).n).toBe(1)
      expect((stream.next()?.event.payload as { n: number }).n).toBe(1)
      expect(stream.next()).toBeUndefined()
      stream.close()
      expect(stream.isCompleted()).toBe(true)
    })

    it("reports progress through a stretch where nothing matches", async () => {
      const store = kronosDbEventStore(kdb, "default")
      const quiet = `quiet-${run}`
      const noise = `noise-${run}`
      const start = await store.getHeadPosition()
      const stream = kronosDbSubscription(kdb, "default", { position: start, query: { tags: { quiet } } }, { progressIntervalMs: 50 })
      await store.append(Array.from({ length: 20 }, (_, i) => fact("Noise", { i }, [["noise", noise]])))
      const head = await store.getHeadPosition()

      await waitFor(() => (stream.progress() ?? 0n) >= head, 10_000)
      expect(stream.hasNextAvailable()).toBe(false)
      stream.close()
    })

    it("ends with the server's reason when the subscription is refused", async () => {
      const refused = kronosDbEventStore(kdb, `no-such-context-${run}`).open({ position: 0n })
      await waitFor(() => refused.isCompleted())
      expect(refused.error()).toBeInstanceOf(KronosDbRequestError)
      expect((refused.error() as KronosDbRequestError).status).toBe("not_found")
    })
  })
})
