import { afterEach, describe, expect, it } from "bun:test"
import { jsonSerializer, markerAt, noMarker, qn, type EventMessage } from "@kronos-ts/core"
import { connectToKronosDb, type KronosDbConnection } from "../connection.js"
import { ConcurrencyError, KronosDbRequestError } from "../errors.js"
import { kronosDbEventStore } from "../kronosdb-event-store.js"
import { kronosDbPositionAt, kronosDbTagsAt, kronosDbTail } from "../kronosdb-log-reads.js"
import { kronosDbSnapshottingEventStore } from "../kronosdb-snapshotting-event-store.js"
import { ErrorCode, FLAG_END, type EventRecord, type Frame } from "../protocol/kronosdb-protocol.js"
import { fakeNativeServer, type FakeConnection, type FakeNativeServer, type ReceivedFrame } from "./fake-native-server.js"

const serializer = jsonSerializer()
const enc = new TextEncoder()

let server: FakeNativeServer | undefined
let connection: KronosDbConnection | undefined

afterEach(async () => {
  connection?.close()
  connection = undefined
  await server?.close()
  server = undefined
})

async function against(answer: (conn: FakeConnection, received: ReceivedFrame) => void) {
  server = await fakeNativeServer({ onFrame: answer })
  connection = connectToKronosDb({ host: "127.0.0.1", port: server.port, componentName: "store-test" })
  const kdb = { connection, serializer }
  return { kdb, sent: () => server!.connections[0]!.received }
}

const sample = (id: string, extra: Partial<EventMessage> = {}): EventMessage => ({
  kind: "event",
  identifier: id,
  name: qn("shop", "OrderPlaced"),
  version: "1",
  payload: { id },
  metadata: { actor: "alice" },
  timestamp: 1_700_000_000_000,
  tags: [{ key: "orderId", value: id }],
  ...extra,
})

const record = (position: bigint, id: string, payload: unknown = { id }): { flags: number; event: EventRecord } => ({
  flags: 0,
  event: {
    position,
    identifier: id,
    name: "shop.OrderPlaced",
    version: "1",
    timestamp: 1_700_000_000_000,
    metadata: [],
    tags: [{ key: enc.encode("orderId"), value: enc.encode(id) }],
    payload: enc.encode(JSON.stringify(payload)),
  },
})

const failed = (code: number, message: string): Frame => ({ name: "RequestFailed", body: { code, message } })

describe("kronosDbEventStore (native)", () => {
  describe("append", () => {
    it("sends the events in one Append frame and answers the marker from Appended", async () => {
      const { kdb, sent } = await against((conn, r) =>
        conn.reply(r.stream, FLAG_END, { name: "Appended", body: { firstPosition: 5n, count: 2, marker: 7n } }),
      )
      const marker = await kronosDbEventStore(kdb).append([sample("a"), sample("b")])

      expect(marker).toEqual(markerAt(7n))
      const frame = sent()[0]!.frame
      expect(frame.name).toBe("Append")
      if (frame.name !== "Append") return
      expect(frame.body.context).toBe(0)
      expect(frame.body.condition).toBeUndefined()
      expect(frame.body.events.map((e) => e.event.identifier)).toEqual(["a", "b"])
      expect(frame.body.events[0]!.event.position).toBe(0n)
      expect(frame.body.events[0]!.event.name).toBe("shop.OrderPlaced")
    })

    it("sends a condition as marker plus criteria", async () => {
      const { kdb, sent } = await against((conn, r) =>
        conn.reply(r.stream, FLAG_END, { name: "Appended", body: { firstPosition: 0n, count: 1, marker: 1n } }),
      )
      await kronosDbEventStore(kdb).append([sample("a")], { query: { tags: { orderId: "a" }, types: ["shop.OrderPlaced"] }, marker: markerAt(41n) })
      const frame = sent()[0]!.frame
      if (frame.name !== "Append") throw new Error("expected Append")
      expect(frame.body.condition?.marker).toBe(41n)
      expect(frame.body.condition?.criteria).toEqual([
        { names: ["shop.OrderPlaced"], tags: [{ key: enc.encode("orderId"), value: enc.encode("a") }] },
      ])
    })

    it("treats the origin marker as position 0 — everything counts", async () => {
      const { kdb, sent } = await against((conn, r) =>
        conn.reply(r.stream, FLAG_END, { name: "Appended", body: { firstPosition: 0n, count: 1, marker: 1n } }),
      )
      await kronosDbEventStore(kdb).append([sample("a")], { query: { tags: { orderId: "a" } }, marker: noMarker() })
      const frame = sent()[0]!.frame
      if (frame.name !== "Append") throw new Error("expected Append")
      expect(frame.body.condition?.marker).toBe(0n)
    })

    it("turns AppendConflict into a ConcurrencyError that names the position", async () => {
      const { kdb } = await against((conn, r) => conn.reply(r.stream, FLAG_END, { name: "AppendConflict", body: { conflictingPosition: 12n } }))
      const refusal = await kronosDbEventStore(kdb)
        .append([sample("a")], { query: { tags: { orderId: "a" } }, marker: markerAt(3n) })
        .then(() => undefined, (e) => e)
      expect(refusal).toBeInstanceOf(ConcurrencyError)
      expect(refusal.message).toBe("consistency condition violated: conflicting event at position 12")
    })

    it("stages an append and sends it only on commit", async () => {
      const { kdb, sent } = await against((conn, r) =>
        conn.reply(r.stream, FLAG_END, { name: "Appended", body: { firstPosition: 0n, count: 1, marker: 9n } }),
      )
      const store = kronosDbEventStore(kdb)
      const tx = await store.appendEvents([sample("a")])
      await Bun.sleep(20)
      expect(server!.connections).toHaveLength(0)

      await tx.commit()
      expect(await tx.afterCommit()).toEqual(markerAt(9n))
      expect(sent()).toHaveLength(1)

      const rolledBack = await store.appendEvents([sample("b")])
      rolledBack.rollback()
      await Bun.sleep(20)
      expect(sent()).toHaveLength(1)
    })

    it("fails at staging, before anything is sent, when a payload cannot be serialized", async () => {
      const { kdb } = await against(() => {})
      const cyclic: Record<string, unknown> = {}
      cyclic.self = cyclic
      const refusal = await kronosDbEventStore(kdb)
        .appendEvents([sample("a", { payload: cyclic })])
        .then(() => undefined, (e) => e)
      expect(refusal).toBeInstanceOf(Error)
      expect(server!.connections).toHaveLength(0)
    })

    it("surfaces the server's refusal with its status", async () => {
      const { kdb } = await against((conn, r) => conn.reply(r.stream, FLAG_END, failed(ErrorCode.unavailable, "not the leader")))
      const refusal = await kronosDbEventStore(kdb)
        .append([sample("a")])
        .then(() => undefined, (e) => e)
      expect(refusal).toBeInstanceOf(KronosDbRequestError)
      expect(refusal.status).toBe("unavailable")
      expect(refusal.transient).toBe(true)
    })
  })

  describe("source", () => {
    it("joins the batches of a read and ends with the marker", async () => {
      const { kdb, sent } = await against((conn, r) => {
        conn.reply(r.stream, 0, { name: "Events", body: { events: [record(3n, "a"), record(4n, "b")] } })
        conn.reply(r.stream, FLAG_END, { name: "Events", body: { events: [record(5n, "c")], marker: 6n } })
      })
      const result = await kronosDbEventStore(kdb).source({ query: { tags: { orderId: "a" } }, start: 3n })

      expect(result.events.map((e) => e.identifier)).toEqual(["a", "b", "c"])
      expect(result.events[0]!.tags).toEqual([{ key: "orderId", value: "a" }])
      expect(result.marker).toEqual(markerAt(6n))
      const frame = sent()[0]!.frame
      if (frame.name !== "Source") throw new Error("expected Source")
      expect(frame.body).toMatchObject({ context: 0, from: 3n, batchSize: 0 })
      expect(frame.body.criteria).toEqual([{ names: [], tags: [{ key: enc.encode("orderId"), value: enc.encode("a") }] }])
      expect(sent()[0]!.flags & FLAG_END).toBe(FLAG_END)
    })

    it("reads an empty store: no events, marker 0", async () => {
      const { kdb } = await against((conn, r) => conn.reply(r.stream, FLAG_END, { name: "Events", body: { events: [], marker: 0n } }))
      const result = await kronosDbEventStore(kdb).source({ query: {} })
      expect(result.events).toEqual([])
      expect(result.marker).toEqual(markerAt(0n))
    })

    it("asks for everything with one empty criterion when the query names nothing", async () => {
      const { kdb, sent } = await against((conn, r) => conn.reply(r.stream, FLAG_END, { name: "Events", body: { events: [], marker: 0n } }))
      await kronosDbEventStore(kdb).source({ query: {} })
      const frame = sent()[0]!.frame
      if (frame.name !== "Source") throw new Error("expected Source")
      expect(frame.body.criteria).toEqual([{ names: [], tags: [] }])
    })

    it("fails with the server's reason", async () => {
      const { kdb } = await against((conn, r) => conn.reply(r.stream, FLAG_END, failed(ErrorCode.corrupted, "data corrupted: bad crc")))
      const refusal = await kronosDbEventStore(kdb)
        .source({ query: {} })
        .then(() => undefined, (e) => e)
      expect(refusal.status).toBe("corrupted")
      expect(refusal.message).toBe("data corrupted: bad crc")
    })

    it("fails, rather than hangs, when the connection drops mid-read", async () => {
      const { kdb } = await against((conn, r) => {
        conn.reply(r.stream, 0, { name: "Events", body: { events: [record(1n, "a")] } })
        setTimeout(() => conn.destroy(), 10)
      })
      const refusal = await kronosDbEventStore(kdb)
        .source({ query: {} })
        .then(() => undefined, (e) => e)
      expect(refusal).toBeInstanceOf(Error)
      expect(refusal.name).toBe("ConnectionFailedError")
    })
  })

  describe("contexts", () => {
    it("binds a named context before the first request and addresses it by id", async () => {
      const { kdb, sent } = await against((conn, r) => {
        if (r.frame.name === "BindContext") conn.reply(r.stream, FLAG_END, { name: "ContextBound", body: { id: r.frame.body.id } })
        else conn.reply(r.stream, FLAG_END, { name: "Head", body: { position: 99n } })
      })
      const store = kronosDbEventStore(kdb, "orders")
      expect(await store.getHeadPosition()).toBe(99n)
      expect(await store.getHeadPosition()).toBe(99n)

      const names = sent().map((r) => r.frame.name)
      expect(names).toEqual(["BindContext", "GetHead", "GetHead"])
      const bind = sent()[0]!.frame
      if (bind.name !== "BindContext") throw new Error("expected BindContext")
      expect(bind.body.name).toBe("orders")
      const head = sent()[1]!.frame
      if (head.name !== "GetHead") throw new Error("expected GetHead")
      expect(head.body.context).toBe(bind.body.id)
    })

    it("uses the connection's own context when none is named", async () => {
      server = await fakeNativeServer({
        onFrame(conn, r) {
          if (r.frame.name === "BindContext") conn.reply(r.stream, FLAG_END, { name: "ContextBound", body: { id: r.frame.body.id } })
          else conn.reply(r.stream, FLAG_END, { name: "Head", body: { position: 1n } })
        },
      })
      connection = connectToKronosDb({ host: "127.0.0.1", port: server.port, componentName: "x", context: "billing" })
      await kronosDbEventStore({ connection, serializer }).getHeadPosition()
      const bind = server.connections[0]!.received[0]!.frame
      expect(bind.name === "BindContext" && bind.body.name).toBe("billing")
    })
  })

  describe("tokens and positions", () => {
    it("answers the head position and the token at the head", async () => {
      const { kdb } = await against((conn, r) => conn.reply(r.stream, FLAG_END, { name: "Head", body: { position: 41n } }))
      const store = kronosDbEventStore(kdb)
      expect(await store.getHeadPosition()).toBe(41n)
      expect(await store.latestToken()).toMatchObject({ kind: "global-sequence", sequence: 41n })
    })
  })

  describe("what the log answers beyond the EventStore shape", () => {
    it("reads the tail", async () => {
      const { kdb, sent } = await against((conn, r) => conn.reply(r.stream, FLAG_END, { name: "Tail", body: { position: 1000n } }))
      expect(await kronosDbTail(kdb, "default")).toBe(1000n)
      expect(sent()[0]!.frame).toEqual({ name: "GetTail", body: { context: 0 } })
    })

    it("reads a position's tags as text", async () => {
      const { kdb, sent } = await against((conn, r) =>
        conn.reply(r.stream, FLAG_END, { name: "Tags", body: { tags: [{ key: enc.encode("orderId"), value: enc.encode("o-1") }] } }),
      )
      expect(await kronosDbTagsAt(kdb, "default", 8n)).toEqual([{ key: "orderId", value: "o-1" }])
      expect(sent()[0]!.frame).toEqual({ name: "GetTags", body: { context: 0, position: 8n } })
    })

    it("finds the position at a time, or says there is none", async () => {
      let answer: bigint | undefined = 77n
      const { kdb, sent } = await against((conn, r) => conn.reply(r.stream, FLAG_END, { name: "PositionAt", body: { position: answer } }))
      expect(await kronosDbPositionAt(kdb, "default", new Date(1_700_000_000_000))).toBe(77n)
      expect(sent()[0]!.frame).toEqual({ name: "GetPositionAt", body: { context: 0, timestamp: 1_700_000_000_000 } })
      answer = undefined
      expect(await kronosDbPositionAt(kdb, "default", 5)).toBeUndefined()
    })
  })

  describe("snapshotting", () => {
    it("stores a snapshot under its key with the position unmodified", async () => {
      const { kdb, sent } = await against((conn, r) => conn.reply(r.stream, FLAG_END, { name: "SnapshotAppended", body: { position: 1n } }))
      const store = kronosDbSnapshottingEventStore(kronosDbEventStore(kdb), kdb)
      await store.storeSnapshot("course-v1:c1", { state: { enrolled: 3 }, position: 17n })
      const frame = sent()[0]!.frame
      if (frame.name !== "AppendSnapshot") throw new Error("expected AppendSnapshot")
      expect(new TextDecoder().decode(frame.body.key)).toBe("course-v1:c1")
      expect(JSON.parse(new TextDecoder().decode(frame.body.state))).toEqual({ enrolled: 3 })
      expect(frame.body.position).toBe(17n)
    })

    it("reads the snapshot and the events after it in one request", async () => {
      const { kdb, sent } = await against((conn, r) => {
        conn.reply(r.stream, 0, {
          name: "SnapshotResult",
          body: { snapshot: { state: enc.encode(JSON.stringify({ enrolled: 3 })), position: 10n } },
        })
        conn.reply(r.stream, FLAG_END, { name: "Events", body: { events: [record(10n, "a"), record(11n, "b")], marker: 12n } })
      })
      const store = kronosDbSnapshottingEventStore(kronosDbEventStore(kdb), kdb)
      const result = await store.source({ query: { tags: { orderId: "a" } }, snapshot: { key: "course-v1:c1" } })

      expect(result.snapshot).toEqual({ state: { enrolled: 3 }, position: 10n })
      expect(result.events.map((e) => e.identifier)).toEqual(["a", "b"])
      expect(result.marker).toEqual(markerAt(12n))
      expect(sent()).toHaveLength(1)
      const frame = sent()[0]!.frame
      if (frame.name !== "SnapshottedSource") throw new Error("expected SnapshottedSource")
      expect(new TextDecoder().decode(frame.body.key)).toBe("course-v1:c1")
    })

    it("composes the condition's own start with the snapshot's floor", async () => {
      const { kdb } = await against((conn, r) => {
        conn.reply(r.stream, 0, { name: "SnapshotResult", body: { snapshot: { state: enc.encode("{}"), position: 10n } } })
        conn.reply(r.stream, FLAG_END, { name: "Events", body: { events: [record(10n, "a"), record(11n, "b"), record(12n, "c")], marker: 13n } })
      })
      const store = kronosDbSnapshottingEventStore(kronosDbEventStore(kdb), kdb)
      const result = await store.source({ query: {}, start: 12n, snapshot: { key: "k" } })
      expect(result.events.map((e) => e.identifier)).toEqual(["c"])
    })

    it("behaves as a plain read when the key has no snapshot", async () => {
      const { kdb } = await against((conn, r) => {
        conn.reply(r.stream, 0, { name: "SnapshotResult", body: {} })
        conn.reply(r.stream, FLAG_END, { name: "Events", body: { events: [record(0n, "a")], marker: 1n } })
      })
      const store = kronosDbSnapshottingEventStore(kronosDbEventStore(kdb), kdb)
      const result = await store.source({ query: {}, snapshot: { key: "k" } })
      expect(result.snapshot).toBeUndefined()
      expect(result.events).toHaveLength(1)
    })

    it("sends a read without a snapshot key to the plain Source", async () => {
      const { kdb, sent } = await against((conn, r) => conn.reply(r.stream, FLAG_END, { name: "Events", body: { events: [], marker: 0n } }))
      const store = kronosDbSnapshottingEventStore(kronosDbEventStore(kdb), kdb)
      await store.source({ query: {} })
      expect(sent()[0]!.frame.name).toBe("Source")
    })
  })
})

