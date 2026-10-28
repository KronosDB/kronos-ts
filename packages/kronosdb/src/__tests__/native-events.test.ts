import { describe, expect, it } from "bun:test"
import { jsonSerializer, qn, type EventMessage } from "@kronos-ts/core"
import {
  criteriaToWire,
  outbound,
  rawBatch,
  readEvents,
  writeCondition,
  writeCriteria,
  writeRecord,
  writeRecords,
} from "../native-events.js"
import {
  ProtocolError,
  encodeCriterion,
  decodeEvents,
  encodeEvents,
  encodeRecord,
  encodeRecords,
  reader,
  writeLen16,
  writeU64,
  writeU8,
  writer,
  written,
  type EventRecord,
  type Writer,
} from "../protocol/kronosdb-protocol.js"

const serializer = jsonSerializer()
const hex = (b: Uint8Array) => Buffer.from(b).toString("hex")

function event(overrides: Partial<EventMessage> = {}): EventMessage {
  return {
    kind: "event",
    identifier: "evt-1",
    name: qn("shop", "OrderPlaced"),
    version: "3",
    payload: { orderId: "o-1", note: "naïve ✓" },
    metadata: { actor: "alice", attempt: 3, dropped: undefined, alsoDropped: null },
    timestamp: 1_700_000_000_123,
    tags: [
      { key: "orderId", value: "o-1" },
      { key: "região", value: "são paulo" },
    ],
    ...overrides,
  }
}

/** The record the codec would write for `e` — the reference the hot path must match byte for byte. */
function codecRecord(e: EventMessage, position = 0n): EventRecord {
  const enc = new TextEncoder()
  return {
    position,
    identifier: e.identifier,
    name: `${e.name.namespace}.${e.name.name}`,
    version: e.version,
    timestamp: e.timestamp,
    metadata: Object.entries(e.metadata)
      .filter(([, v]) => v !== undefined && v !== null)
      .map(([key, value]) => ({ key, value: String(value) })),
    tags: e.tags.map((t) => ({ key: enc.encode(t.key), value: enc.encode(t.value) })),
    payload: serializer.serialize(e.payload, "", "").data,
  }
}

function encodeWith(fn: (w: Writer) => void): Uint8Array {
  const w = writer(8)
  fn(w)
  return written(w)
}

describe("writing events", () => {
  it("writes the bytes the generated codec writes", () => {
    const e = event()
    const mine = encodeWith((w) => writeRecord(w, e, outbound(serializer, [e])[0]!.data))
    const reference = encodeWith((w) => encodeRecord(w, { flags: 0, event: codecRecord(e) }))
    expect(hex(mine)).toBe(hex(reference))
  })

  it("writes a whole batch as a count and the records", () => {
    const events = [event(), event({ identifier: "evt-2", payload: {} })]
    const mine = encodeWith((w) => writeRecords(w, outbound(serializer, events)))
    const reference = encodeWith((w) => encodeRecords(w, events.map((e) => ({ flags: 0, event: codecRecord(e) })), "events"))
    expect(hex(mine)).toBe(hex(reference))
  })

  it("skips null and undefined metadata, and sends the rest as text", () => {
    // Write with the hot path, read back with the codec.
    const e = event()
    const body = encodeWith((w) => {
      writeRecords(w, outbound(serializer, [e]))
      writeU8(w, 0)
    })
    const decoded = decodeEvents(reader(body))
    expect(decoded.events[0]!.event.metadata).toEqual([
      { key: "actor", value: "alice" },
      { key: "attempt", value: "3" },
    ])
    expect(decoded.events[0]!.event.tags.map((t) => new TextDecoder().decode(t.key))).toEqual(["orderId", "região"])
  })

  it("fails, and leaves nothing behind, when a field does not fit", () => {
    const w = writer(8)
    const before = w.pos
    expect(() => writeRecord(w, event({ identifier: "x".repeat(70_000) }), new Uint8Array(0))).toThrow(ProtocolError)
    // The caller rewinds; the writer itself is still usable.
    w.pos = before
    writeRecord(w, event(), new Uint8Array(0))
    expect(w.pos).toBeGreaterThan(0)
  })

  it("writes criteria as the codec does", () => {
    const criteria = criteriaToWire({
      kind: "either",
      criteria: [
        { kind: "tags", tags: [{ key: "a", value: "1" }] },
        { kind: "type-restricted", inner: { kind: "tags", tags: [{ key: "b", value: "é" }] }, types: ["x.Y", "x.Z"] },
        { kind: "any-tag" },
      ],
    })
    expect(criteria).toHaveLength(3)
    const mine = encodeWith((w) => writeCriteria(w, criteria))
    const enc = new TextEncoder()
    const reference = encodeWith((w) => {
      writeLen16(w, criteria.length, "criteria")
      for (const c of criteria) {
        encodeCriterion(w, { names: [...c.names], tags: c.tags.map((t) => ({ key: enc.encode(t.key), value: enc.encode(t.value) })) })
      }
    })
    expect(hex(mine)).toBe(hex(reference))
  })

  it("writes an absent condition as one zero byte and a present one as marker and criteria", () => {
    expect(hex(encodeWith((w) => writeCondition(w, undefined)))).toBe("00")
    const present = encodeWith((w) => writeCondition(w, { marker: 7n, criteria: [{ names: ["n"], tags: [] }] }))
    const reference = encodeWith((w) => {
      writeU8(w, 1)
      writeU64(w, 7n)
      writeCriteria(w, [{ names: ["n"], tags: [] }])
    })
    expect(hex(present)).toBe(hex(reference))
  })
})

/** An Events frame body, written by the generated codec. */
function eventsBody(records: Array<{ flags: number; event: EventRecord }>, marker?: bigint): Uint8Array {
  return encodeWith((w) => encodeEvents(w, { events: records, marker }))
}

describe("reading events", () => {
  const first = event()
  const second = event({ identifier: "evt-2", payload: { orderId: "o-2" }, tags: [], metadata: {} })

  it("decodes what the codec wrote, with tags, and reports the marker", () => {
    const body = eventsBody(
      [
        { flags: 0, event: codecRecord(first, 10n) },
        { flags: 0, event: codecRecord(second, 11n) },
      ],
      12n,
    )
    const out: EventMessage[] = []
    const marker = readEvents(body, 0, body.length, serializer, out)

    expect(marker).toBe(12n)
    expect(out).toHaveLength(2)
    expect(out[0]).toEqual({
      kind: "event",
      identifier: "evt-1",
      name: qn("shop", "OrderPlaced"),
      version: "3",
      payload: { orderId: "o-1", note: "naïve ✓" },
      metadata: { actor: "alice", attempt: "3" },
      timestamp: 1_700_000_000_123,
      tags: [
        { key: "orderId", value: "o-1" },
        { key: "região", value: "são paulo" },
      ],
    })
    expect(out[1]!.payload).toEqual({ orderId: "o-2" })
  })

  it("reports no marker on an intermediate frame", () => {
    const body = eventsBody([{ flags: 0, event: codecRecord(first, 1n) }])
    expect(readEvents(body, 0, body.length, serializer, [])).toBeUndefined()
  })

  it("steps over events below the floor without decoding them", () => {
    const poisoned = { ...codecRecord(first, 3n), payload: new TextEncoder().encode("{ not json") }
    const body = eventsBody(
      [
        { flags: 0, event: poisoned },
        { flags: 0, event: codecRecord(second, 4n) },
      ],
      5n,
    )
    const out: EventMessage[] = []
    readEvents(body, 0, body.length, serializer, out, 4n)
    expect(out.map((e) => e.identifier)).toEqual(["evt-2"])
  })

  it("steps over records that are not events", () => {
    const body = eventsBody([
      { flags: 0x40, event: codecRecord(first, 1n) },
      { flags: 0, event: codecRecord(second, 2n) },
    ])
    const out: EventMessage[] = []
    readEvents(body, 0, body.length, serializer, out)
    expect(out.map((e) => e.identifier)).toEqual(["evt-2"])
  })

  it("treats an empty payload as the empty object", () => {
    const body = eventsBody([{ flags: 0, event: { ...codecRecord(first, 1n), payload: new Uint8Array(0) } }])
    const out: EventMessage[] = []
    readEvents(body, 0, body.length, serializer, out)
    expect(out[0]!.payload).toEqual({})
  })

  it("reads tags that are not UTF-8 without failing", () => {
    const body = eventsBody([{ flags: 0, event: { ...codecRecord(first, 1n), tags: [{ key: new Uint8Array([0x6b, 0xff]), value: new Uint8Array([0xc3, 0x28]) }] } }])
    const out: EventMessage[] = []
    readEvents(body, 0, body.length, serializer, out)
    expect(out[0]!.tags).toHaveLength(1)
    expect(out[0]!.tags[0]!.key.startsWith("k")).toBe(true)
  })

  it("rejects a record that overruns its frame, or leaves bytes behind", () => {
    const body = eventsBody([{ flags: 0, event: codecRecord(first, 1n) }], 2n)
    expect(() => readEvents(body, 0, body.length - 12, serializer, [])).toThrow(ProtocolError)

    const padded = eventsBody([{ flags: 0, event: codecRecord(first, 1n) }])
    // Grow the record's declared length by one: the body no longer ends where it says.
    new DataView(padded.buffer, padded.byteOffset).setUint32(4, new DataView(padded.buffer, padded.byteOffset).getUint32(4, true) + 1, true)
    expect(() => readEvents(padded, 0, padded.length, serializer, [])).toThrow(ProtocolError)
  })

  describe("a batch kept as bytes", () => {
    const body = eventsBody(
      [
        { flags: 0, event: codecRecord(first, 20n) },
        { flags: 0, event: codecRecord(second, 21n) },
      ],
      22n,
    )

    it("knows its size at once and decodes an event only when it is taken", () => {
      const batch = rawBatch(body, 0, body.length, serializer)
      expect(batch.remaining()).toBe(2)
      const one = batch.take()!
      expect(one.sequence).toBe(20n)
      expect(one.event.identifier).toBe("evt-1")
      expect(batch.remaining()).toBe(1)
      const two = batch.take()!
      expect(two.sequence).toBe(21n)
      expect(batch.take()).toBeUndefined()
      expect(batch.remaining()).toBe(0)
    })

    it("does not decode anything up front", () => {
      const poisoned = { ...codecRecord(first, 1n), payload: new TextEncoder().encode("{ not json") }
      const lazy = rawBatch(eventsBody([{ flags: 0, event: poisoned }]), 0, eventsBody([{ flags: 0, event: poisoned }]).length, serializer)
      expect(lazy.remaining()).toBe(1)
      expect(() => lazy.take()).toThrow()
    })

    it("works over a view into a larger buffer", () => {
      const padded = new Uint8Array(body.length + 30)
      padded.set(body, 17)
      const batch = rawBatch(padded.subarray(10), 7, 7 + body.length, serializer)
      expect(batch.take()!.event.identifier).toBe("evt-1")
    })
  })
})
