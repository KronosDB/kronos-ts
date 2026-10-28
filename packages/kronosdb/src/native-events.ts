// ---------------------------------------------------------------------------
// EVENTS ON THE NATIVE WIRE — written straight into the frame, read straight
// out of it.
//
// The protocol carries an event as the server's own stored record:
//
//   len u32 | flags u8 | position u64 | identifier | name | version | timestamp
//          | metadata[] | tags[] | payload
//
// so there is nothing to translate between. Writing an event is appending
// those fields to the frame being built; reading one is walking the received
// bytes. This file does exactly that and skips the codec's intermediate
// objects (`StoredRecord`, `EventRecord`, one `MetadataEntry` and `Tag` apiece)
// — the generated codec stays the reference the conformance suite holds the
// bytes to, and this is the hot path that must produce the same bytes without
// building a tree first.
//
// READING IS LAZY WHERE IT CAN BE. A batch is kept as the bytes it arrived in
// and an event is decoded when somebody takes it: a subscription holds raw
// batches and decodes at `next()`, so a consumer that is behind pays memory in
// network bytes, not in event objects. `Source` has to hand back an array, so it
// decodes as the frames arrive — but still without an intermediate tree, and
// the payload goes to the serializer as a VIEW into the frame (never a copy).
// ---------------------------------------------------------------------------

import { qualifiedNameFromString, qualifiedNameToString, type QualifiedName, type Serializer } from "@kronos-ts/core"
import type { EventCriteria, EventMessage, SequencedEvent } from "@kronos-ts/core"
import {
  ProtocolError,
  ensure,
  readBytes32,
  readMillis,
  readPresent,
  readStr16,
  readU16,
  readU32,
  readU64,
  readU8,
  writeLen16,
  writeLen32,
  writeMillis,
  writeStr16,
  writeU16,
  writeU32,
  writeU64,
  writeU8,
  type Reader,
  type Writer,
} from "./protocol/kronosdb-protocol.js"

// ---------------------------------------------------------------------------
// Criteria
//
// KronosDB's Criterion is `names` (any of; empty = any type) plus `tags` (all
// of). A read or condition carries a list of them, ORed together.
// ---------------------------------------------------------------------------

/** A criterion with its tags as text — what the framework's queries hold. */
export type WireCriterion = {
  readonly names: ReadonlyArray<string>
  readonly tags: ReadonlyArray<{ readonly key: string; readonly value: string }>
}

export function criteriaToWire(criteria: EventCriteria): WireCriterion[] {
  switch (criteria.kind) {
    case "tags":
      return [{ names: [], tags: criteria.tags }]
    case "type-restricted":
      return [{ names: criteria.types, tags: criteria.inner.kind === "tags" ? criteria.inner.tags : [] }]
    case "either":
      return criteria.criteria.flatMap(criteriaToWire)
    case "any-tag":
      return [{ names: [], tags: [] }]
  }
}

/**
 * Write a criterion list. A tag key and value are `bytes16` on the wire, which
 * is a u16 length and the bytes — the same layout `str16` has, so the text
 * goes in as it is.
 */
export function writeCriteria(w: Writer, criteria: ReadonlyArray<WireCriterion>): void {
  writeLen16(w, criteria.length, "criteria")
  for (const criterion of criteria) {
    writeLen16(w, criterion.names.length, "criterion names")
    for (const name of criterion.names) writeStr16(w, name, "criterion name")
    writeLen16(w, criterion.tags.length, "criterion tags")
    for (const tag of criterion.tags) {
      writeStr16(w, tag.key, "tag key")
      writeStr16(w, tag.value, "tag value")
    }
  }
}

// ---------------------------------------------------------------------------
// Writing events
// ---------------------------------------------------------------------------

/** An event and its payload, serialized — everything a record needs. */
export type OutboundEvent = {
  readonly event: EventMessage
  readonly data: Uint8Array
}

/** Serialize the payloads (which may throw, and should before anything is sent). */
export function outbound(serializer: Serializer, events: ReadonlyArray<EventMessage>): OutboundEvent[] {
  const out = new Array<OutboundEvent>(events.length)
  for (let i = 0; i < events.length; i++) {
    const event = events[i]!
    out[i] = { event, data: serializer.serialize(event.payload, qualifiedNameToString(event.name), event.version).data }
  }
  return out
}

/**
 * Append one record: `len u32 | flags u8 | body`. Position is 0 — the server
 * assigns it. Metadata values are text on the wire, as they were on the gRPC
 * one; a null or undefined value is not sent.
 */
export function writeRecord(w: Writer, event: EventMessage, data: Uint8Array): void {
  const start = w.pos
  writeU32(w, 0)
  writeU8(w, 0)
  writeU64(w, 0n)
  writeStr16(w, event.identifier, "event identifier")
  writeStr16(w, qualifiedNameToString(event.name), "event name")
  writeStr16(w, event.version, "event version")
  writeMillis(w, event.timestamp)

  // The entry count precedes the entries and nulls are skipped, so it is
  // written last, into the space reserved for it.
  const metadataCountAt = w.pos
  writeU16(w, 0)
  let entries = 0
  for (const key in event.metadata) {
    const value = event.metadata[key]
    if (value === undefined || value === null) continue
    writeStr16(w, key, "metadata key")
    writeStr16(w, typeof value === "string" ? value : String(value), "metadata value")
    entries++
  }
  if (entries > 0xffff) throw new ProtocolError(`metadata: ${entries} entries exceed the u16 limit`)
  w.view.setUint16(metadataCountAt, entries, true)

  writeLen16(w, event.tags.length, "event tags")
  for (const tag of event.tags) {
    writeStr16(w, tag.key, "tag key")
    writeStr16(w, tag.value, "tag value")
  }

  writeLen32(w, data.length, "event payload")
  ensure(w, data.length)
  w.buf.set(data, w.pos)
  w.pos += data.length

  w.view.setUint32(start, w.pos - start - 4, true)
}

/** Append `count u32` and the records. */
export function writeRecords(w: Writer, events: ReadonlyArray<OutboundEvent>): void {
  writeLen32(w, events.length, "events")
  for (const { event, data } of events) writeRecord(w, event, data)
}

/** Append an optional append condition: presence byte, marker, criteria. */
export function writeCondition(w: Writer, condition: { marker: bigint; criteria: ReadonlyArray<WireCriterion> } | undefined): void {
  if (condition === undefined) {
    writeU8(w, 0)
    return
  }
  writeU8(w, 1)
  writeU64(w, condition.marker)
  writeCriteria(w, condition.criteria)
}

// ---------------------------------------------------------------------------
// Reading events
// ---------------------------------------------------------------------------

const utf8 = new TextDecoder()

/** Tag text: opaque bytes on the wire, ASCII almost always, never an error. */
function readTagText(r: Reader): string {
  const len = readU16(r)
  if (r.pos + len > r.end) throw new ProtocolError(`truncated: need ${len} bytes at offset ${r.pos}`)
  const start = r.pos
  r.pos += len
  if (len <= 64) {
    let s = ""
    for (let i = start; i < r.pos; i++) {
      const c = r.buf[i]!
      if (c >= 0x80) return utf8.decode(r.buf.subarray(start, r.pos))
      s += String.fromCharCode(c)
    }
    return s
  }
  return utf8.decode(r.buf.subarray(start, r.pos))
}

// Event type names come from a small, fixed set per application. Splitting
// "namespace.Name" on every event would allocate the same pair again and again.
const names = new Map<string, QualifiedName>()
function qualified(fqn: string): QualifiedName {
  let name = names.get(fqn)
  if (name === undefined) {
    if (names.size >= 4096) names.clear()
    name = qualifiedNameFromString(fqn)
    names.set(fqn, name)
  }
  return name
}

/** The event at `r.pos` after its position: identifier through payload. */
function readEventBody(r: Reader, serializer: Serializer): EventMessage {
  const identifier = readStr16(r)
  const name = readStr16(r)
  const version = readStr16(r)
  const timestamp = readMillis(r)

  const metadata: Record<string, unknown> = {}
  for (let n = readU16(r); n > 0; n--) {
    const key = readStr16(r)
    metadata[key] = readStr16(r)
  }

  const tagCount = readU16(r)
  const tags = new Array<{ key: string; value: string }>(tagCount)
  for (let i = 0; i < tagCount; i++) {
    const key = readTagText(r)
    tags[i] = { key, value: readTagText(r) }
  }

  const data = readBytes32(r)
  // An empty payload is the empty object, as it was on the gRPC wire.
  const payload = data.length > 0 ? serializer.deserialize({ data, type: name, revision: version }) : {}

  return {
    kind: "event",
    identifier,
    name: qualified(name),
    version,
    payload,
    metadata,
    timestamp,
    tags,
  }
}

/**
 * Read the record at `r.pos`. Returns the event and, if asked, its position;
 * `undefined` for a record that is not an event (flags other than 0), which a
 * client that does not know it must step over.
 */
function readRecordAt(
  r: Reader,
  serializer: Serializer,
  sequenced: boolean,
  floor?: bigint,
): EventMessage | SequencedEvent | undefined {
  const len = readU32(r)
  if (len === 0) throw new ProtocolError("zero-length record")
  const end = r.pos + len
  if (end > r.end) throw new ProtocolError("record extends past the frame")
  if (readU8(r) !== 0) {
    r.pos = end
    return undefined
  }
  let sequence = 0n
  if (sequenced || floor !== undefined) {
    sequence = readU64(r)
    // Below the floor: not wanted, and not worth decoding.
    if (floor !== undefined && sequence < floor) {
      r.pos = end
      return undefined
    }
  } else {
    r.pos += 8
  }
  const event = readEventBody(r, serializer)
  if (r.pos !== end) throw new ProtocolError("record body has trailing bytes")
  return sequenced ? { sequence, event } : event
}

/**
 * The events of an Events frame body, decoded into `out` as they are; returns
 * the marker (present only on the final frame of a Source). With a `floor`,
 * events positioned below it are stepped over undecoded.
 */
export function readEvents(
  buf: Uint8Array,
  start: number,
  end: number,
  serializer: Serializer,
  out: EventMessage[],
  floor?: bigint,
): bigint | undefined {
  const r: Reader = { buf, view: new DataView(buf.buffer, buf.byteOffset, buf.byteLength), pos: start, end }
  for (let n = readU32(r); n > 0; n--) {
    const event = readRecordAt(r, serializer, false, floor)
    if (event !== undefined) out.push(event as EventMessage)
  }
  return readPresent(r) ? readU64(r) : undefined
}

/**
 * An Events frame kept as the bytes it arrived in. `take()` decodes the next
 * event; nothing is decoded until then, and `remaining` is known up front so a
 * consumer can count what it was sent without reading it.
 */
export type RawBatch = {
  /** Records in the batch not yet taken; a record that is not an event counts until it is stepped over. */
  readonly remaining: () => number
  /** The next event, or undefined when the batch is spent. */
  take(): SequencedEvent | undefined
}

export function rawBatch(buf: Uint8Array, start: number, end: number, serializer: Serializer): RawBatch {
  const r: Reader = { buf, view: new DataView(buf.buffer, buf.byteOffset, buf.byteLength), pos: start, end }
  let left = readU32(r)
  return {
    remaining: () => left,
    take() {
      while (left > 0) {
        left--
        const event = readRecordAt(r, serializer, true)
        if (event !== undefined) return event as SequencedEvent
      }
      return undefined
    },
  }
}
