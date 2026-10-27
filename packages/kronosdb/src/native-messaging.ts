// ---------------------------------------------------------------------------
// MESSAGES ON THE NATIVE WIRE — commands, queries and their results, written
// straight into the frame and read straight out of it.
//
// A command and a query have the same layout, and so do the results a handler
// answers them with, so one writer and one reader serve both buses:
//
//   message: message_id | name | timestamp | payload | metadata[] | instructions[]
//            | client_id | component_name
//   result:  message_id | request_id | error_code | error? | payload? | metadata[]
//            | instructions[]
//
// WRITING builds no intermediate tree. The codec's `Command` is an object with
// an array of metadata entries, each holding a tagged value; a dispatch would
// allocate all of that to throw it away one statement later. Here the fields go
// into the frame in schema order, and the processing instructions — constant
// for a bus's queries, absent for nearly every command — are bytes written once.
//
// READING takes only what the receiver uses. A handler needs the identifier,
// name, instant, payload and metadata of a command, never its instructions or
// who sent it, so the reader stops after the metadata. A dispatcher needs the
// error code and message, or the payload, of a result: nothing else is decoded.
// The generated codec stays the reference the conformance suite holds the bytes
// to; the tests here round-trip both directions through it.
// ---------------------------------------------------------------------------

import { positiveInteger, type Metadata, type SerializedObject, type Serializer } from "@kronos-ts/core"
import {
  ProcessingKey,
  ProtocolError,
  ensure,
  readBytes32,
  readMillis,
  readPresent,
  readStr16,
  readStr32,
  readU16,
  readU32,
  readU8,
  writeBytes32,
  writeF64,
  writeLen16,
  writeMillis,
  writeStr16,
  writeStr32,
  writeU16,
  writeU32,
  writeU8,
  writer,
  type MessageMetadata,
  type Reader,
  type Writer,
} from "./protocol/kronosdb-protocol.js"

/** Hints a command can carry to the server, beyond its payload. */
export type ProcessingInstructions = {
  /** Commands with the same key go to the same handler. */
  routingKey?: string
  priority?: number
  /** Milliseconds the sender will wait. */
  timeoutMs?: number
}

// MetadataValue variant tags, as the schema numbers them.
const TEXT = 1
const NUMBER = 2
const BOOLEAN = 3
const DOUBLE = 4
const OBJECT = 5

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/** `list16<ProcessingInstruction>` with no entries. */
export const NO_INSTRUCTIONS: Uint8Array = new Uint8Array(2)

/** A number as an i64, without the BigInt a safe integer does not need. */
function writeI64Number(w: Writer, value: number): void {
  ensure(w, 8)
  if (Number.isSafeInteger(value)) {
    const hi = Math.floor(value / 4294967296)
    w.view.setUint32(w.pos, value - hi * 4294967296, true)
    w.view.setInt32(w.pos + 4, hi, true)
  } else {
    const big = BigInt(value)
    if (BigInt.asIntN(64, big) !== big) throw new ProtocolError(`${value} does not fit in an i64`)
    w.view.setBigInt64(w.pos, big, true)
  }
  w.pos += 8
}

/**
 * The instruction list for these hints, as it goes on the wire. Numbers are
 * `number` metadata values, the routing key `text`.
 */
export function instructionBytes(instructions: {
  routingKey?: string
  priority?: number
  timeoutMs?: number
  nrOfResults?: number
}): Uint8Array {
  const w = writer(64)
  const countAt = w.pos
  writeU16(w, 0)
  let count = 0
  const number = (key: number, value: number | undefined) => {
    if (value === undefined) return
    writeU8(w, key)
    writeU8(w, 1)
    writeU8(w, NUMBER)
    writeI64Number(w, value)
    count++
  }
  if (instructions.routingKey !== undefined) {
    writeU8(w, ProcessingKey.routing_key)
    writeU8(w, 1)
    writeU8(w, TEXT)
    writeStr32(w, instructions.routingKey, "ProcessingInstruction.routing_key")
    count++
  }
  number(ProcessingKey.priority, instructions.priority)
  number(ProcessingKey.timeout, instructions.timeoutMs)
  number(ProcessingKey.nr_of_results, instructions.nrOfResults)
  w.view.setUint16(countAt, count, true)
  return w.buf.slice(0, w.pos)
}

function writeBytes(w: Writer, bytes: Uint8Array): void {
  ensure(w, bytes.length)
  w.buf.set(bytes, w.pos)
  w.pos += bytes.length
}

/** `list16<MessageMetadata>` from the framework's metadata. */
export function writeMetadata(w: Writer, metadata: Metadata | undefined): void {
  const countAt = w.pos
  writeU16(w, 0)
  if (metadata === undefined) return
  let count = 0
  for (const key in metadata) {
    if (!Object.hasOwn(metadata, key)) continue
    const value = metadata[key]
    const kind = typeof value
    if (kind === "string") {
      writeStr16(w, key, "MessageMetadata.key")
      writeU8(w, TEXT)
      writeStr32(w, value as string, "MetadataValue.text")
    } else if (kind === "number") {
      writeStr16(w, key, "MessageMetadata.key")
      if (Number.isInteger(value)) {
        writeU8(w, NUMBER)
        writeI64Number(w, value as number)
      } else {
        writeU8(w, DOUBLE)
        writeF64(w, value as number)
      }
    } else if (kind === "boolean") {
      writeStr16(w, key, "MessageMetadata.key")
      writeU8(w, BOOLEAN)
      writeU8(w, value ? 1 : 0)
    } else if (kind === "bigint") {
      const big = value as bigint
      if (BigInt.asIntN(64, big) !== big) throw new ProtocolError(`metadata '${key}': ${big} does not fit in an i64`)
      writeStr16(w, key, "MessageMetadata.key")
      writeU8(w, NUMBER)
      ensure(w, 8)
      w.view.setBigInt64(w.pos, big, true)
      w.pos += 8
    } else if (value !== undefined && value !== null) {
      const text = JSON.stringify(value)
      // A function or symbol has no JSON form; it is not metadata.
      if (text === undefined) continue
      writeStr16(w, key, "MessageMetadata.key")
      writeU8(w, TEXT)
      writeStr32(w, text, "MetadataValue.text")
    } else {
      continue
    }
    count++
  }
  if (count > 0xffff) throw new ProtocolError(`message metadata: ${count} entries exceed the u16 limit`)
  w.view.setUint16(countAt, count, true)
}

export function writePayload(w: Writer, payload: SerializedObject): void {
  writeStr16(w, payload.type, "Payload.type_name")
  writeStr16(w, payload.revision, "Payload.revision")
  writeBytes32(w, payload.data, "Payload.data")
}

/** What a dispatcher sends: a command or a query, before it has a frame. */
export type OutgoingMessage = {
  readonly messageId: string
  readonly name: string
  readonly timestamp: number
  readonly payload: SerializedObject
  readonly metadata: Metadata | undefined
}

/** A `Command` or a `Query`: the two share a layout. `instructions` is a ready `list16`. */
export function writeMessage(
  w: Writer,
  message: OutgoingMessage,
  instructions: Uint8Array,
  clientId: string,
  componentName: string,
): void {
  writeStr16(w, message.messageId, "Command.message_id")
  writeStr16(w, message.name, "Command.name")
  writeMillis(w, message.timestamp)
  writePayload(w, message.payload)
  writeMetadata(w, message.metadata)
  writeBytes(w, instructions)
  writeStr16(w, clientId, "Command.client_id")
  writeStr16(w, componentName, "Command.component_name")
}

/** What a handler answers: the same layout for a command and for a query. */
export type OutgoingResult = {
  readonly requestId: string
  /** Empty on success. */
  readonly errorCode: string
  readonly errorMessage: string
  readonly location: string
  readonly payload: SerializedObject | undefined
}

/** A `CommandResult` or `QueryResult`, with no metadata and no instructions. */
export function writeResult(w: Writer, result: OutgoingResult): void {
  writeStr16(w, "", "Result.message_id")
  writeStr16(w, result.requestId, "Result.request_id")
  writeStr16(w, result.errorCode, "Result.error_code")
  if (result.errorCode === "") {
    writeU8(w, 0)
  } else {
    writeU8(w, 1)
    writeStr32(w, result.errorMessage, "ErrorDetail.message")
    writeStr16(w, result.location, "ErrorDetail.location")
    writeLen16(w, 0, "ErrorDetail.details")
    writeStr16(w, result.errorCode, "ErrorDetail.error_code")
  }
  if (result.payload === undefined) {
    writeU8(w, 0)
  } else {
    writeU8(w, 1)
    writePayload(w, result.payload)
  }
  writeU16(w, 0) // metadata
  writeU16(w, 0) // instructions
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

function skip(r: Reader, n: number): void {
  if (r.pos + n > r.end) throw new ProtocolError(`truncated: need ${n} bytes at offset ${r.pos}`)
  r.pos += n
}

const skipStr16 = (r: Reader): void => skip(r, readU16(r))
const skipStr32 = (r: Reader): void => skip(r, readU32(r))

function skipPayload(r: Reader): void {
  skipStr16(r)
  skipStr16(r)
  skip(r, readU32(r))
}

function skipErrorDetail(r: Reader): void {
  skipStr32(r)
  skipStr16(r)
  for (let n = readU16(r); n > 0; n--) skipStr32(r)
  skipStr16(r)
}

/** `list16<MessageMetadata>` as the framework's metadata. Typed objects are not kept: this client never sends one. */
export function readMetadata(r: Reader): Metadata {
  const count = readU16(r)
  const out: Record<string, unknown> = {}
  for (let i = 0; i < count; i++) {
    const key = readStr16(r)
    let value: unknown
    switch (readU8(r)) {
      case TEXT:
        value = readStr32(r)
        break
      case NUMBER: {
        // An i64 as a number: exact up to 2^53, which is all a JSON-shaped value holds.
        need(r, 8)
        value = r.view.getInt32(r.pos + 4, true) * 4294967296 + r.view.getUint32(r.pos, true)
        r.pos += 8
        break
      }
      case BOOLEAN:
        value = readU8(r) === 1
        break
      case DOUBLE:
        need(r, 8)
        value = r.view.getFloat64(r.pos, true)
        r.pos += 8
        break
      case OBJECT:
        skipPayload(r)
        continue
      default:
        throw new ProtocolError(`MetadataValue has no variant with that tag`)
    }
    if (key === "__proto__") Object.defineProperty(out, key, { value, enumerable: true, configurable: true, writable: true })
    else out[key] = value
  }
  return out
}

/** Metadata the codec already decoded, as the framework's: the same mapping {@link readMetadata} makes. */
export function metadataFromEntries(entries: ReadonlyArray<MessageMetadata>): Metadata {
  const out: Record<string, unknown> = {}
  for (const { key, value } of entries) {
    let converted: unknown
    switch (value.kind) {
      case "text":
      case "boolean":
      case "double":
        converted = value.value
        break
      case "number":
        converted = Number(value.value)
        break
      case "object":
        continue
    }
    if (key === "__proto__") Object.defineProperty(out, key, { value: converted, enumerable: true, configurable: true, writable: true })
    else out[key] = converted
  }
  return out
}

function need(r: Reader, n: number): void {
  if (r.pos + n > r.end) throw new ProtocolError(`truncated: need ${n} bytes at offset ${r.pos}`)
}

/** What a handler is given of a command or query. */
export type IncomingMessage = {
  readonly messageId: string
  readonly name: string
  readonly timestamp: number
  /** The serialized payload; `data` is a view into the received frame. */
  readonly payload: SerializedObject
  readonly metadata: Metadata
}

/** A `Command` or `Query` up to its metadata; the rest (instructions, sender) is not read. */
export function readMessage(r: Reader): IncomingMessage {
  const messageId = readStr16(r)
  const name = readStr16(r)
  const timestamp = readMillis(r)
  const type = readStr16(r)
  const revision = readStr16(r)
  const data = readBytes32(r)
  const metadata = readMetadata(r)
  return { messageId, name, timestamp, payload: { type, revision, data }, metadata }
}

/** What a dispatcher is given of a handler's answer. */
export type IncomingResult = {
  /** Empty on success. */
  readonly errorCode: string
  readonly errorMessage: string
  /** Absent when the handler answered with nothing. `data` is a view into the received frame. */
  readonly payload: SerializedObject | undefined
}

/** A `CommandResult` or `QueryResult`: the error code and message, or the payload. */
export function readResult(r: Reader): IncomingResult {
  skipStr16(r) // message_id
  skipStr16(r) // request_id
  const errorCode = readStr16(r)
  if (errorCode !== "") {
    // A failure is its code and its message; nothing after them is needed.
    return { errorCode, errorMessage: readPresent(r) ? readStr32(r) : "", payload: undefined }
  }
  if (readPresent(r)) skipErrorDetail(r)
  if (!readPresent(r)) return { errorCode, errorMessage: "", payload: undefined }
  const type = readStr16(r)
  const revision = readStr16(r)
  const data = readBytes32(r)
  return { errorCode, errorMessage: "", payload: { type, revision, data } }
}

// ---------------------------------------------------------------------------
// What both buses share
// ---------------------------------------------------------------------------

const DEFAULT_PERMITS = 5000

export type FlowControlConfig = {
  permits?: number
  refillThreshold?: number
}

/** The credit window a bus grants its server: how many, and when it tops up. */
export function creditWindow(flowControl: FlowControlConfig | undefined): { permits: number; threshold: number } {
  const permits = positiveInteger(flowControl?.permits ?? DEFAULT_PERMITS, "flowControl.permits")
  if (permits > 0xffffffff) throw new RangeError("flowControl.permits exceeds what one Credit frame can grant")
  const threshold = flowControl?.refillThreshold ?? Math.floor(permits / 2)
  if (threshold < 0 || threshold >= permits) throw new RangeError("refillThreshold must be between zero and permits - 1")
  return { permits, threshold }
}

/** A request deadline: a positive number of milliseconds a timer can hold. */
export function requestTimeout(timeoutMs: number | undefined): number {
  const ms = positiveInteger(timeoutMs ?? 30000, "timeoutMs")
  if (ms > 2_147_483_647) throw new RangeError("timeoutMs exceeds the timer range")
  return ms
}

/** What a serializer makes of a payload off the wire: nothing, when there was none. */
export function deserializeResult(serializer: Serializer, payload: SerializedObject | undefined): unknown {
  if (payload === undefined || payload.data.length === 0) return undefined
  return serializer.deserialize(payload)
}
