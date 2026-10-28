// The vendored codec against the server repository's golden frames: every
// fixture decodes to its canonical JSON and encodes back to the identical
// bytes. Adapted from protocol/ts/conformance.test.ts in the KronosDB checkout
// (node:test there, bun:test here); the fixtures and the codec are copied by
// `scripts/vendor-protocol.ts` and never edited.

import { describe, expect, it } from "bun:test"
import { readdirSync, readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import * as P from "../kronosdb-protocol.js"
import { frameBodyToJson, frameFromJson } from "./kronosdb-protocol-json.js"

/**
 * The schema hash this package is built against. Changing it is the review
 * point for a wire change: re-run `scripts/vendor-protocol.ts`, update this
 * line, and let the fixtures below prove the new codec.
 */
const PINNED_SCHEMA_HASH = "a65e0eff"

const here = dirname(fileURLToPath(import.meta.url))
const fixtures = join(here, "fixtures")
const names = readdirSync(fixtures)
  .filter((f) => f.endsWith(".bin") && !f.startsWith("."))
  .map((f) => f.slice(0, -".bin".length))
  .sort()

const hex = (b: Uint8Array) => Buffer.from(b).toString("hex")

/** CRC32C (Castagnoli), bitwise: the schema is read once, so speed is irrelevant. */
function crc32c(bytes: Uint8Array): string {
  let crc = 0xffffffff
  for (const byte of bytes) {
    crc ^= byte
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (crc & 1 ? 0x82f63b78 : 0)
  }
  return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0")
}

describe("vendored protocol codec", () => {
  it("is built from the schema this package pins", () => {
    expect(P.SCHEMA_HASH).toBe(PINNED_SCHEMA_HASH)
  })

  it("agrees with the vendored schema file it claims to be generated from", () => {
    expect(crc32c(readFileSync(join(here, "..", "kronosdb.schema")))).toBe(P.SCHEMA_HASH)
  })

  it("speaks the protocol version this client implements", () => {
    expect(P.PROTOCOL_VERSION).toBe(1)
  })

  it("has the fixtures", () => {
    expect(names.length).toBeGreaterThanOrEqual(30)
  })

  for (const name of names) {
    it(`fixture ${name}: decodes to its JSON and encodes to its bytes`, () => {
      const bin = new Uint8Array(readFileSync(join(fixtures, `${name}.bin`)))
      const expected = JSON.parse(readFileSync(join(fixtures, `${name}.json`), "utf8"))
      delete expected.description

      const header = P.readFrameHeader(bin)
      expect(header).toBeDefined()
      expect(header!.end).toBe(bin.length)
      const frame = P.decodeFrameBody(header!.kind, P.reader(bin, header!.bodyStart, header!.end))
      expect(frame).toBeDefined()
      expect({ frame: frame!.name, stream: header!.stream, flags: header!.flags, body: frameBodyToJson(frame!) }).toEqual(expected)

      const w = P.writer(16)
      P.encodeFrame(w, expected.stream, expected.flags, frameFromJson(expected.frame, expected.body))
      expect(hex(P.written(w))).toBe(hex(bin))
    })
  }

  it("round-trips every metadata value variant", () => {
    const w = P.writer()
    const values: P.MetadataValue[] = [
      { kind: "text", value: "ünïcode" },
      { kind: "number", value: -42n },
      { kind: "boolean", value: false },
      { kind: "double", value: Math.PI },
      { kind: "object", value: { typeName: "T", revision: "", data: new Uint8Array([1, 2]) } },
    ]
    for (const v of values) P.encodeMetadataValue(w, v)
    const r = P.reader(P.written(w))
    for (const v of values) expect(P.decodeMetadataValue(r)).toEqual(v)
    expect(r.pos).toBe(w.pos)
    expect(() => P.decodeMetadataValue(P.reader(new Uint8Array([9])))).toThrow(P.ProtocolError)
  })

  it("rejects truncated frames instead of misreading them", () => {
    const bin = new Uint8Array(readFileSync(join(fixtures, "append-conditional.bin")))
    const header = P.readFrameHeader(bin)!
    for (let end = header.bodyStart; end < header.end; end++) {
      expect(() => P.decodeFrameBody(header.kind, P.reader(bin, header.bodyStart, end))).toThrow(P.ProtocolError)
    }
    expect(P.readFrameHeader(bin.subarray(0, bin.length - 1))).toBeUndefined()
  })

  it("fails to encode strings that do not fit their length field", () => {
    const w = P.writer()
    expect(() => P.encodeFrame(w, 0, 0, { name: "BindContext", body: { id: 1, name: "x".repeat(70_000) } })).toThrow(
      /BindContext\.name/,
    )
  })
})
