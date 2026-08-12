import { describe, expect, it } from "bun:test"
import type { SerializedObject, Serializer } from "@kronos-ts/common"
import { jsonSerializer, multiSerializer } from "../serializer.js"

/**
 * A stand-in binary format. Real ones (MessagePack, CBOR, Avro) live in their
 * own packages — `SerializedObject.data` is already `Uint8Array`, so they need
 * no change to the Serializer contract.
 */
function upperBytesSerializer(prefix: string): Serializer {
  return {
    serialize(value, type, revision = ""): SerializedObject {
      return { type, revision, data: new TextEncoder().encode(JSON.stringify(value).toUpperCase()) }
    },
    deserialize<T>(data: SerializedObject): T {
      return JSON.parse(new TextDecoder().decode(data.data).toLowerCase()) as T
    },
    canConvert(type: string): boolean {
      return type.startsWith(prefix)
    },
  }
}

describe("multiSerializer", () => {
  const serializer = multiSerializer([upperBytesSerializer("loud."), jsonSerializer()])

  it("routes a type to the first serializer that accepts it", () => {
    const loud = serializer.serialize({ a: "x" }, "loud.Shout")
    expect(new TextDecoder().decode(loud.data)).toBe('{"A":"X"}')

    const plain = serializer.serialize({ a: "x" }, "quiet.Whisper")
    expect(new TextDecoder().decode(plain.data)).toBe('{"a":"x"}')
  })

  it("round-trips through the same serializer that wrote it", () => {
    const written = serializer.serialize({ a: "x" }, "loud.Shout")
    expect(serializer.deserialize(written)).toEqual({ a: "x" })
  })

  it("a catch-all last still lets specific formats win", () => {
    // jsonSerializer.canConvert() is true for everything, so ordering is what
    // makes the mix work at all.
    expect(serializer.canConvert("loud.Shout")).toBe(true)
    expect(serializer.canConvert("anything.Else")).toBe(true)
  })

  it("throws when nothing accepts the type", () => {
    const strict = multiSerializer([upperBytesSerializer("loud.")])
    expect(() => strict.serialize({}, "quiet.Whisper")).toThrow(/no serializer accepts/)
  })

  it("rejects an empty chain", () => {
    expect(() => multiSerializer([])).toThrow(/at least one/)
  })
})
