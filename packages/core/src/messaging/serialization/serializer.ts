import type { Serializer, SerializedObject } from "./converter.js"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

/**
 * JSON serializer — the default serializer for the TypeScript framework.
 *
 * Serializes values as JSON-encoded Uint8Array. Handles `undefined`,
 * `null`, and all JSON-compatible values.
 *
 * A SERIALIZER ENCODES. It does not validate, and there is no decorator here
 * that does: validation happens where the DESCRIPTOR is — at the handling
 * boundary (`validatingHandler`) and at the edge (`validate`) — and a serializer
 * has only a type name and a revision, which is why it used to need a registry
 * to look a schema up by. Nobody has to ask that question any more. See
 * `validation/`.
 */
export function jsonSerializer(): Serializer {
  return {
    serialize(value: unknown, type: string, revision: string = ""): SerializedObject {
      return {
        type,
        revision,
        data: encoder.encode(JSON.stringify(value)),
      }
    },

    deserialize<T>(data: SerializedObject): T {
      if (data.data.length === 0) return undefined as T
      return JSON.parse(decoder.decode(data.data)) as T
    },

    canConvert(): boolean {
      return true
    },
  }
}
