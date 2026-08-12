import type { Serializer, SerializedObject } from "@kronos-ts/common"
import type { z } from "zod"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

/**
 * JSON serializer — the default serializer for the TypeScript framework.
 *
 * Serializes values as JSON-encoded Uint8Array. Handles `undefined`,
 * `null`, and all JSON-compatible values.
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

/**
 * Dispatch serialization per message type, so one app can mix formats — Avro or
 * MessagePack for the high-volume streams, JSON for everything else.
 *
 * ```ts
 * const serializer = multiSerializer([avroSerializer({ registry }), jsonSerializer()])
 * ```
 *
 * Order matters: the FIRST serializer whose `canConvert(type)` returns true
 * wins, so put the specific ones before a catch-all like {@link jsonSerializer}
 * (which converts everything).
 *
 * IMPORTANT — the type→format mapping must be stable over time. `SerializedObject`
 * carries `{ type, revision, data }` and no format marker, so deserialization
 * re-runs the same `canConvert` dispatch that serialization used. Moving a type
 * from one format to another makes previously-written events undecodable. If you
 * need to migrate a type's format, keep the old serializer in the chain and
 * discriminate on `revision`.
 */
export function multiSerializer(serializers: readonly Serializer[]): Serializer {
  if (serializers.length === 0) throw new Error("multiSerializer: at least one serializer is required")

  function pick(type: string): Serializer {
    const found = serializers.find((s) => s.canConvert(type))
    if (!found) throw new Error(`multiSerializer: no serializer accepts type "${type}"`)
    return found
  }

  return {
    serialize(value: unknown, type: string, revision?: string): SerializedObject {
      return pick(type).serialize(value, type, revision)
    },
    deserialize<T>(data: SerializedObject): T {
      return pick(data.type).deserialize<T>(data)
    },
    canConvert(type: string): boolean {
      return serializers.some((s) => s.canConvert(type))
    },
  }
}

// ---------------------------------------------------------------------------
// Schema registries — per message type
// ---------------------------------------------------------------------------

/**
 * A registry of Zod schemas indexed by type name + revision.
 * Used by the validating serializer decorator to validate
 * deserialized payloads against their expected schema.
 */
export interface SchemaRegistry {
  register(typeName: string, revision: string, schema: z.ZodType): void
  get(typeName: string, revision: string): z.ZodType | undefined
}

/** Schema registry for event payloads. */
export function createEventSchemaRegistry(): SchemaRegistry {
  return createSchemaRegistry()
}

/** Schema registry for command payloads. */
export function createCommandSchemaRegistry(): SchemaRegistry {
  return createSchemaRegistry()
}

/** Schema registry for query payloads. */
export function createQuerySchemaRegistry(): SchemaRegistry {
  return createSchemaRegistry()
}

function createSchemaRegistry(): SchemaRegistry {
  const schemas = new Map<string, z.ZodType>()

  function key(typeName: string, revision: string): string {
    return `${typeName}@${revision}`
  }

  return {
    register(typeName, revision, schema) {
      schemas.set(key(typeName, revision), schema)
    },

    get(typeName, revision) {
      // Try exact match first, then fallback to no revision
      return schemas.get(key(typeName, revision)) ?? schemas.get(key(typeName, ""))
    },
  }
}

// ---------------------------------------------------------------------------
// Zod-validating serializer decorator
// ---------------------------------------------------------------------------

/**
 * Wraps a delegate serializer with Zod validation on deserialization.
 *
 * When deserializing, looks up the schema in the registry by type name
 * and revision. If found, validates the deserialized value against it.
 * If not found, passes through without validation.
 *
 * ```typescript
 * const serializer = zodValidatingSerializer(
 *   jsonSerializer(),
 *   mySchemaRegistry,
 * )
 * ```
 */
export function zodValidatingSerializer(
  delegate: Serializer,
  schemaRegistry: SchemaRegistry,
): Serializer {
  return {
    serialize(value, type, revision) {
      return delegate.serialize(value, type, revision)
    },

    deserialize<T>(data: SerializedObject): T {
      const raw = delegate.deserialize<unknown>(data)
      const schema = schemaRegistry.get(data.type, data.revision)
      if (schema) {
        return schema.parse(raw) as T
      }
      return raw as T
    },

    canConvert(type) {
      return delegate.canConvert(type)
    },
  }
}
