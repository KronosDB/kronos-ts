import type { Serializer, SerializedObject } from "../primitives/converter.js"
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
export function eventSchemaRegistry(): SchemaRegistry {
  return createSchemaRegistry()
}

/** Schema registry for command payloads. */
export function commandSchemaRegistry(): SchemaRegistry {
  return createSchemaRegistry()
}

/** Schema registry for query payloads. */
export function querySchemaRegistry(): SchemaRegistry {
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
