import type { Serializer, SerializedObject } from "@kronos-ts/common"

/**
 * An intermediate representation of an event during the upcasting pipeline.
 *
 * After deserialization from bytes (JSON.parse) but before Zod validation,
 * the event passes through the upcaster chain as an IntermediateEventRepresentation.
 * Each upcaster can transform the payload, type name, revision, and metadata.
 */
export interface IntermediateEventRepresentation {
  /** The deserialized payload (raw JSON, not yet Zod-validated). */
  readonly payload: unknown
  /** The qualified type name (e.g., "university.CourseCreated"). */
  readonly typeName: string
  /** The schema revision (e.g., "1.0"). */
  readonly revision: string
  /** Event metadata. */
  readonly metadata: Record<string, unknown>
}

/**
 * An event upcaster transforms events from one schema version to another.
 *
 * Upcasters run after JSON deserialization but before Zod validation,
 * so the payload is raw parsed JSON. The upcaster can add fields, rename
 * fields, change the type name, bump the revision, etc.
 *
 * An upcaster can return:
 * - A single representation (one-to-one transform)
 * - An array of representations (one-to-many, e.g., splitting events)
 * - An empty array (filtering out an event)
 */
export interface EventUpcaster {
  /** Whether this upcaster can handle the given type + revision. */
  canUpcast(typeName: string, revision: string): boolean

  /**
   * Transform the intermediate representation.
   * Return the upcasted representation(s).
   */
  upcast(event: IntermediateEventRepresentation): IntermediateEventRepresentation | IntermediateEventRepresentation[]
}

/**
 * Convenience function for the common case: a single event type
 * being transformed from one revision to the next.
 *
 * ```typescript
 * const upcaster = singleEventUpcaster({
 *   typeName: "university.CourseCreated",
 *   fromRevision: "1.0",
 *   toRevision: "2.0",
 *   upcast: (payload: any) => ({
 *     ...payload,
 *     capacity: payload.capacity ?? 30,  // added in v2
 *   }),
 * })
 * ```
 */
export function singleEventUpcaster(options: {
  typeName: string
  fromRevision: string
  toRevision: string
  upcast: (payload: unknown) => unknown
  upcastMetadata?: (metadata: Record<string, unknown>) => Record<string, unknown>
}): EventUpcaster {
  return {
    canUpcast(typeName, revision) {
      return typeName === options.typeName && revision === options.fromRevision
    },

    upcast(event) {
      return {
        payload: options.upcast(event.payload),
        typeName: options.typeName,
        revision: options.toRevision,
        metadata: options.upcastMetadata
          ? options.upcastMetadata(event.metadata)
          : event.metadata,
      }
    },
  }
}

/**
 * Chains multiple upcasters into a single upcaster.
 * Upcasters are applied in order. Each upcaster's output becomes
 * the next upcaster's input. Handles one-to-many expansion.
 *
 * ```typescript
 * const chain = upcasterChain(
 *   courseCreatedV1ToV2,
 *   courseCreatedV2ToV3,
 * )
 * ```
 */
export function upcasterChain(...upcasters: EventUpcaster[]): EventUpcaster {
  return {
    canUpcast(typeName, revision) {
      return upcasters.some((u) => u.canUpcast(typeName, revision))
    },

    upcast(event) {
      let representations: IntermediateEventRepresentation[] = [event]

      for (const upcaster of upcasters) {
        const next: IntermediateEventRepresentation[] = []
        for (const rep of representations) {
          if (upcaster.canUpcast(rep.typeName, rep.revision)) {
            const result = upcaster.upcast(rep)
            if (Array.isArray(result)) {
              next.push(...result)
            } else {
              next.push(result)
            }
          } else {
            next.push(rep)
          }
        }
        representations = next
      }

      return representations.length === 1 ? representations[0]! : representations
    },
  }
}

/**
 * Creates an upcasting serializer decorator.
 *
 * On deserialization, the raw payload is first deserialized by the delegate,
 * then passed through the upcaster chain before being returned. The type name
 * and revision from the SerializedObject drive upcaster selection.
 *
 * ```typescript
 * const serializer = upcastingSerializer(
 *   jsonSerializer(),
 *   upcasterChain(v1ToV2, v2ToV3),
 * )
 * ```
 */
export function upcastingSerializer(
  delegate: Serializer,
  upcaster: EventUpcaster,
): Serializer {
  return {
    serialize(value, type, revision) {
      return delegate.serialize(value, type, revision)
    },

    deserialize<T>(data: SerializedObject): T {
      const raw = delegate.deserialize<unknown>(data)

      if (!upcaster.canUpcast(data.type, data.revision)) {
        return raw as T
      }

      const representation: IntermediateEventRepresentation = {
        payload: raw,
        typeName: data.type,
        revision: data.revision,
        metadata: {},
      }

      const result = upcaster.upcast(representation)
      const upcasted = Array.isArray(result) ? result[0]! : result
      return upcasted.payload as T
    },

    canConvert(type) {
      return delegate.canConvert(type)
    },
  }
}
