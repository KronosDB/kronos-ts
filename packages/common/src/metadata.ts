/**
 * Message metadata — a read-only string-keyed map of arbitrary values.
 * Carries cross-cutting information like correlation IDs, trace context,
 * user identity, etc.
 *
 * Metadata is immutable — all transformation methods return new instances.
 */
export type Metadata = Readonly<Record<string, unknown>>

/**
 * Well-known metadata keys used by the framework.
 */
export const MetadataKeys = {
  CORRELATION_ID: "correlationId",
  CAUSATION_ID: "causationId",
  TRACE_ID: "traceId",
} as const

/**
 * Creates an empty metadata object.
 */
export function emptyMetadata(): Metadata {
  return {}
}

/**
 * Creates metadata with a single entry.
 */
export function metadataWith(key: string, value: unknown): Metadata {
  return { [key]: value }
}

/**
 * Merges two metadata objects. Values from `override` take precedence.
 */
export function mergeMetadata(base: Metadata, override: Metadata): Metadata {
  return { ...base, ...override }
}

/**
 * Returns new metadata with the given entry added (or replaced).
 */
export function metadataAnd(metadata: Metadata, key: string, value: unknown): Metadata {
  return { ...metadata, [key]: value }
}

/**
 * Returns new metadata with the given entry added only if not already present.
 */
export function metadataAndIfNotPresent(metadata: Metadata, key: string, supplier: () => unknown): Metadata {
  if (key in metadata) return metadata
  return { ...metadata, [key]: supplier() }
}

/**
 * Returns new metadata with the specified keys removed.
 */
export function metadataWithoutKeys(metadata: Metadata, ...keys: string[]): Metadata {
  const result: Record<string, unknown> = { ...metadata }
  for (const key of keys) {
    delete result[key]
  }
  return result
}

/**
 * Returns new metadata containing only the specified keys.
 */
export function metadataSubset(metadata: Metadata, ...keys: string[]): Metadata {
  const result: Record<string, unknown> = {}
  for (const key of keys) {
    if (key in metadata) {
      result[key] = metadata[key]
    }
  }
  return result
}

/**
 * Check if metadata contains a specific key.
 */
export function metadataContains(metadata: Metadata, key: string): boolean {
  return key in metadata
}
