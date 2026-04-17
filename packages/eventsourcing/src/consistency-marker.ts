import { resourceKey, type ResourceKey } from "@kronos-ts/common"

/**
 * An opaque marker representing a position in the event store.
 * Used by append conditions to guarantee consistency —
 * "no conflicting events were appended since this marker."
 */
export interface ConsistencyMarker {
  readonly position: bigint
}

/** Marker representing the origin (before any events). */
export const ORIGIN: ConsistencyMarker = { position: -1n }

/** Marker representing infinity (no consistency check needed). */
export const INFINITY: ConsistencyMarker = { position: BigInt(Number.MAX_SAFE_INTEGER) }

/** Resource key for storing a ConsistencyMarker in a ProcessingContext. */
export const MARKER_RESOURCE_KEY: ResourceKey<ConsistencyMarker> = resourceKey("consistencyMarker")

export function noMarker(): ConsistencyMarker {
  return ORIGIN
}

export function markerAt(position: bigint): ConsistencyMarker {
  return { position }
}

/**
 * Returns the lower bound of two markers — the most restrictive position.
 * When checking consistency across multiple sourcing operations,
 * use the lower bound to ensure no events are missed.
 */
export function markerLowerBound(a: ConsistencyMarker, b: ConsistencyMarker): ConsistencyMarker {
  return a.position < b.position ? a : b
}

/**
 * Returns the upper bound of two markers — the least restrictive position.
 */
export function markerUpperBound(a: ConsistencyMarker, b: ConsistencyMarker): ConsistencyMarker {
  return a.position > b.position ? a : b
}
