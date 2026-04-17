/**
 * Represents a fraction of the event stream assigned to a processor instance.
 *
 * Segments use bitmask-based routing to deterministically assign events
 * to processors. An event matches a segment when `(hash(event) & mask) === segmentId`.
 *
 * Segments can be split (doubling parallelism) and merged (halving parallelism).
 * This is the foundation for horizontal scaling — multiple instances of the
 * same processor each claim different segments.
 *
 * The ROOT_SEGMENT (segmentId=0, mask=0) matches ALL events and is the
 * starting point before any splitting.
 */
export interface Segment {
  /** The unique identifier of this segment. */
  readonly segmentId: number
  /**
   * The bitmask used to match events to this segment.
   * An event matches when `(hash & mask) === segmentId`.
   */
  readonly mask: number
}

/** The root segment — matches all events. Starting point before splitting. */
export const ROOT_SEGMENT: Segment = { segmentId: 0, mask: 0 }

/**
 * Create a segment with the given id and mask.
 */
export function segment(segmentId: number, mask: number): Segment {
  return { segmentId, mask }
}

/**
 * Check if a hash value matches this segment.
 * Used to route events to the correct processor instance.
 *
 * @param seg The segment to check against
 * @param hash The hash of the event's sequence identifier (e.g., aggregate ID hash)
 */
export function segmentMatches(seg: Segment, hash: number): boolean {
  return (hash & seg.mask) === seg.segmentId
}

/**
 * Split a segment into two child segments.
 * Doubles the processing parallelism for this segment's portion of the stream.
 *
 * Returns a tuple of [segment keeping the original ID, new sibling segment].
 */
export function splitSegment(seg: Segment): [Segment, Segment] {
  const newMask = (seg.mask << 1) | 1
  const newSegmentId = seg.segmentId | (seg.mask + 1)

  return [
    { segmentId: seg.segmentId, mask: newMask },
    { segmentId: newSegmentId, mask: newMask },
  ]
}

/**
 * Check if two segments can be merged (they are siblings from the same split).
 */
export function isMergeable(a: Segment, b: Segment): boolean {
  if (a.mask !== b.mask) return false
  if (a.mask === 0) return false
  // Siblings differ only in the highest bit of the mask
  return (a.segmentId ^ b.segmentId) === (a.mask >>> 0) - (a.mask >>> 1)
}

/**
 * Merge two sibling segments back into their parent.
 * Halves the processing parallelism.
 *
 * @throws Error if segments are not mergeable
 */
export function mergeSegments(a: Segment, b: Segment): Segment {
  if (!isMergeable(a, b)) {
    throw new Error(
      `Segments ${a.segmentId}/${a.mask} and ${b.segmentId}/${b.mask} are not mergeable`,
    )
  }

  return {
    segmentId: Math.min(a.segmentId, b.segmentId),
    mask: a.mask >>> 1,
  }
}

/**
 * Compute the total number of segments at the current split level.
 * For a segment with mask M, the total count is M + 1.
 */
export function segmentCount(seg: Segment): number {
  return seg.mask + 1
}

/**
 * Compute a hash from a string value (for event routing).
 * Uses a simple but well-distributed hash function.
 */
export function hashOf(value: string): number {
  let hash = 0
  for (let i = 0; i < value.length; i++) {
    const char = value.charCodeAt(i)
    hash = ((hash << 5) - hash + char) | 0
  }
  return hash >>> 0 // Ensure unsigned
}

/**
 * Create N balanced segments by splitting the root segment.
 *
 * @param count Number of segments (must be a power of 2)
 * @returns Array of segments that together cover the entire event stream
 */
export function createSegments(count: number): Segment[] {
  if (count <= 0) throw new Error("Segment count must be positive")
  if (count === 1) return [ROOT_SEGMENT]

  // Find the nearest power of 2
  const power = Math.ceil(Math.log2(count))
  const actualCount = Math.pow(2, power)

  let segments: Segment[] = [ROOT_SEGMENT]
  while (segments.length < actualCount) {
    const next: Segment[] = []
    for (const seg of segments) {
      const [a, b] = splitSegment(seg)
      next.push(a, b)
    }
    segments = next
  }

  return segments
}
