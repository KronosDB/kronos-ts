/**
 * A segment: a durable share of a processor's sequences, with its own token and
 * its own claim. An event belongs to the segment whose id equals the hash of its
 * sequence id, masked: `(hash & mask) === id`.
 *
 * Segments form a binary tree. Splitting `{ id, mask }` gives `{ id, mask' }`
 * and `{ id + mask + 1, mask' }` with `mask' = mask * 2 + 1`, so a sequence only
 * ever moves into one of its segment's two halves, and its order is kept. The
 * mask is never stored: the set of segment ids a processor has is enough to work
 * every mask out ({@link segmentsOf}).
 */
export type Segment = { readonly id: number; readonly mask: number }

/** The one segment a processor that was never split has. */
export const WHOLE: Segment = { id: 0, mask: 0 }

/** The two halves of a segment. */
export function splitSegment(segment: Segment): readonly [Segment, Segment] {
  const mask = segment.mask * 2 + 1
  return [
    { id: segment.id, mask },
    { id: segment.id + segment.mask + 1, mask },
  ]
}

/** The id of the segment this one merges with: its other half. */
export function mergeableSegmentId(segment: Segment): number {
  const parent = segment.mask >>> 1
  return segment.id ^ (segment.mask ^ parent)
}

/**
 * The segments a processor has, from the ids of its token rows: walk down from
 * the whole, splitting while the second half of a split is one of the ids.
 */
export function segmentsOf(ids: ReadonlyArray<number>): Segment[] {
  const present = new Set(ids)
  const found: Segment[] = []
  const walk = (segment: Segment) => {
    const [first, second] = splitSegment(segment)
    if (segment.mask < 2 ** 30 && present.has(second.id)) {
      walk(first)
      walk(second)
    } else {
      found.push(segment)
    }
  }
  if (ids.length > 0) walk(WHOLE)
  return found.sort((a, b) => a.id - b.id)
}

/** Whether a sequence with this hash belongs to the segment. */
export function inSegment(segment: Segment, hash: number): boolean {
  return segment.mask === 0 || (hash & segment.mask) === segment.id
}

/**
 * A sequence id's hash: 32-bit FNV-1a over the string's UTF-16 code units. The
 * same on every runtime and every machine, which is the whole requirement — a
 * sequence must land in the same segment on every node, now and after a
 * restart.
 */
export function sequenceHash(id: string): number {
  let hash = 0x811c9dc5
  for (let i = 0; i < id.length; i++) {
    hash ^= id.charCodeAt(i)
    hash = Math.imul(hash, 0x01000193)
  }
  return hash >>> 0
}
