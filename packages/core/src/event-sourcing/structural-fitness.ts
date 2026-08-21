// ---------------------------------------------------------------------------
// THE SECOND GATE: is this cached value even the right KIND of thing?
//
// THE CACHE KEY IS THE FIRST GATE, and it is the one that handles MEANING. You
// wrote it; you change it when the fold's meaning changes. Nothing here second-
// guesses that, and nothing here is a substitute for it.
//
// This is the SAFETY NET underneath, for the ways a cached value can come back
// wrong without anybody having changed a key:
//
//   - STORAGE CORRUPTION. A truncated BYTEA, a half-written row, a value that
//     came back from the wire as something other than what went in.
//   - SERIALIZER DRIFT. The bytes were written by one encoder and read by
//     another — a different `Serializer` wired at the composition root, a
//     custom codec whose round trip is not quite the identity, a JSON revival
//     that turned a number into a string.
//   - SHAPE DRIFT YOU DID NOT NOTICE. You added a field to the initial state
//     and did not think of it as a change of meaning, so the key stayed. Every
//     old entry now lacks a key the fold reads — which is the failure mode that
//     actually bites, and it is caught here rather than corrupting a fold.
//
// It costs nothing to run and nothing to maintain, because the specimen is data
// the fold already computes: `evolve[0]` is the INITIAL STATE, a live example of
// the shape this fold works in, and it cannot drift from the fold because it IS
// the fold's first line. NO CODE IS INSPECTED — this reads a VALUE, the same
// way the fold does.
// ---------------------------------------------------------------------------

/**
 * Does `candidate` still have the shape `specimen` describes?
 *
 * `specimen` is a freshly-built initial state — the current code's own example of the
 * shape — and `candidate` is a deserialized snapshot. The comparison is
 * ONE-DIRECTIONAL and deliberately generous:
 *
 * - Every key the specimen has, the candidate must have too, with a recursively
 *   matching `typeof`. THIS IS THE HAZARD THE CHECK EXISTS FOR: new code that
 *   reads a field old snapshots do not carry, which is silently `undefined`
 *   and corrupts the fold from there on.
 * - EXTRA keys on the candidate are FINE. A field you removed leaves leftovers
 *   in the cache; the fold ignores them, so the check does too.
 * - Objects recurse. Arrays compare every element against the specimen array's
 *   FIRST element, which is the only element an initial state can be teaching with.
 * - An EMPTY array in the specimen teaches nothing about its elements, so
 *   anything array-shaped passes. Same for a `null` or `undefined` leaf: an initial state
 *   that starts a field at `null` is saying nothing about what it becomes.
 * - A key whose specimen value is `undefined` is skipped entirely, presence
 *   included — such a field does not survive a JSON round trip, so demanding it
 *   would fail every snapshot ever written.
 *
 * WHAT IT DOES NOT CATCH, said plainly: a change that keeps the structure and
 * changes the MEANING. Cents becoming dollars, an enum growing a case — all of
 * them are `number` before and `number` after, and no structural check can see
 * the difference. THAT IS WHAT THE KEY IS FOR, and why the key is yours: only
 * you know that the number means something new, so only you can decide the old
 * entries are dead. Change the key.
 *
 * Pure, total, and allocation-free apart from `Object.keys`. Never throws:
 * "not fit" is an answer, not an error.
 */
export function matchesInitialStructure(specimen: unknown, candidate: unknown): boolean {
  // An initial-state leaf that is null/undefined is not describing a type. Nothing to
  // check, so nothing fails.
  if (specimen === null || specimen === undefined) return true

  if (Array.isArray(specimen)) {
    if (!Array.isArray(candidate)) return false
    // Element zero is the only element an initial state can teach with — and an empty
    // empty one teaches nothing at all.
    const element = specimen[0]
    if (specimen.length === 0 || element === null || element === undefined) return true
    return candidate.every((item) => matchesInitialStructure(element, item))
  }

  if (typeof specimen === "object") {
    if (candidate === null || typeof candidate !== "object" || Array.isArray(candidate)) {
      return false
    }
    const seedRecord = specimen as Record<string, unknown>
    const candidateRecord = candidate as Record<string, unknown>
    for (const key of Object.keys(seedRecord)) {
      const expected = seedRecord[key]
      // `undefined` does not survive serialization, so its key is not required
      // to have come back.
      if (expected === undefined) continue
      if (!(key in candidateRecord)) return false
      if (!matchesInitialStructure(expected, candidateRecord[key])) return false
    }
    return true
  }

  // A primitive leaf: the specimen names a type, and the candidate must be it.
  // `typeof` is the whole comparison — a `bigint` that came back a `string` is
  // exactly the kind of drift worth catching, and a `number` that came back a
  // `number` is all this level can ever know.
  return typeof candidate === typeof specimen
}

