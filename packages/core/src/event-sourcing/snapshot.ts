// ---------------------------------------------------------------------------
// SNAPSHOTTING IS A CACHE OVER THE FOLD, and everything in this file follows
// from that one word.
//
// THERE IS NO SNAPSHOT STORE SEAM. Snapshotting is not a second thing beside
// the log — it is a CAPABILITY TIER on the log, added by wrapping an event
// store in its family's `…SnapshottingEventStore`. What lives here is the
// VOCABULARY the fold and the wrappers share: what one cached fold IS
// ({@link Snapshot}), when one is due ({@link SnapshotPolicy}), what a state
// declares ({@link SnapshotConfig}) and how an id becomes part of a key. The
// CAPABILITY is `SnapshotCapableEventStore` in `event-store.ts`; the DEMAND
// that makes wiring it a compile-time obligation is `IfSnapshotCapable` in
// `load.ts`; the FITNESS CHECK is `structural-fitness.ts`, beside the
// repository that asks it.
//
// A snapshot is not a fact and not a record. It is the answer to a fold that
// somebody already computed, kept so the next reader does not have to compute
// it again. The log stays the truth; the snapshot is a shortcut to it. Which
// gives the mechanism its three rules, and they are the whole design:
//
//   - LATEST ONLY. There is no history of snapshots, because a cache has no
//     history — it has a current entry. `store` REPLACES; `load` answers with
//     the one entry or with nothing.
//   - NEVER MIGRATED. A cache entry you cannot use is DISCARDED, not converted.
//     Upcasting exists because events are kept forever and must keep meaning
//     something; a cache entry that has stopped meaning something is thrown
//     away and recomputed.
//   - NEVER LOAD-BEARING. Every read path falls back to full sourcing on a
//     miss, an unusable entry, or an outright failure to reach the cache. A
//     snapshot store that is down makes loads slower and nothing else.
//
// THE KEY IS YOURS. A snapshot is filed under a STRING YOU WROTE — at the raw
// layer you pass it to `ctx.source(query, { snapshot })` and to
// `eventStore.storeSnapshot(key, …)`; through `state()` you declare it once as
// `snapshot: { key, when }`. Nothing is derived from your code, nothing is
// hashed, and nothing about the framework's opinion of your fold enters into
// it.
//
// WHICH MAKES INVALIDATION ONE SENTENCE: CHANGED THE FOLD'S MEANING? CHANGE THE
// KEY. Rename `"course-v1"` to `"course-v2"` and every old entry becomes
// unreachable in the same instant, without a migration, a backfill or a
// version column. It is the same move you already make for a cache anywhere
// else, it is visible in the diff, and it happens exactly when you decide it
// should rather than when a heuristic guesses.
//
// The framework's job is to hold the key you gave it and hand back what is
// filed under it. Deciding when two folds are the same fold is a judgement
// about meaning, and meaning is not derivable.
// ---------------------------------------------------------------------------

/**
 * What a load actually cost — the input a {@link SnapshotPolicy} judges.
 */
export type EvolutionResult = {
  /** Number of events folded on top of whatever the load started from. */
  readonly eventsApplied: number
  /** Wall time spent sourcing and folding, in milliseconds. */
  readonly sourcingTimeMs: number
}

/**
 * When a snapshot of a state is due — the WRITE trigger, and nothing else.
 *
 * It rides on the STATE value (`state({ snapshot: { key, when } })`) because how
 * often a state is worth caching is a property of its event volume, not of the
 * log it happens to be written to. At the raw layer there is no policy at all —
 * you call `eventStore.storeSnapshot(...)` when you decide one is due.
 */
export type SnapshotPolicy = {
  /** Whether this load earned a new cache entry. */
  shouldSnapshot(result: EvolutionResult): boolean

  /** Combine with another policy — the result triggers if either does. */
  or(other: SnapshotPolicy): SnapshotPolicy
}

/** Triggers once a load has folded more than `threshold` events. */
export function afterEvents(threshold: number): SnapshotPolicy {
  return createPolicy((result) => result.eventsApplied > threshold)
}

/** Triggers once a load has spent at least `thresholdMs` sourcing. */
export function whenSourcingTimeExceeds(thresholdMs: number): SnapshotPolicy {
  return createPolicy((result) => result.sourcingTimeMs >= thresholdMs)
}

/** A policy that never triggers. No snapshots are written. */
export function noSnapshotPolicy(): SnapshotPolicy {
  return createPolicy(() => false)
}

function createPolicy(
  predicate: (result: EvolutionResult) => boolean,
): SnapshotPolicy {
  const policy: SnapshotPolicy = {
    shouldSnapshot: predicate,
    or(other: SnapshotPolicy): SnapshotPolicy {
      return createPolicy(
        (result) => predicate(result) || other.shouldSnapshot(result),
      )
    },
  }
  return policy
}

/**
 * ONE cached fold: the state value, and the position it is folded up to.
 *
 * TWO FIELDS. There is no version, no timestamp and no metadata — see the
 * doctrine at the top of this file. `position` is the CONSISTENCY MARKER's
 * position, the same `bigint` a store hands back from a `source`. A reader
 * resumes at `position + 1`, so a snapshot says "every event up to and
 * including here is already in `state`".
 */
export type Snapshot = {
  /** The folded state. Opaque to the mechanism; the fold that wrote it owns its shape. */
  readonly state: unknown
  /** The last position folded INTO `state`. Resume at `position + 1`. */
  readonly position: bigint
}

/**
 * A state's snapshotting configuration: WHERE entries are filed, and WHEN one
 * is written.
 *
 * Both are FIELDS rather than a pipeline because neither happens before the
 * other in any meaningful sense — one governs reads, one governs writes.
 *
 * ```ts
 * snapshot: { key: "course-v1", when: afterEvents(100) }
 * ```
 *
 * `key` is REQUIRED. A state that snapshots must say where, because "where" is
 * a decision about MEANING that only you can make: two folds are the same
 * cache exactly when you say they are.
 */
export type SnapshotConfig = {
  /**
   * THE CACHE KEY, and the whole invalidation story.
   *
   * CHANGED THE FOLD'S MEANING? CHANGE THE KEY. Rename `"course-v1"` to
   * `"course-v2"` and every entry the old fold wrote becomes unreachable in the
   * same instant — no migration, no backfill, no version column, no
   * heuristic guessing on your behalf. It is one character in a diff, it is
   * reviewable, and it happens exactly when you decide it should.
   *
   * You do not have to change it for a refactor that preserves meaning, and
   * nothing will change it behind your back for one that does not.
   *
   * `state()` files entries under `"<key>:<flattened id>"` — see
   * {@link snapshotIdentifier} — so one key serves every instance of the state
   * without them colliding.
   */
  readonly key: string
  /** When a snapshot is due. */
  readonly when: SnapshotPolicy
}

/**
 * A state id, flattened into the string `state()` appends to your key.
 *
 * Object keys are SORTED, so `{ courseId, studentId }` and
 * `{ studentId, courseId }` name the same entry — id construction order is not
 * part of an identity. A bigint keeps its `n` suffix so `1n` and `"1"` stay
 * distinct. A string id is itself, unquoted, so the common single-field case
 * reads as itself in a database row.
 *
 * IT IS PART OF A CACHE KEY, so changing this encoding breaks nothing: every
 * old entry simply stops being found, and is recomputed and overwritten.
 *
 * A RAW USER NEED NOT USE IT. `ctx.source(query, { snapshot })` takes whatever
 * string you like — `\`course:${courseId}\`` reads better in a database row
 * than JSON does, and nothing downstream parses either.
 */
export function snapshotIdentifier(id: unknown): string {
  if (typeof id === "string") return id
  const encoded = JSON.stringify(id, (_key, value: unknown) => {
    if (typeof value === "bigint") return `${value}n`
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      const sorted: Record<string, unknown> = {}
      for (const key of Object.keys(value).sort()) {
        sorted[key] = (value as Record<string, unknown>)[key]
      }
      return sorted
    }
    return value
  })
  return encoded ?? String(id)
}
