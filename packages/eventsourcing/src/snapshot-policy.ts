/**
 * Metrics collected during entity state evolution (event sourcing).
 * Used by snapshot policies to decide if a new snapshot should be created.
 *
 * Aligned with Kronos Framework's `EvolutionResult`.
 */
export interface EvolutionResult {
  /** Number of events applied to reach the current state. */
  readonly eventsApplied: number
  /** Time spent sourcing in milliseconds. */
  readonly sourcingTimeMs: number
}

/**
 * Determines when snapshots should be created for an entity.
 *
 * Policies are composable via `or()` — if any policy triggers,
 * a snapshot is created.
 *
 * Aligned with Kronos Framework's `SnapshotPolicy`.
 */
export interface SnapshotPolicy {
  /**
   * Whether a snapshot should be created based on the evolution result.
   */
  shouldSnapshot(result: EvolutionResult): boolean

  /**
   * Combine this policy with another. The combined policy triggers
   * if either policy triggers.
   */
  or(other: SnapshotPolicy): SnapshotPolicy
}

/**
 * Creates a snapshot policy that triggers after N events have been applied.
 *
 * Aligned with Kronos Framework's `SnapshotPolicy.afterEvents()`.
 */
export function afterEvents(threshold: number): SnapshotPolicy {
  return createPolicy((result) => result.eventsApplied > threshold)
}

/**
 * Creates a snapshot policy that triggers when sourcing time exceeds
 * the given threshold in milliseconds.
 *
 * Aligned with Kronos Framework's `SnapshotPolicy.whenSourcingTimeExceeds()`.
 */
export function whenSourcingTimeExceeds(thresholdMs: number): SnapshotPolicy {
  return createPolicy((result) => result.sourcingTimeMs >= thresholdMs)
}

/**
 * A policy that never triggers. No snapshots will be created.
 */
export function noSnapshotPolicy(): SnapshotPolicy {
  return createPolicy(() => false)
}

// ---------------------------------------------------------------------------
// Internal helper
// ---------------------------------------------------------------------------

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
