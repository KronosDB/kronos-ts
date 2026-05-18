/**
 * A point-in-time capture of a state value.
 *
 */
export interface Snapshot {
  /** The global position in the event stream at snapshot time. */
  readonly position: bigint
  /** The serialized state. */
  readonly payload: unknown
  /** When the snapshot was created (epoch ms). */
  readonly timestamp: number
  /** Optional metadata associated with the snapshot. */
  readonly metadata: Record<string, string>
}

/**
 * Stores and retrieves state snapshots.
 *
 * Separate from the event store — snapshots are an optimization, not
 * part of the event stream.
 *
 */
export interface SnapshotStore {
  /**
   * Store a snapshot for a state value.
   * Replaces any existing snapshot for the same state and id.
   */
  store(stateName: string, id: unknown, snapshot: Snapshot): Promise<void>

  /**
   * Load the most recent snapshot for a state value.
   * Returns undefined if no snapshot exists.
   */
  load(stateName: string, id: unknown): Promise<Snapshot | undefined>

  /**
   * Delete all snapshots for a state value.
   */
  deleteSnapshots(stateName: string, id: unknown): Promise<void>
}

/**
 * In-memory snapshot store for testing and standalone usage.
 */
export function createInMemorySnapshotStore(): SnapshotStore {
  // Key: "stateName:id"
  const snapshots = new Map<string, Snapshot>()

  function key(stateName: string, id: unknown): string {
    const idStr = typeof id === "object" && id !== null ? JSON.stringify(id) : String(id)
    return `${stateName}:${idStr}`
  }

  return {
    async store(stateName, id, snapshot) {
      snapshots.set(key(stateName, id), snapshot)
    },

    async load(stateName, id) {
      return snapshots.get(key(stateName, id))
    },

    async deleteSnapshots(stateName, id) {
      snapshots.delete(key(stateName, id))
    },
  }
}
