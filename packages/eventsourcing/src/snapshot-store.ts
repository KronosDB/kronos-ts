/**
 * A point-in-time capture of an entity's state.
 *
 * Aligned with Kronos Framework's `Snapshot`.
 */
export interface Snapshot {
  /** The global position in the event stream at snapshot time. */
  readonly position: bigint
  /** The serialized entity state. */
  readonly payload: unknown
  /** When the snapshot was created (epoch ms). */
  readonly timestamp: number
  /** Optional metadata associated with the snapshot. */
  readonly metadata: Record<string, string>
}

/**
 * Stores and retrieves entity snapshots.
 *
 * Separate from the event store — snapshots are an optimization, not
 * part of the event stream.
 *
 * Aligned with Kronos Framework's `SnapshotStore`.
 */
export interface SnapshotStore {
  /**
   * Store a snapshot for an entity.
   * Replaces any existing snapshot for the same entity.
   */
  store(entityName: string, id: unknown, snapshot: Snapshot): Promise<void>

  /**
   * Load the most recent snapshot for an entity.
   * Returns undefined if no snapshot exists.
   */
  load(entityName: string, id: unknown): Promise<Snapshot | undefined>

  /**
   * Delete all snapshots for an entity.
   */
  deleteSnapshots(entityName: string, id: unknown): Promise<void>
}

/**
 * In-memory snapshot store for testing and standalone usage.
 */
export function createInMemorySnapshotStore(): SnapshotStore {
  // Key: "entityName:id"
  const snapshots = new Map<string, Snapshot>()

  function key(entityName: string, id: unknown): string {
    const idStr = typeof id === "object" && id !== null ? JSON.stringify(id) : String(id)
    return `${entityName}:${idStr}`
  }

  return {
    async store(entityName, id, snapshot) {
      snapshots.set(key(entityName, id), snapshot)
    },

    async load(entityName, id) {
      return snapshots.get(key(entityName, id))
    },

    async deleteSnapshots(entityName, id) {
      snapshots.delete(key(entityName, id))
    },
  }
}
