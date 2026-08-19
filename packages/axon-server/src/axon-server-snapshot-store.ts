import type { SnapshotStore, Snapshot } from "@kronos-ts/core"
import type { Serializer } from "@kronos-ts/core"
import type { AxonServerStoreSource } from "./connection.js"
import { contextView } from "./context-view.js"
import type { Snapshot as ProtoSnapshot } from "./generated/dcb.js"

// ---------------------------------------------------------------------------
// Conversion — framework Snapshot ↔ proto Snapshot
// ---------------------------------------------------------------------------

const encoder = new TextEncoder()

function createSnapshotConverters(serializer: Serializer) {
  return {
    snapshotToProto(snapshot: Snapshot): ProtoSnapshot {
      const serialized = serializer.serialize(snapshot.payload, "snapshot", "")
      return {
        name: "",
        version: "",
        payload: serialized.data,
        timestamp: BigInt(snapshot.timestamp),
        metadata: snapshot.metadata,
      }
    },

    snapshotFromProto(proto: ProtoSnapshot, position: bigint): Snapshot {
      const payload =
        proto.payload.length > 0
          ? serializer.deserialize({ data: proto.payload, type: "snapshot", revision: "" })
          : {}

      return {
        position,
        payload,
        timestamp: Number(proto.timestamp),
        metadata: proto.metadata ?? {},
      }
    },
  }
}

function encodeKey(stateName: string, id: unknown): Uint8Array {
  const idStr = typeof id === "object" && id !== null ? JSON.stringify(id) : String(id)
  return encoder.encode(`${stateName}:${idStr}`)
}

// ---------------------------------------------------------------------------
// Axon Server snapshot store
// ---------------------------------------------------------------------------

/**
 * A SnapshotStore over Axon Server's gRPC snapshot service, in one context.
 *
 * Uses the `DcbSnapshotStore` gRPC service to store and retrieve state
 * snapshots. `context` is a per-call header, so this shares the one channel
 * `conn` holds with every other context — see `contextView`. Payloads go
 * through the connection's serializer.
 */
export function axonServerSnapshotStore(
  conn: AxonServerStoreSource,
  context: string,
): SnapshotStore {
  const { connection, serializer, metadata: createAxonMetadata } = contextView(conn, context)
  const { snapshotToProto, snapshotFromProto } = createSnapshotConverters(serializer)

  return {
    async store(stateName: string, id: unknown, snapshot: Snapshot): Promise<void> {
      await connection.snapshotStore.add(
        {
          key: encodeKey(stateName, id),
          sequence: snapshot.position,
          prune: true,
          snapshot: snapshotToProto(snapshot),
        },
        { metadata: createAxonMetadata() },
      )
    },

    async load(stateName: string, id: unknown): Promise<Snapshot | undefined> {
      try {
        const response = await connection.snapshotStore.getLast(
          { key: encodeKey(stateName, id) },
          { metadata: createAxonMetadata() },
        )

        if (!response.snapshot) {
          return undefined
        }

        return snapshotFromProto(response.snapshot, response.sequence)
      } catch (err) {
        // Axon Server throws when no snapshot exists — treat as "not found"
        if (String(err).includes("No snapshot found")) {
          return undefined
        }
        throw err
      }
    },

    async deleteSnapshots(stateName: string, id: unknown): Promise<void> {
      await connection.snapshotStore.delete(
        {
          key: encodeKey(stateName, id),
          toSequence: BigInt(Number.MAX_SAFE_INTEGER),
        },
        { metadata: createAxonMetadata() },
      )
    },
  }
}
