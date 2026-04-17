import type { SnapshotStore, Snapshot } from "@kronos-ts/eventsourcing"
import type { Serializer } from "@kronos-ts/common"
import type { AxonServerConnection } from "./connection.js"
import type {
  Snapshot as ProtoSnapshot,
} from "./generated/dcb.js"
import { Metadata } from "nice-grpc"

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
      const payload = proto.payload.length > 0
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

function encodeKey(entityName: string, id: unknown): Uint8Array {
  const idStr = typeof id === "object" && id !== null ? JSON.stringify(id) : String(id)
  return encoder.encode(`${entityName}:${idStr}`)
}

// ---------------------------------------------------------------------------
// Axon Server snapshot store
// ---------------------------------------------------------------------------

/**
 * Creates a SnapshotStore backed by Axon Server's gRPC snapshot service.
 *
 * Uses the `DcbSnapshotStore` gRPC service to store and retrieve
 * entity state snapshots. Payload serialization uses the configured
 * Serializer (defaults to JSON).
 */
export function createAxonServerSnapshotStore(
  connection: AxonServerConnection,
  serializer: Serializer,
): SnapshotStore {
  const { snapshotToProto, snapshotFromProto } = createSnapshotConverters(serializer)

  function createAxonMetadata(): Metadata {
    const axonMetadata = new Metadata()
    axonMetadata.set("AxonIQ-Context", connection.config.context)
    if (connection.config.token) {
      axonMetadata.set("AxonIQ-Access-Token", connection.config.token)
    }
    return axonMetadata
  }

  return {
    async store(entityName: string, id: unknown, snapshot: Snapshot): Promise<void> {
      await connection.snapshotStore.add(
        {
          key: encodeKey(entityName, id),
          sequence: snapshot.position,
          prune: true,
          snapshot: snapshotToProto(snapshot),
        },
        { metadata: createAxonMetadata() },
      )
    },

    async load(entityName: string, id: unknown): Promise<Snapshot | undefined> {
      try {
        const response = await connection.snapshotStore.getLast(
          { key: encodeKey(entityName, id) },
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

    async deleteSnapshots(entityName: string, id: unknown): Promise<void> {
      await connection.snapshotStore.delete(
        {
          key: encodeKey(entityName, id),
          toSequence: BigInt(Number.MAX_SAFE_INTEGER),
        },
        { metadata: createAxonMetadata() },
      )
    },
  }
}
