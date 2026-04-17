import type { SnapshotStore, Snapshot } from "@kronos-ts/eventsourcing"
import type { Serializer } from "@kronos-ts/common"
import type { KronosDbConnection } from "./connection.js"
import { createKronosMetadata } from "./connection.js"

const encoder = new TextEncoder()

/**
 * Encode a snapshot key as binary.
 * Format: entityName + NUL + id (matching the Java connector's key format).
 */
function encodeKey(entityName: string, id: unknown): Uint8Array {
  const idStr = typeof id === "object" && id !== null ? JSON.stringify(id) : String(id)
  return encoder.encode(`${entityName}\0${idStr}`)
}

function createSnapshotConverters(serializer: Serializer) {
  return {
    snapshotToProto(snapshot: Snapshot): any {
      const serialized = serializer.serialize(snapshot.payload, "snapshot", "")
      return {
        name: "",
        version: "",
        payload: serialized.data,
        timestamp: BigInt(snapshot.timestamp),
        metadata: snapshot.metadata ?? {},
      }
    },

    snapshotFromProto(proto: any, position: bigint): Snapshot {
      const payload = proto.payload && proto.payload.length > 0
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

/**
 * Creates a SnapshotStore backed by KronosDB's gRPC snapshot service.
 *
 * Uses NUL-separated keys (entityName\0id) matching the Java connector's format.
 */
export function createKronosDbSnapshotStore(
  connection: KronosDbConnection,
  serializer: Serializer,
): SnapshotStore {
  const { snapshotToProto, snapshotFromProto } = createSnapshotConverters(serializer)

  function getMetadata() {
    return createKronosMetadata(connection.config)
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
        { metadata: getMetadata() },
      )
    },

    async load(entityName: string, id: unknown): Promise<Snapshot | undefined> {
      try {
        const response = await connection.snapshotStore.getLast(
          { key: encodeKey(entityName, id) },
          { metadata: getMetadata() },
        )

        if (!response.snapshot) {
          return undefined
        }

        return snapshotFromProto(response.snapshot, response.sequence)
      } catch (err) {
        // KronosDB returns empty response when no snapshot exists
        if (String(err).includes("No snapshot found") || String(err).includes("NOT_FOUND")) {
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
        { metadata: getMetadata() },
      )
    },
  }
}
