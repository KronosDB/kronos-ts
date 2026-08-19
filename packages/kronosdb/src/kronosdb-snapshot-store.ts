import type { SnapshotStore, Snapshot } from "@kronos-ts/core"
import type { Serializer } from "@kronos-ts/core"
import { contextView, kronosMetadata } from "./connection.js"
import type { KronosDbConnectionHandle } from "./kronosdb.js"

const encoder = new TextEncoder()

/**
 * Encode a snapshot key as binary.
 * Format: stateName + NUL + id.
 */
function encodeKey(stateName: string, id: unknown): Uint8Array {
  const idStr = typeof id === "object" && id !== null ? JSON.stringify(id) : String(id)
  return encoder.encode(`${stateName}\0${idStr}`)
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
 * Uses NUL-separated keys (stateName\0id).
 *
 * `context` is the KronosDB context this store addresses — see
 * {@link kronosDbEventStore}. Both share the one channel on `kdb`.
 */
export function kronosDbSnapshotStore(
  kdb: Pick<KronosDbConnectionHandle, "connection" | "serializer">,
  context: string = kdb.connection.config.context,
): SnapshotStore {
  const connection = contextView(kdb.connection, context)
  const { snapshotToProto, snapshotFromProto } = createSnapshotConverters(kdb.serializer)

  function getMetadata() {
    return kronosMetadata(connection.config)
  }

  return {
    async store(stateName: string, id: unknown, snapshot: Snapshot): Promise<void> {
      await connection.snapshotStore.add(
        {
          key: encodeKey(stateName, id),
          sequence: snapshot.position,
          prune: true,
          snapshot: snapshotToProto(snapshot),
        },
        { metadata: getMetadata() },
      )
    },

    async load(stateName: string, id: unknown): Promise<Snapshot | undefined> {
      try {
        const response = await connection.snapshotStore.getLast(
          { key: encodeKey(stateName, id) },
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

    async deleteSnapshots(stateName: string, id: unknown): Promise<void> {
      await connection.snapshotStore.delete(
        {
          key: encodeKey(stateName, id),
          toSequence: BigInt(Number.MAX_SAFE_INTEGER),
        },
        { metadata: getMetadata() },
      )
    },
  }
}
