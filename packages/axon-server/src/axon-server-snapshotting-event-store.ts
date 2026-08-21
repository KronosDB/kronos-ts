// ---------------------------------------------------------------------------
// AXON SERVER SNAPSHOTS — the capability tier, served by `DcbSnapshotStore`.
//
// THE PROBE, AND WHAT IT SETTLED. Axon Server's public API defines both a
// snapshot STORE service (`DcbSnapshotStore`: Add / Delete / List / GetLast)
// and, on newer API drops, a fused `SnapshottedDcbEventStore.Source` whose
// stream leads with the snapshot. Only the first of those is actually served:
//
//   axoniq/axonserver:2025.2.5   DcbSnapshotStore/GetLast   → answers
//                                SnapshottedDcbEventStore/Source → UNIMPLEMENTED
//   axoniq/axonserver:2026.0.4   same, both ways
//
// So there is no server-side fusion to call here — there is nothing on the
// other end of the wire. The READ is therefore fused CLIENT-SIDE: `GetLast` for
// the entry, then a source after its position, assembled into the one
// `SourcingResult` a fold expects. Two round trips, the same answers, correct
// today.
//
// When a server version does serve `SnapshottedDcbEventStore.Source`, the fused
// call lands INSIDE this function — `source` stops making two calls and starts
// making one — and no host changes a line, because the capability was never a
// promise about round trips.
// ---------------------------------------------------------------------------

import type {
  EventStore,
  Snapshot,
  SnapshotCapability,
  SourcingCondition,
  SourcingResult,
} from "@kronos-ts/core"
import type { Serializer } from "@kronos-ts/core"
import { withoutSnapshotKey } from "@kronos-ts/core"
import type { AxonServerStoreSource } from "./connection.js"
import { contextView } from "./context-view.js"
import type { Snapshot as ProtoSnapshot } from "./generated/dcb.js"

// ---------------------------------------------------------------------------
// Conversion — framework Snapshot ↔ proto Snapshot
// ---------------------------------------------------------------------------

const encoder = new TextEncoder()

/**
 * The proto message carries `name`, `version`, `timestamp` and a metadata map
 * the narrowed seam has no equivalent for. `name` carries the cache key,
 * `timestamp` is filled at write time for operators, and `version` stays EMPTY
 * — snapshots have no versions, and inventing one to fill a proto field would
 * be exactly the bookkeeping this design removed. The metadata map stays empty
 * too: a cache entry carries no cargo.
 */
function createSnapshotConverters(serializer: Serializer) {
  return {
    snapshotToProto(name: string, snapshot: Snapshot): ProtoSnapshot {
      const serialized = serializer.serialize(snapshot.state, name, "")
      return {
        name,
        version: "",
        payload: serialized.data,
        timestamp: BigInt(Date.now()),
        metadata: {},
      }
    },

    snapshotFromProto(name: string, proto: ProtoSnapshot, position: bigint): Snapshot {
      const state =
        proto.payload.length > 0
          ? serializer.deserialize({ data: proto.payload, type: name, revision: "" })
          : {}

      return { state, position }
    },
  }
}

function encodeKey(key: string): Uint8Array {
  return encoder.encode(key)
}

// ---------------------------------------------------------------------------
// Axon Server snapshot store
// ---------------------------------------------------------------------------

/**
 * Add the snapshotting capability to an Axon Server-backed log.
 *
 * ```ts
 * const eventStore = axonServerSnapshottingEventStore(
 *   axonServerEventStore(axon, "default"),
 *   axon,
 *   "default",
 * )
 * ```
 *
 * ADDITIVE, NOT COLLAPSING. It returns `E & SnapshotCapability` — the store you
 * passed in, plus the write — so capabilities stack in either order and nothing
 * the inner store carried is laundered on the way through.
 *
 * `context` is a per-call header, so this shares the one channel `conn` holds
 * with every other context — see `contextView`. Payloads go through the
 * connection's serializer.
 *
 * LATEST-ONLY, over a service that is not. `DcbSnapshotStore` keeps a sequence
 * of snapshots per key; the capability keeps one. `add` therefore sets
 * `prune: true`, so writing an entry retires the ones before it, and `getLast`
 * is the only read. That is the narrowing doing its job: the framework programs
 * against the cache it needs, not against everything the backend happens to
 * offer.
 */
export function axonServerSnapshottingEventStore<E extends EventStore>(
  next: E,
  conn: AxonServerStoreSource,
  context: string,
): E & SnapshotCapability {
  const { connection, serializer, metadata: createAxonMetadata } = contextView(conn, context)
  const { snapshotToProto, snapshotFromProto } = createSnapshotConverters(serializer)

  /** The cached fold filed under `key`, or nothing — the first of the two calls. */
  async function loadSnapshot(key: string): Promise<Snapshot | undefined> {
    try {
      const response = await connection.snapshotStore.getLast(
        { key: encodeKey(key) },
        { metadata: createAxonMetadata() },
      )

      if (!response.snapshot) {
        return undefined
      }

      return snapshotFromProto(key, response.snapshot, response.sequence)
    } catch (err) {
      // Axon Server throws rather than answering empty when no snapshot
      // exists — and a cache miss is not an error to anybody upstream.
      if (
        String(err).includes("No snapshot found") ||
        String(err).includes("not found")
      ) {
        return undefined
      }
      throw err
    }
  }

  return {
    ...next,

    async storeSnapshot(key: string, snapshot: Snapshot): Promise<void> {
      await connection.snapshotStore.add(
        {
          key: encodeKey(key),
          sequence: snapshot.position,
          prune: true,
          snapshot: snapshotToProto(key, snapshot),
        },
        { metadata: createAxonMetadata() },
      )
    },

    /**
     * THE CLIENT-SIDE FUSION. Two calls, assembled into the one result a fold
     * expects — and the cache is NEVER LOAD-BEARING, so a miss or an outright
     * throw from the snapshot service both fall through to a full read.
     */
    async source(condition: SourcingCondition): Promise<SourcingResult> {
      const key = condition.snapshot
      if (key === undefined) return next.source(condition)

      // The strategy is CONSUMED here; the store below gets a plain condition.
      const plain = withoutSnapshotKey(condition)

      let snapshot: Snapshot | undefined
      try {
        snapshot = await loadSnapshot(key.key)
      } catch {
        // A cache you cannot reach is a cache miss. Loads stay correct; they
        // just cost what they always cost.
        return next.source(plain)
      }
      if (snapshot === undefined) return next.source(plain)

      // Resume AFTER the position the snapshot already folded; a condition that
      // independently asked to start later keeps its own floor.
      const resumeFrom = snapshot.position + 1n
      const start =
        plain.start !== undefined && plain.start > resumeFrom ? plain.start : resumeFrom

      const result = await next.source({ ...plain, start })
      return { ...result, snapshot }
    },
  } as E & SnapshotCapability
}
