// ---------------------------------------------------------------------------
// KRONOSDB SNAPSHOTS — the capability tier, served NATIVELY by the log itself.
//
// SNAPSHOTS RIDE THE LOG (KronosDB ADR-0005). There is no snapshot store beside
// the event store any more — the old standalone `SnapshotStore` service
// (`Add`/`Delete`/`List`/`GetLast`) is gone from the server, and with it the
// second durability domain it created. A snapshot is now one system record on
// the replicated log, written through the same append path as any event, and
// the EventStore service serves it back over two RPCs:
//
//   AppendSnapshot     — the write. One round trip, opaque bytes under an
//                        opaque key. The server never interprets the state.
//   SnapshottedSource  — THE FUSED READ. The latest snapshot under the key
//                        (one SnapshotResult frame, always first), then the
//                        events matching the criteria from that snapshot's fold
//                        marker onward, then the same consistency marker a plain
//                        `Source` ends with. ONE call, ONE consistent view.
//
// SO THE FUSION IS THE SERVER'S NOW. This file used to do it client-side —
// `getLast` for the entry, then a second `source` after its position, the two
// results assembled here into the one `SourcingResult` a fold expects. That is
// deleted, and there is NO FALLBACK to it: the native RPC is THE path, and a
// server that does not serve it fails loudly rather than quietly costing twice.
//
// AND THE FUSION WAS SUBTLY WRONG, which is the part worth keeping in mind.
// Client-side, it resumed at `snapshot.position + 1`. But a KronosDB marker is
// NEXT-EXCLUSIVE — it is already the sequence a replay resumes AT — so the `+1`
// could step over an event that landed between the fold and the snapshot write.
// The server resumes at `position` exactly, for exactly this reason. The
// off-by-one did not survive the move, and no caller has to know it was there.
// ---------------------------------------------------------------------------

import type { EventStore, Snapshot, SnapshotStoreCapability, SourcingCondition, SourcingResult } from "@kronos-ts/core"
import type { ConsistencyMarker, EventMessage, Serializer } from "@kronos-ts/core"
import { markerAt, noMarker } from "@kronos-ts/core"
import { sourceCriteria } from "./kronosdb-event-store.js"
import type { KronosDbConnectionHandle } from "./kronosdb.js"
import { readEvents, writeCriteria } from "./native-events.js"
import { contextRequest, contextStream, unexpected } from "./native-requests.js"
import {
  FLAG_END,
  FrameKind,
  decodeSnapshotResult,
  encodeAppendSnapshot,
  reader,
  writeBytes16,
  writeU16,
  writeU32,
} from "./protocol/kronosdb-protocol.js"

const encoder = new TextEncoder()

/**
 * Encode the snapshot key as binary. It is one opaque string the caller wrote,
 * so there is nothing to compose and nothing to escape.
 */
function encodeKey(key: string): Uint8Array {
  return encoder.encode(key)
}

/**
 * The wire snapshot is TWO FIELDS — opaque `state` bytes and the fold-time
 * `position` — and so is {@link Snapshot}. There is no name, no version, no
 * timestamp and no metadata map to fill in or leave empty: the server stores
 * what it is given and hands it back byte-exact, and everything the entry means
 * is the client's business.
 *
 * The serializer's type tag is the key itself, which is what the client-side
 * era did too — a cache entry is read back by the same fold that wrote it.
 */
function createSnapshotConverters(serializer: Serializer) {
  return {
    stateToBytes(key: string, snapshot: Snapshot): Uint8Array {
      return serializer.serialize(snapshot.state, key, "").data
    },

    snapshotFromWire(key: string, state: Uint8Array, position: bigint): Snapshot {
      return {
        state: state.length > 0 ? serializer.deserialize({ data: state, type: key, revision: "" }) : {},
        position,
      }
    },
  }
}

/**
 * Add the snapshotting capability to a KronosDB-backed log.
 *
 * ```ts
 * const eventStore = kronosDbSnapshottingEventStore(
 *   kronosDbEventStore(kdb, "default"),
 *   kdb,
 *   "default",
 * )
 * ```
 *
 * ADDITIVE, NOT COLLAPSING. It returns `E & SnapshotStoreCapability` — the store you
 * passed in, plus the write — so capabilities stack in either order and nothing
 * the inner store carried is laundered on the way through.
 *
 * `context` is the KronosDB context this capability addresses — see
 * {@link kronosDbEventStore}. Both share the one socket on `kdb`.
 *
 * LATEST-ONLY, over a log that keeps everything. Every snapshot write appends a
 * record; a newer one supersedes older ones purely by being later, and the
 * server resolves "the latest under this key" on read. Nothing is pruned and
 * nothing is deleted, because deletion from an append-only log is not free and
 * superseding is.
 */
export function kronosDbSnapshottingEventStore<E extends EventStore>(
  next: E,
  kdb: Pick<KronosDbConnectionHandle, "connection" | "serializer">,
  context: string = kdb.connection.config.context,
): E & SnapshotStoreCapability {
  const link = kdb.connection.native
  const { stateToBytes, snapshotFromWire } = createSnapshotConverters(kdb.serializer)

  return {
    ...next,

    /**
     * Write the cached fold — ONE RPC, and FIRE-AND-FORGET by contract.
     *
     * The snapshot record is appended on its own, AFTER the transaction whose
     * events it summarizes has committed; it is not part of that transaction
     * and there is no unit of work to enlist in. That is the contract, not a
     * shortcut: the log is append-only and a snapshot is a cache entry, so a
     * write that lands is a saving and a write that does not is a replay. The
     * repository already calls this outside the task for exactly that reason.
     *
     * `snapshot.position` goes over UNMODIFIED. It is the marker the fold was
     * computed against, and the server stores it as the sequence a later replay
     * resumes AT — no arithmetic here, in either direction.
     */
    async storeSnapshot(key: string, snapshot: Snapshot): Promise<void> {
      const state = stateToBytes(key, snapshot)
      await contextRequest(
        link,
        context,
        FrameKind.AppendSnapshot,
        (w, id) => encodeAppendSnapshot(w, { context: id, key: encodeKey(key), state, position: snapshot.position }),
        (kind) => {
          if (kind !== FrameKind.SnapshotAppended) throw unexpected(kind, "SnapshotAppended")
        },
      )
    },

    /**
     * THE FUSED READ — one `SnapshottedSource` request, one consistent view.
     *
     * The answer is a SnapshotResult frame first — holding the snapshot, or
     * nothing when the key has none — then Events frames. The final one carries
     * the consistency marker exactly as a plain `Source` does, which is what
     * makes an append condition built from this read hold identically on both
     * paths. A key with no snapshot behaves as a plain `Source` from the
     * beginning.
     *
     * The server has ALREADY applied the snapshot's floor to the events. What
     * it cannot express is a `start` the condition asked for independently —
     * `SnapshottedSource` has no `from` — so that floor is composed here,
     * against the position each event arrives with. The two narrowings compose
     * instead of fighting, as they do in the SQL family.
     */
    async source(condition: SourcingCondition): Promise<SourcingResult> {
      const key = condition.snapshot
      if (key === undefined) return next.source(condition)

      const criteria = sourceCriteria(condition.query)
      const wireKey = encodeKey(key.key)
      const events: EventMessage[] = []
      let marker: ConsistencyMarker = noMarker()
      let snapshot: Snapshot | undefined

      return new Promise<SourcingResult>((resolve, reject) => {
        contextStream(
          link,
          context,
          FrameKind.SnapshottedSource,
          (w, id) => {
            writeU16(w, id)
            writeCriteria(w, criteria)
            writeBytes16(w, wireKey, "snapshot key")
            // Events per frame; 0 lets the server pick its default.
            writeU32(w, 0)
          },
          true,
          (kind, flags, buf, start, end) => {
            if (kind === FrameKind.SnapshotResult) {
              const found = decodeSnapshotResult(reader(buf, start, end)).snapshot
              if (found !== undefined) snapshot = snapshotFromWire(key.key, found.state, found.position)
            } else if (kind === FrameKind.Events) {
              // The final batch carries the marker; 0n is a valid marker for an
              // empty store, so presence — not truthiness — decides.
              const reached = readEvents(buf, start, end, kdb.serializer, events, condition.start)
              if (reached !== undefined) marker = markerAt(reached)
            } else {
              throw unexpected(kind, "SnapshotResult or Events")
            }
            if ((flags & FLAG_END) !== 0) resolve({ events, marker, ...(snapshot !== undefined ? { snapshot } : {}) })
          },
          reject,
        ).catch(reject)
      })
    },
  } as E & SnapshotStoreCapability
}
