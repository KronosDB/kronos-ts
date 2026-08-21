/**
 * How a cached fold becomes a `kronos_snapshots` row, and back.
 *
 * PACKAGE-PRIVATE, and used by the ONE function that owns both sides:
 * `postgresSnapshottingEventStore` writes rows through `encodeSnapshot` and
 * reads them back inside its fused source query through `decodeSnapshot`. It
 * used to be two functions a host wired separately, which meant two serializers
 * a host could get wrong; the encoding stays in its own file because a row
 * format is worth naming, not because anybody else needs it.
 *
 * ROW SHAPE (unchanged by the seam narrowing — see `schema.ts`):
 *
 *   key          TEXT      the cache key the caller wrote — ONE column, because
 *                          it is one opaque string
 *   position     BIGINT    the last position folded into `payload`
 *   payload      BYTEA     `serializer.serialize(snapshot.state, …).data`
 *   metadata     JSONB     the envelope below — never user data
 *   recorded_at  TIMESTAMPTZ  operator-facing only; nothing reads it back
 *
 * The narrowed `Snapshot` is `{ state, position }` and nothing else — no
 * metadata, no timestamp, and no version, because snapshots have no versions.
 * So the JSONB column carries exactly the two serializer keys, and
 * `recorded_at` is filled by `now()`. Both columns stay: `recorded_at` is what
 * an operator sorts by when a cache looks wrong, and it costs a default.
 *
 * `key` holds whatever string the caller wrote — `"course-v1:{\"courseId\":\"cs-101\"}"`
 * through `state()`, or anything at all at the raw layer. Nothing here parses
 * it, which is exactly why an operator can read it.
 */

import type { Snapshot, Serializer } from "@kronos-ts/core"

/**
 * Internal envelope keys inside the `metadata` JSONB. The `__kr_` prefix dates
 * from when user metadata shared the column; it stays because the rows do.
 */
export const SERIALIZER_TYPE_KEY = "__kr_serializer_type"
export const SERIALIZER_REVISION_KEY = "__kr_serializer_revision"

export type SnapshotMetadata = Record<string, string>

/** The `payload` + `metadata` a snapshot row is written with. */
export function encodeSnapshot(
  serializer: Serializer,
  key: string,
  snapshot: Snapshot,
): { payload: Buffer; metadata: SnapshotMetadata } {
  const serialized = serializer.serialize(snapshot.state, key, "")
  return {
    payload: Buffer.from(serialized.data),
    metadata: {
      [SERIALIZER_TYPE_KEY]: serialized.type,
      [SERIALIZER_REVISION_KEY]: serialized.revision,
    },
  }
}

/**
 * A snapshot row back into a `Snapshot`.
 *
 * `payload` is whatever the driver handed back for a BYTEA — `Buffer`,
 * `Uint8Array`, or a base64 string when the fused query asked for
 * `encode(payload, 'base64')` (one query cannot mix BYTEA and event columns in
 * a UNION, so the native path asks for text).
 */
export function decodeSnapshot(
  serializer: Serializer,
  key: string,
  row: { position: string | number | bigint; payload: unknown; metadata: unknown },
): Snapshot {
  const metadata = normaliseMetadata(row.metadata)
  const state = serializer.deserialize({
    data: toBytes(row.payload),
    type: metadata[SERIALIZER_TYPE_KEY] ?? key,
    revision: metadata[SERIALIZER_REVISION_KEY] ?? "",
  })
  return { state, position: BigInt(row.position) }
}

/**
 * bunSqlAdapter returns JSONB as a raw string; pgAdapter/postgresAdapter return
 * it parsed. Normalise either way before key lookups.
 */
export function normaliseMetadata(value: unknown): SnapshotMetadata {
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as SnapshotMetadata
    } catch {
      return {}
    }
  }
  return (value as SnapshotMetadata | null) ?? {}
}

function toBytes(payload: unknown): Uint8Array {
  if (payload instanceof Uint8Array) return payload
  if (Buffer.isBuffer(payload)) {
    return new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength)
  }
  // The fused read asks for base64 text, because a BYTEA cannot ride alongside
  // the event columns in one result set without a cast.
  if (typeof payload === "string") return new Uint8Array(Buffer.from(payload, "base64"))
  return new Uint8Array(payload as ArrayBufferLike)
}
