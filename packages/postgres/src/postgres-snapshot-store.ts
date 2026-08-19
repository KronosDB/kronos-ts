/**
 * Postgres-backed SnapshotStore implementation.
 *
 * Schema (from src/schema.ts):
 *   kronos_snapshots (
 *     state_name  TEXT,
 *     state_id    TEXT,
 *     position    BIGINT,
 *     payload     BYTEA,
 *     metadata    JSONB,
 *     recorded_at TIMESTAMPTZ,
 *     PRIMARY KEY (state_name, state_id)
 *   )
 *
 * Payload roundtrip:
 *   store: Serializer.serialize(payload, type) -> SerializedObject { data, type, revision }
 *          .data goes to BYTEA, .type/.revision plus user metadata into JSONB.
 *   load:  BYTEA + JSONB -> reconstruct SerializedObject -> Serializer.deserialize.
 *
 * State id stringification matches inMemorySnapshotStore: objects are
 * JSON-stringified, everything else gets String(). The eventsourcing protocol
 * passes `unknown` ids, and the kronos_snapshots.state_id column is TEXT.
 */

import type { Snapshot, SnapshotStore } from "@kronos-ts/core"
import type { Serializer } from "@kronos-ts/core"
import type { PostgresResource } from "./postgres-pool.js"

export interface PostgresSnapshotStoreConfig {
  /**
   * How a state's payload becomes bytes. Required, not defaulted: the
   * `payload` column is BYTEA and what goes into it is the application's
   * choice — `jsonSerializer()` for the common case, something else when
   * snapshots must survive a schema change.
   */
  readonly serializer: Serializer
}

function stateIdToString(id: unknown): string {
  return typeof id === "object" && id !== null ? JSON.stringify(id) : String(id)
}

// Internal envelope keys we co-locate in the metadata JSONB alongside any
// user-supplied snapshot.metadata entries. The "__kr_" prefix avoids
// colliding with user keys.
const SERIALIZER_TYPE_KEY = "__kr_serializer_type"
const SERIALIZER_REVISION_KEY = "__kr_serializer_revision"

export function postgresSnapshotStore(
  pg: PostgresResource,
  config: PostgresSnapshotStoreConfig,
): SnapshotStore {
  const adapter = pg
  const { serializer } = config
  const tables = pg.tables

  return {
    async store(stateName: string, id: unknown, snapshot: Snapshot): Promise<void> {
      const stateId = stateIdToString(id)
      const serialized = serializer.serialize(snapshot.payload, stateName, "")
      const payloadBuf = Buffer.from(serialized.data)
      const metadata = {
        ...snapshot.metadata,
        [SERIALIZER_TYPE_KEY]: serialized.type,
        [SERIALIZER_REVISION_KEY]: serialized.revision,
      }
      // recorded_at uses to_timestamp(epoch_seconds); snapshot.timestamp is milliseconds.
      await adapter.query(
        `INSERT INTO ${tables.snapshots}
           (state_name, state_id, position, payload, metadata, recorded_at)
         VALUES ($1, $2, $3::bigint, $4, $5::jsonb, to_timestamp($6))
         ON CONFLICT (state_name, state_id) DO UPDATE
           SET position    = EXCLUDED.position,
               payload     = EXCLUDED.payload,
               metadata    = EXCLUDED.metadata,
               recorded_at = EXCLUDED.recorded_at`,
        [
          stateName,
          stateId,
          String(snapshot.position),
          payloadBuf,
          JSON.stringify(metadata),
          snapshot.timestamp / 1000,
        ],
      )
    },

    async load(stateName: string, id: unknown): Promise<Snapshot | undefined> {
      const stateId = stateIdToString(id)
      const row = await adapter.queryOne<{
        position: string
        payload: unknown
        metadata: Record<string, string>
        recorded_at: Date | string | number
      }>(
        `SELECT position::text AS position, payload, metadata, recorded_at
           FROM ${tables.snapshots}
          WHERE state_name = $1 AND state_id = $2`,
        [stateName, stateId],
      )
      if (!row) return undefined

      // Convert BYTEA result to Uint8Array for the deserializer
      let payloadBytes: Uint8Array
      if (row.payload instanceof Uint8Array) {
        payloadBytes = row.payload
      } else if (Buffer.isBuffer(row.payload)) {
        payloadBytes = new Uint8Array(row.payload.buffer, row.payload.byteOffset, row.payload.byteLength)
      } else {
        payloadBytes = new Uint8Array((row.payload as unknown as Buffer))
      }

      // bunSqlAdapter returns JSONB as a raw string; pgAdapter/postgresAdapter
      // return it parsed. Normalise either way before key lookups.
      const rawMetadata: Record<string, string> =
        typeof row.metadata === "string"
          ? (JSON.parse(row.metadata) as Record<string, string>)
          : (row.metadata ?? {})
      const serializedType = rawMetadata[SERIALIZER_TYPE_KEY] ?? stateName
      const serializedRevision = rawMetadata[SERIALIZER_REVISION_KEY] ?? ""
      const userMetadata: Record<string, string> = {}
      for (const [k, v] of Object.entries(rawMetadata)) {
        if (k !== SERIALIZER_TYPE_KEY && k !== SERIALIZER_REVISION_KEY) userMetadata[k] = v
      }

      const payload = serializer.deserialize({
        data: payloadBytes,
        type: serializedType,
        revision: serializedRevision,
      })

      // Reconstruct timestamp as milliseconds from what the DB returned
      let timestamp: number
      if (row.recorded_at instanceof Date) {
        timestamp = row.recorded_at.getTime()
      } else if (typeof row.recorded_at === "string") {
        timestamp = new Date(row.recorded_at).getTime()
      } else {
        timestamp = Number(row.recorded_at)
      }

      return {
        position: BigInt(row.position),
        payload,
        timestamp,
        metadata: userMetadata,
      }
    },

    async deleteSnapshots(stateName: string, id: unknown): Promise<void> {
      const stateId = stateIdToString(id)
      await adapter.query(
        `DELETE FROM ${tables.snapshots} WHERE state_name = $1 AND state_id = $2`,
        [stateName, stateId],
      )
    },
  }
}
