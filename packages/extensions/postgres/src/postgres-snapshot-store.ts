/**
 * Postgres-backed SnapshotStore implementation.
 *
 * Schema (from src/schema.ts):
 *   kronos_snapshots (
 *     entity_name TEXT,
 *     entity_id   TEXT,
 *     position    BIGINT,
 *     payload     BYTEA,
 *     metadata    JSONB,
 *     recorded_at TIMESTAMPTZ,
 *     PRIMARY KEY (entity_name, entity_id)
 *   )
 *
 * Payload roundtrip:
 *   store: Serializer.serialize(snapshot.payload) -> Uint8Array -> BYTEA
 *   load:  BYTEA -> Uint8Array -> Serializer.deserialize -> snapshot.payload
 *
 * Why BYTEA (not JSONB) for payload: the framework Serializer can produce
 * arbitrary binary encodings (Avro, MessagePack, Protobuf wire), not just
 * JSON. BYTEA preserves the bytes exactly with no JSONB normalisation cost.
 * The events table uses JSONB because the engine wants `@>` query support;
 * snapshots are opaque to Postgres.
 */

import type { Snapshot, SnapshotStore } from "@kronos-ts/eventsourcing"
import type { PostgresAdapter } from "./adapter.js"
import { type TableNames, DEFAULT_TABLE_NAMES } from "./schema.js"

export interface Serializer {
  serialize(value: unknown): Uint8Array
  deserialize<T = unknown>(bytes: Uint8Array): T
}

export interface PostgresSnapshotStoreConfig {
  readonly adapter: PostgresAdapter
  readonly serializer: Serializer
  readonly tableNames?: TableNames
}

export function createPostgresSnapshotStore(
  config: PostgresSnapshotStoreConfig,
): SnapshotStore {
  const { adapter, serializer } = config
  const tables = config.tableNames ?? DEFAULT_TABLE_NAMES

  return {
    async store(entityName: string, entityId: string, snapshot: Snapshot): Promise<void> {
      const payloadBytes = serializer.serialize(snapshot.payload)
      // pg accepts Buffer / Uint8Array for BYTEA. Convert to Buffer for pg driver compatibility.
      const payloadBuf = Buffer.from(payloadBytes)
      // recorded_at uses to_timestamp(epoch_seconds); snapshot.timestamp is milliseconds.
      await adapter.query(
        `INSERT INTO ${tables.snapshots}
           (entity_name, entity_id, position, payload, metadata, recorded_at)
         VALUES ($1, $2, $3::bigint, $4, $5::jsonb, to_timestamp($6))
         ON CONFLICT (entity_name, entity_id) DO UPDATE
           SET position    = EXCLUDED.position,
               payload     = EXCLUDED.payload,
               metadata    = EXCLUDED.metadata,
               recorded_at = EXCLUDED.recorded_at`,
        [
          entityName,
          entityId,
          String(snapshot.position),
          payloadBuf,
          JSON.stringify(snapshot.metadata),
          snapshot.timestamp / 1000,
        ],
      )
    },

    async load(entityName: string, entityId: string): Promise<Snapshot | null> {
      const row = await adapter.queryOne<{
        position: string
        payload: Buffer | Uint8Array
        metadata: Record<string, string>
        recorded_at: Date | string | number
      }>(
        `SELECT position::text AS position, payload, metadata, recorded_at
           FROM ${tables.snapshots}
          WHERE entity_name = $1 AND entity_id = $2`,
        [entityName, entityId],
      )
      if (!row) return null

      // Convert BYTEA result to Uint8Array for the deserializer
      let payloadBytes: Uint8Array
      if (row.payload instanceof Uint8Array) {
        payloadBytes = row.payload
      } else if (Buffer.isBuffer(row.payload)) {
        payloadBytes = new Uint8Array(row.payload.buffer, row.payload.byteOffset, row.payload.byteLength)
      } else {
        // Some drivers return hex-encoded string for BYTEA
        payloadBytes = new Uint8Array((row.payload as unknown as Buffer))
      }

      const payload = serializer.deserialize(payloadBytes)

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
        metadata: row.metadata ?? {},
      }
    },

    async deleteSnapshots(entityName: string, entityId: string): Promise<void> {
      await adapter.query(
        `DELETE FROM ${tables.snapshots} WHERE entity_name = $1 AND entity_id = $2`,
        [entityName, entityId],
      )
    },
  }
}
