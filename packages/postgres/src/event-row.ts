/**
 * How a `kronos_events` row becomes an `EventMessage`.
 *
 * PACKAGE-PRIVATE, and shared by the two sides that read the table:
 * `postgresEventStore` for the plain and streaming reads, and
 * `postgresSnapshottingEventStore` for the FUSED read it writes itself. They
 * are separate functions a host composes separately, so the decoding lives
 * where neither of them owns it — otherwise the fused read would be a second,
 * quietly diverging copy of the plain one.
 */

import type { EventMessage } from "@kronos-ts/core"
import { qualifiedNameFromString } from "@kronos-ts/core"

/** One `kronos_events` row, as every read path asks for it. */
export type EventRow = {
  sequence_position: string
  event_id: string
  type: string
  payload: unknown
  metadata: unknown
  version: string
  timestamp: string | number
}

/**
 * The columns every read path selects, in the one order both of them use. The
 * `tags` column is the index reads are matched on, and is not read back.
 */
export const EVENT_COLUMNS =
  "sequence_position, event_id, type, payload, metadata, version, timestamp"

export function decodeEvent(row: {
  type: string
  payload: unknown
  metadata: unknown
  sequence_position: string
  event_id: string
  version: string
  timestamp: string | number
}): EventMessage {
  const qn = qualifiedNameFromString(row.type)
  return {
    kind: "event",
    identifier: row.event_id,
    name: qn,
    version: row.version,
    payload: decodeJsonb(row.payload),
    metadata: decodeJsonb(row.metadata) as EventMessage["metadata"],
    timestamp: Number(row.timestamp),
  }
}

/**
 * What a JSONB column held, as the value that was stored.
 *
 * All three drivers return JSONB parsed. A string here is a row written
 * through postgresAdapter or bunSqlAdapter before payload and metadata were
 * bound as TEXT: those rows hold a JSON string containing the JSON text, and
 * they are never rewritten, so the string is parsed here.
 */
export function decodeJsonb(v: unknown): unknown {
  if (typeof v === "string") {
    try {
      return JSON.parse(v)
    } catch {
      return v
    }
  }
  return v
}
