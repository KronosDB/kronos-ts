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
import { TAG_DELIMITER } from "./criteria-sql.js"

/** One `kronos_events` row, as every read path asks for it. */
export type EventRow = {
  sequence_position: string
  event_id: string
  type: string
  tags: string[]
  payload: unknown
  metadata: unknown
  version: string
  timestamp: string | number
}

/** The columns every read path selects, in the one order both of them use. */
export const EVENT_COLUMNS =
  "sequence_position, event_id, type, tags, payload, metadata, version, timestamp"

export function decodeEvent(row: {
  type: string
  tags: string[]
  payload: unknown
  metadata: unknown
  sequence_position: string
  event_id: string
  version: string
  timestamp: string | number
}): EventMessage {
  const qn = qualifiedNameFromString(row.type)
  const tags = row.tags.map((t) => {
    const sep = t.indexOf(TAG_DELIMITER)
    return sep >= 0
      ? { key: t.slice(0, sep), value: t.slice(sep + 1) }
      : { key: t, value: "" }
  })
  return {
    kind: "event",
    identifier: row.event_id,
    name: qn,
    version: row.version,
    tags,
    payload: decodeJsonb(row.payload),
    metadata: decodeJsonb(row.metadata) as EventMessage["metadata"],
    timestamp: Number(row.timestamp),
  }
}

/**
 * Adapter-agnostic JSONB decoding: pgAdapter/postgresAdapter return parsed
 * objects, but bunSqlAdapter (Bun.SQL) returns JSONB as a raw string. Normalise
 * here so callers always see a JS object.
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
