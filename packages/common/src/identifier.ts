import { v7 as uuidv7 } from "uuid"

/**
 * Generates a unique identifier for messages.
 *
 * Returns a UUID v7 (RFC 9562) — a time-ordered UUID whose first 48 bits
 * encode a unix-millisecond timestamp. v7 ids are shape-compatible with v4
 * (same 8-4-4-4-12 hex-with-dashes string), so consumers that treat the id
 * as an opaque string keep working. The time-ordered prefix keeps UNIQUE
 * btree indexes (e.g. event-store identifier columns) compact under insert
 * load, since new ids land at the right edge of the index instead of
 * fragmenting it like random v4 ids do.
 */
export function generateIdentifier(): string {
  return uuidv7()
}
