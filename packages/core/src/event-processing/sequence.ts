import type { EventMessage } from "../messaging/messages.js"
/**
 * Which ordered lane an event belongs to.
 *
 * TOTAL: every event has a lane. Events sharing a lane are processed in order,
 * and if one is parked in the dead-letter queue the rest of its lane parks
 * behind it. "No ordering constraint" is not a missing answer, it is the lane
 * `(e) => e.identifier` — every event in a lane of its own.
 *
 * A plain function, not a policy object: there is one operation here, and a
 * record with one method is a function wearing a hat. There is no helper for
 * the usual one — a bucket per tag value is a line, and writing it shows where
 * an event without the tag goes:
 *
 * ```ts
 * const byAccount = (e: EventMessage) => e.tags.find((t) => t.key === "accountId")?.value ?? e.identifier
 * ```
 */
export type Sequence = (event: EventMessage) => string
