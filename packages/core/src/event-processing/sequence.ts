import type { EventMessage } from "../messaging/messages.js"
/**
 * Which sequence an event belongs to.
 *
 * TOTAL: every event has a sequence. Events sharing a sequence are processed in
 * order, and if one is parked in the dead-letter queue the rest of its sequence
 * parks behind it. "No ordering constraint" is not a missing answer, it is
 * `(e) => e.identifier` — every event in a sequence of its own.
 *
 * A plain function, not a policy object: there is one operation here, and a
 * record with one method is a function wearing a hat. There is no helper for
 * the usual one — a sequence per tag value is a line, and writing it shows where
 * an event without the tag goes:
 *
 * ```ts
 * const byAccount = (e: EventMessage) => e.tags.find((t) => t.key === "accountId")?.value ?? e.identifier
 * ```
 */
export type Sequence = (event: EventMessage) => string
