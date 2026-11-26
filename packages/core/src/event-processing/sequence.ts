import type { EventMessage } from "../messaging/messages.js"
/**
 * Which sequence an event belongs to.
 *
 * TOTAL: every event has a sequence. Events sharing a sequence are processed in
 * order, and if one is parked in the dead-letter queue the rest of its sequence
 * parks behind it. "No ordering constraint" is not a missing answer, it is
 * `(e) => e.identifier` — every event in a sequence of its own.
 *
 * A plain function of the event. The usual one reads a field of the payload,
 * and says where an event without that field goes:
 *
 * ```ts
 * const byAccount = (e: EventMessage) => (e.payload as { accountId?: string }).accountId ?? e.identifier
 * ```
 */
export type Sequence = (event: EventMessage) => string
