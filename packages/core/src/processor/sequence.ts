import { qualifiedNameToString } from "../primitives/qualified-name.js"
import type { EventMessage } from "../messages/message.js"

/**
 * Which ordered lane an event belongs to.
 *
 * TOTAL: every event has a lane. Events sharing a lane are processed in order,
 * and if one is parked in the dead-letter queue the rest of its lane parks
 * behind it. "No ordering constraint" is not a missing answer, it is the lane
 * `(e) => e.identifier` — every event in a lane of its own.
 *
 * A plain function, not a policy object: there is one operation here, and a
 * record with one method is a function wearing a hat.
 */
export type Sequence = (event: EventMessage) => string

/**
 * Lane an event by the value of a named tag, falling back to the event name
 * when the tag is absent. The DCB/tag-world analog of Axon's
 * `SequentialPerAggregatePolicy` (aggregate id → sequence).
 *
 * One line, written out rather than hidden, so the fallback is visible:
 *
 * ```ts
 * eventProcessor({ name: "balances", eventStore, tokenStore, unitOfWork,
 *                  sequence: sequentialPerTag("accountId"), deadLetterQueue })
 * ```
 */
export function sequentialPerTag(key: string): Sequence {
  return (event) => {
    const match = event.tags.find((t) => t.key === key)
    return match ? match.value : qualifiedNameToString(event.name)
  }
}
