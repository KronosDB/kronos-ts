import { qualifiedNameToString } from "@kronos-ts/common"
import type { EventMessage } from "./message.js"

/**
 * Decides which ordered sequence an event belongs to.
 *
 * Events sharing a sequence identifier are processed in order; if one is
 * dead-lettered, subsequent events in the same sequence are parked behind it
 * (see {@link deadLetteringDelivery}). This is the minimal analog of
 * Axon's `SequencingPolicy` — a plain function rather than a class hierarchy,
 * because Kronos does not (yet) do segmented parallel processing where the
 * policy would also drive segment assignment. When/if it does, this same type
 * is the seam to extend.
 */
export type SequencingPolicy = (event: EventMessage) => string

/**
 * Sequences events by the value of a named tag, falling back to the event name
 * when the tag is absent. The DCB/tag-world analog of Axon's
 * `SequentialPerAggregatePolicy` (aggregate id → sequence).
 *
 * Prefer this — naming the tag explicitly — over relying on tag *order*.
 *
 * ```typescript
 * trackingProcessor("balances")
 *   .deadLetterQueue(dlq)
 *   .sequencingPolicy(sequentialPerTag("accountId"))
 * ```
 */
export function sequentialPerTag(tagKey: string): SequencingPolicy {
  return (event) => {
    const match = event.tags.find((t) => t.key === tagKey)
    return match ? match.value : qualifiedNameToString(event.name)
  }
}

/**
 * Default policy: sequence by the first tag value, else the event name.
 *
 * Order-dependent on the tag list, so it is a reasonable default but a poor
 * deliberate choice — prefer {@link sequentialPerTag} with an explicit key.
 */
export const defaultSequencingPolicy: SequencingPolicy = (event) => {
  if (event.tags.length > 0) {
    return event.tags[0]!.value
  }
  return qualifiedNameToString(event.name)
}

/**
 * Full concurrency: every event is its own singleton sequence (keyed by the
 * unique message identifier), so no two events ever block each other. Use when
 * handlers carry no cross-event ordering requirement.
 */
export const fullConcurrencyPolicy: SequencingPolicy = (event) => event.identifier
