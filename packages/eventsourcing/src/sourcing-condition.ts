import type { EventCriteria } from "@kronos-ts/messaging"

/**
 * Defines which events to source from the event store.
 * Combines criteria (what to match) with an optional start position.
 */
export interface SourcingCondition {
  readonly criteria: EventCriteria
  readonly start?: bigint
}

/**
 * Create a sourcing condition from criteria and an optional start position.
 */
export function sourcingCondition(criteria: EventCriteria, start?: bigint): SourcingCondition {
  return start !== undefined ? { criteria, start } : { criteria }
}
