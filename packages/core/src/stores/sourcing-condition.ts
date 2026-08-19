import type { EventQuery } from "../query/event-query.js"

/**
 * Defines which events to source from the event store.
 * Combines the query (what to match) with an optional start position.
 */
export interface SourcingCondition {
  readonly query: EventQuery
  readonly start?: bigint
}

/**
 * Create a sourcing condition from a query and an optional start position.
 */
export function sourcingCondition(query: EventQuery, start?: bigint): SourcingCondition {
  return start !== undefined ? { query, start } : { query }
}
