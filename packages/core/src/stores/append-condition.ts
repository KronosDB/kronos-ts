import type { EventQuery } from "../query/event-query.js"
import type { ConsistencyMarker } from "./consistency-marker.js"

/**
 * Defines the consistency boundary for appending events.
 *
 * By default, the append condition matches the sourcing condition —
 * guaranteeing that no conflicting events were appended since the state was loaded.
 *
 * Can be overridden per command handler for cases where less strict consistency
 * is valid (e.g. a bank debit that doesn't conflict with credits).
 */
export interface AppendCondition {
  readonly query: EventQuery
  readonly marker: ConsistencyMarker
}

/**
 * Create an append condition from a query and a consistency marker.
 */
export function appendCondition(query: EventQuery, marker: ConsistencyMarker): AppendCondition {
  return { query, marker }
}
