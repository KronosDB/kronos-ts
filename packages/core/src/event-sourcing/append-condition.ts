import type { EventQuery } from "./dcb-query.js"
import type { ConsistencyMarker } from "./consistency-marker.js"

/**
 * Defines the consistency boundary for appending events.
 *
 * By default, the append condition matches the sourcing condition —
 * guaranteeing that no conflicting events were appended since the state was loaded.
 *
 * Can be overridden per command handler for cases where less strict consistency
 * is valid (e.g. a bank debit that doesn't conflict with credits).
 *
 * `query` and `marker` are the condition every store checks: refuse the append
 * if an event matching `query` sits after `marker`. When the condition covers
 * several reads, `query` is their union and `marker` the EARLIEST of their
 * markers, so no read's unseen events escape the check.
 *
 * `reads`, when present, is the same condition read by read. A store that can
 * check each read against its own marker refuses exactly when some read's
 * query has a match after THAT read's marker, which spares the conflicts the
 * earliest-marker form reports for events a later read already saw. A store
 * that cannot ignores `reads`. A wrapper that rewrites `query` or `marker`
 * must rewrite `reads` the same way, or drop it.
 */
export type AppendCondition = {
  readonly query: EventQuery
  readonly marker: ConsistencyMarker
  readonly reads?: ReadonlyArray<AppendConditionRead>
}

/** One read an append condition covers: its query, and the marker it returned. */
export type AppendConditionRead = {
  readonly query: EventQuery
  readonly marker: ConsistencyMarker
}

/**
 * Create an append condition from a query and a consistency marker.
 */
export function appendCondition(query: EventQuery, marker: ConsistencyMarker): AppendCondition {
  return { query, marker }
}
