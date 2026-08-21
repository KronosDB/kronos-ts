import type { EventQuery } from "./dcb-query.js"

/**
 * WHICH cached fold a sourcing read may start from — the SNAPSHOTTING STRATEGY,
 * said on the condition.
 *
 * It is a KEY, not a snapshot: the string the entry is filed under, and nothing
 * else — no version, and nothing derived from your code. Whoever serves the read
 * decides what to do with it, which is exactly what lets ONE address cover every
 * storage family: a log wrapped in `postgresSnapshottingEventStore` fuses the
 * lookup into its own query in one round trip, one wrapped in the in-memory,
 * KronosDB or Axon Server wrapper resolves it client-side in two, and a log that
 * was never wrapped ignores it and replays in full. All three are correct.
 *
 * WHETHER THE ENTRY IS USABLE IS NOT ASKED HERE. A store answers "here is what
 * is cached"; the REPOSITORY, which is the only party that knows the shape it
 * folds into, decides whether to start from it. See `structural-fitness.ts`.
 *
 * IT IS SET BY WHOEVER ASKED FOR IT — `ctx.source(query, { snapshot })` at the
 * raw layer, or a `ctx.load` whose state declared `snapshot: { key, when }`.
 * A bare `ctx.source(query)` sets nothing and reads the whole history, which is
 * what it always did.
 */
export type SnapshotKey = {
  /**
   * THE STRING YOU WROTE. One opaque key, filed as one column — not a name the
   * framework assigned and not a hash of anything. A raw `ctx.source` caller
   * passes it directly; `state()` composes `"<your key>:<flattened id>"`.
   */
  readonly key: string
}

/**
 * Defines which events to source from the event store.
 * Combines the query (what to match) with an optional start position, and —
 * for the state-load path only — the snapshot strategy.
 */
export type SourcingCondition = {
  readonly query: EventQuery
  readonly start?: bigint
  /**
   * Serve this read from the cached fold filed under this key, if one is there.
   * Absent means "read the log", which is what a bare `ctx.source(query)` says.
   */
  readonly snapshot?: SnapshotKey
}

/**
 * Create a sourcing condition from a query, an optional start position and an
 * optional snapshot strategy.
 *
 * Both trailing parameters are omitted from the result rather than set to
 * `undefined`, so a store can test for the property itself.
 */
export function sourcingCondition(
  query: EventQuery,
  start?: bigint,
  snapshot?: SnapshotKey,
): SourcingCondition {
  return {
    query,
    ...(start !== undefined ? { start } : {}),
    ...(snapshot !== undefined ? { snapshot } : {}),
  }
}

/**
 * The same condition with the strategy STRIPPED — a plain full-range read of
 * the same query, and what a snapshotting wrapper hands the store below it once
 * it has consumed the key itself.
 *
 * Spelled out rather than spread-with-undefined because `SourcingCondition` is
 * an exact-optional shape: `{ ...condition, snapshot: undefined }` would set the
 * key to `undefined` rather than remove it, and a store below that tests for the
 * property itself would see one it has to guess about.
 *
 * It lives here, beside the condition, because all four family wrappers need it
 * and none of them owns it.
 */
export function withoutSnapshotKey(condition: SourcingCondition): SourcingCondition {
  return condition.start !== undefined
    ? { query: condition.query, start: condition.start }
    : { query: condition.query }
}
