import { requireLive, type SourcingInfo, type UnitOfWork } from "../unit-of-work/unit-of-work.js"
import { repositoryFor } from "./repository.js"
import type { EventMessage } from "../messaging/messages.js"
import type { EventQuery } from "./dcb-query.js"
import { sourcingCondition } from "./sourcing-condition.js"
import type { EventStore } from "./event-store.js"
import type { State } from "./state.js"

// ---------------------------------------------------------------------------
// THE TWO READS A HANDLING CAN MAKE, and the one piece of bookkeeping they
// share.
//
// `ctx.load(Course, id)` is the FOLDED read: the state says which events it
// folds and how, and what comes back is the state. `ctx.source(query)` is the
// RAW one: you write the query, you run the fold, and what comes back is the
// events in stream order.
//
// They differ in everything except the thing that matters most. BOTH record
// what they read onto the task — the query, and the position they read up to —
// and the PREPARE_COMMIT flush turns those entries into the append condition
// (see `unit-of-work/event-flush.ts`). So a hand-rolled fold gets exactly the
// DCB optimistic-concurrency guarantee a `state()` fold gets: an append in the
// same task fails if anything matching what you read landed in between. That
// is `recordSourcing` below, and it is deliberately the ONLY way either read
// contributes to a write.
//
// SNAPSHOTTING IS NOT VISIBLE HERE, and that is deliberate. It is a tier on
// the STORE: wrap the entry's log in `<family>SnapshottingEventStore`, declare
// `state({ snapshot })`, and the repository behind `ctx.load` asks the log for
// the fused read. A handler's context has no member for it — there is nothing
// new a handler would call — so nothing on a context changes shape when a
// host adds the tier. A state with a snapshot policy loaded through a log that
// cannot serve one is refused at runtime, loudly, by `capableOrThrow` in
// `repository.ts`, on the first load in any test that runs the handler.
// ---------------------------------------------------------------------------

/**
 * Load event-sourced state for a state value within the unit of work.
 *
 * NOTHING IS REGISTERED. The state is named at the CALL SITE and the log comes
 * off the ENTRY's site, so the pair `ctx.load` needs is complete the moment it
 * is called. A state is data; data needs no invitation.
 *
 * The third argument to `State` is `any` deliberately: this takes a state of
 * EITHER snapshot-ness. Whether the entry's log can serve the policy a state
 * declares is a fact about the wired store, checked where the store is used.
 */
export type LoadFunction = <Id, S>(state: State<Id, S, any>, id: Id) => Promise<S>

/**
 * Run a query against the log this handling reads from and get the matching
 * events back, in stream order — the RAW layer under `ctx.load`.
 *
 * The query is the same plain data a `state()` derives for itself: `types` is
 * an any-of, `tags` an all-of, and an array of items is an OR. What is NOT
 * different is the consistency story — see {@link sourceFunction}.
 */
export type SourceFunction = (query: EventQuery) => Promise<ReadonlyArray<EventMessage>>

/**
 * The log a read comes off, or an error naming exactly what was missing and
 * where to put it. Shared by both reads, because "no `eventStore` on this
 * entry" is one mistake with one fix, however it was noticed.
 */
function requireLog(eventStore: EventStore | undefined, call: string): EventStore {
  if (!eventStore) {
    throw new Error(
      `${call} needs a log to source from, but no \`eventStore\` was attached to this entry. ` +
      "Attach one at the composition root, e.g. `{ ...handler, eventStore }`.",
    )
  }
  return eventStore
}

/**
 * THE BOOKKEEPING — one entry per read, and the only route from a read to an
 * append condition.
 *
 * `ctx.load` and `ctx.source` both come through here, which is what makes their
 * consistency guarantee literally the same guarantee rather than two
 * implementations of one idea. The PREPARE_COMMIT flush concatenates every
 * recorded query's items (they OR) and takes the highest marker; the store then
 * refuses the write if anything matching landed after it.
 */
function recordSourcing(uow: UnitOfWork, info: SourcingInfo): void {
  uow.events.sourcingInfos.push(info)
}

/**
 * Stable cache key for a state id. State ids are typically objects
 * (e.g. `{ ticketId }`), and `String({...})` collapses every object to
 * `"[object Object]"` — which would make two different ids of the same state
 * share a cache entry within a UoW and return each other's state. Serialize
 * structurally instead, with sorted keys (so id construction order is
 * irrelevant) and bigint support. Primitive ids serialize to a unique string
 * too, so this is strictly safer than `String(id)`.
 */
function stableIdKey(id: unknown): string {
  return JSON.stringify(id, (_key, value) => {
    if (typeof value === "bigint") return `${value}n`
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const sorted: Record<string, unknown> = {}
      for (const k of Object.keys(value).sort()) sorted[k] = (value as Record<string, unknown>)[k]
      return sorted
    }
    return value
  })
}

/**
 * Build the `load` capability for ONE invocation, closed over that
 * invocation's unit of work and the SITE of the entry being invoked — its log,
 * which is ALSO its snapshot cache when the host wrapped one.
 *
 * Internal — handlers reach the result as `ctx.load`.
 *
 * Read-only — NOT phase-guarded, but throws NoActiveUnitOfWork once the unit
 * of work has closed. Caches state on the unit of work (duplicate `load()`
 * calls for the same state-id pair return the cached promise without
 * re-querying the store) and records a sourcing info per call, which the
 * PREPARE_COMMIT flush turns into the DCB append condition.
 */
export function loadFunction(deps: {
  uow: UnitOfWork
  eventStore?: EventStore
}): LoadFunction {
  return async <Id, S>(state: State<Id, S, any>, id: Id): Promise<S> => {
    const uow = requireLive(deps.uow)
    const eventStore = requireLog(deps.eventStore, `ctx.load(${state.identity}, …)`)

    // The state half of the key is the definition's `identity` — the handle
    // `state()` assigns per definition — not its `name`, which is optional and
    // means durable snapshot identity, not "which definition is this".
    const { entries, modules } = uow.stateCache
    const cacheKey = `${state.identity}:${stableIdKey(id)}`
    if (!entries.has(cacheKey)) {
      entries.set(cacheKey, repositoryFor(state, eventStore).load(id))
      modules.set(cacheKey, { module: state, id })
    }
    const result = (await entries.get(cacheKey)!) as {
      state: S
      sourcingInfo: SourcingInfo
    }
    recordSourcing(uow, result.sourcingInfo)
    return result.state
  }
}

/**
 * Build the `source` capability for ONE invocation — THE RAW LAYER.
 *
 * Internal — handlers reach the result as `ctx.source`.
 *
 * `state()` writes a query for you: it derives one per folded event type from
 * the tag record and the fold, and the SAME query becomes the append condition.
 * This is the layer under that. You write the query, you run the fold — and the
 * append condition still holds, because the read is recorded on the task
 * exactly the way `ctx.load`'s is (see `recordSourcing`). A hand-rolled
 * `is()` + `reduce` therefore has the identical DCB optimistic-concurrency
 * guarantee a decision state has:
 *
 * ```ts
 * const events = await ctx.source({ tags: { courseId }, types: [CourseCreated, StudentSubscribed] })
 * const subscribed = events.filter((e) => is(e, StudentSubscribed)).length
 * if (subscribed >= capacity) throw new Error("full")
 * ctx.append(StudentSubscribed, { courseId, studentId })   // conditioned on that very read
 * ```
 *
 * THE CONFLICT WINDOW IS WHAT YOU DECLARED. Naming `types` narrows it to those
 * event types; omitting `types` is legal and means "every event carrying these
 * tags", which is a WIDER window and more spurious conflicts. That narrowing is
 * one of the things `state()`'s derivation was doing on your behalf.
 *
 * It reads the ENTRY's store — the same object `ctx.load` reads — so a store
 * composed with `upcastingEventStore` hands this layer upcasted events too.
 * The store seam answers with the whole matching result and its marker, so
 * there is nothing left to drain: what comes back is an array, in stream order,
 * ready to be the input of a fold.
 *
 * Read-only, so it is NOT phase-guarded; it throws NoActiveUnitOfWork once the
 * unit of work has closed. It is deliberately NOT cached on the unit of work
 * the way `ctx.load` is: a state-and-id pair names the same answer twice, an
 * ad-hoc query does not.
 */
export function sourceFunction(deps: {
  uow: UnitOfWork
  eventStore?: EventStore
}): SourceFunction {
  return async (query: EventQuery): Promise<ReadonlyArray<EventMessage>> => {
    const uow = requireLive(deps.uow)
    const eventStore = requireLog(deps.eventStore, "ctx.source(…)")
    const { events, marker } = await eventStore.source(sourcingCondition(query))
    recordSourcing(uow, { query, markerPosition: marker.position })
    return events
  }
}
