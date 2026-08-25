import { requireLive, type SourcingInfo, type UnitOfWork } from "../unit-of-work/unit-of-work.js"
import { repositoryFor } from "./repository.js"
import type { EventMessage } from "../messaging/messages.js"
import type { EventQuery } from "./dcb-query.js"
import { sourcingCondition } from "./sourcing-condition.js"
import type { EventStore, SnapshotCapableEventStore } from "./event-store.js"
import type { Snapshot } from "./snapshot.js"
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
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// THE DEMAND — one alias, and every read surface that needs the snapshotting
// capability derives from it.
// ---------------------------------------------------------------------------

/**
 * THE DEMAND. Branch on whether `E` — the log an entry actually wired — carries
 * the snapshotting capability.
 *
 * THIS IS THE ONE PLACE THE QUESTION IS ASKED. Both public faces of the demand
 * are written in terms of it and neither repeats the `extends` for itself:
 *
 * - {@link SnapshotReads} — what a CAPABLE store ADDS to a context: the fused
 *   `ctx.source(query, { snapshot })` overload. Against a bare store that
 *   overload is structurally ABSENT — not present-and-erroring — so a
 *   two-argument call is a no-such-signature error rather than a type mismatch
 *   inside a signature nobody should have been offered.
 * - {@link SnapshotDemand} — what a BARE store ADDS to `ctx.load`'s state
 *   parameter: a refusal branded with the fix, so a state that declares a
 *   snapshot policy cannot be loaded through a log that cannot serve one.
 *
 * The two faces are exact mirrors, which is not a coincidence: the capability
 * either widens what you may ask for, or narrows what you may ask it OF.
 *
 * ANCHOR ANYTHING LATER HERE. A native KronosDB fused read, a family that grows
 * a second capability, a store tier nobody has thought of yet — every one of
 * them is a question about what a wired store can serve, and asking it in one
 * place is what keeps the answer consistent across three contexts, two read
 * verbs and four storage families. Add a face; do not add a predicate.
 *
 * NOTHING RUNS. This is erased entirely: the JavaScript a demanded `ctx.load`
 * emits is byte for byte the JavaScript an undemanded one emits, and the only
 * runtime trace of the whole mechanism is one defensive `throw` in
 * `repository.ts` for callers who have no compiler at all.
 */
export type IfSnapshotCapable<E extends EventStore, Capable, Bare> =
  E extends SnapshotCapableEventStore ? Capable : Bare

/**
 * `ctx.load`'s face of {@link IfSnapshotCapable}: nothing extra when the log is
 * capable, a BRANDED REFUSAL on `snapshot` when it is not.
 *
 * THE REFUSAL IS SPELLED INLINE, AND THAT IS DELIBERATE. Give the object type a
 * name and TypeScript prints the NAME in the diagnostic — `WrapTheEventStore`,
 * which tells a reader nothing. Left anonymous, the compiler has no shorthand
 * to reach for and prints the structure, so the property keys ARE the error
 * message and they arrive at the call site without anybody following a link.
 *
 * A state that declares `snapshot: { key, when }` types that field as a
 * `SnapshotConfig`; this demands two properties a config does not have, so the
 * compiler reports exactly which state, and what to do. A state WITHOUT a
 * policy types the field `undefined`, satisfies the demand vacuously, and never
 * sees a word of any of this.
 *
 * THE FIX IS SPELLED GENERALLY, AND THAT IS A RULE RATHER THAN A HEDGE. A
 * diagnostic may only be as specific as the code that owns it is CERTAIN about,
 * and core is certain about the capability and the shape of the wrapper — not
 * about which family this host chose. Naming a postgres function here would be
 * a confident wrong answer for the five hosts that are not on postgres. The
 * packages that DO know say so in their own diagnostics; see the family brands
 * in the persistence packages, which name their own factory because they are
 * entitled to.
 */
export type SnapshotDemand<E extends EventStore> = IfSnapshotCapable<
  E,
  unknown,
  {
    readonly snapshot?: {
      readonly ERROR: "this state declares a snapshot policy, but this handler's eventStore cannot serve one"
      readonly FIX: "wrap this entry's eventStore in the snapshotting wrapper for its persistence family — <family>SnapshottingEventStore(store, …)"
    }
  }
>

/**
 * `ctx.source`'s face of {@link IfSnapshotCapable}: the FUSED overload when the
 * log is capable, nothing at all when it is not.
 *
 * A context is ASSEMBLED by intersection — `EventHandlerContext<E, Q, U>` is the
 * base shape (whose `source` has only the plain one-argument signature)
 * intersected with this. Against a capable store the property becomes an
 * overload set, plain signature first; against a bare one it stays exactly the
 * single-signature function it has always been, and `unknown` disappears from
 * the intersection without a trace.
 */
export type SnapshotReads<E extends EventStore> = IfSnapshotCapable<
  E,
  { readonly source: FusedSourceFunction },
  unknown
>

/**
 * Load event-sourced state for a state value within the unit of work.
 *
 * NOTHING IS REGISTERED. The state is named at the CALL SITE and the log comes
 * off the ENTRY's site, so the pair `ctx.load` needs is complete the moment it
 * is called. A state is data; data needs no invitation.
 *
 * `E` IS THE ENTRY'S LOG, threaded here from the composition root so the state
 * and the log can be checked against each other. The third argument to `State`
 * is `any` deliberately: this verb takes a state of EITHER snapshot-ness, and
 * which ones it will actually accept is said once, beside it, by
 * {@link SnapshotDemand} — not by narrowing the parameter twice.
 */
export type LoadFunction<E extends EventStore = EventStore> = <Id, S>(
  state: State<Id, S, any> & SnapshotDemand<E>,
  id: Id,
) => Promise<S>

/**
 * Run a query against the log this handling reads from and get the matching
 * events back, in stream order — the RAW layer under `ctx.load`.
 *
 * The query is the same plain data a `state()` derives for itself: `types` is
 * an any-of, `tags` an all-of, and an array of items is an OR. What is NOT
 * different is the consistency story — see {@link sourceFunction}.
 *
 * ONE SIGNATURE. The fused two-argument form is not here: it is contributed by
 * {@link SnapshotReads}, and only to a context whose log can serve it.
 */
export type SourceFunction = (query: EventQuery) => Promise<ReadonlyArray<EventMessage>>

/**
 * THE FUSED READ, contributed to a context only by a capable log: the cached
 * fold filed under `snapshot`, plus only the events after it. See
 * {@link SnapshottedSource}.
 */
export type FusedSourceFunction = (
  query: EventQuery,
  opts: { snapshot: string },
) => Promise<SnapshottedSource>

/**
 * What a FUSED raw read hands back — the three things a hand-rolled fold needs
 * and nothing else.
 *
 * This is the whole snapshotting mechanism at the raw layer. You brought a key,
 * you get back whatever is filed under it and the events that came after it;
 * what you do with the pair is your fold's business, including whether to
 * trust the cached value at all.
 *
 * ```ts
 * const { snapshot, events, position } = await ctx.source(
 *   { tags: { courseId }, types: [CourseCreated, StudentSubscribed] },
 *   { snapshot: `course:${courseId}` },
 * )
 * const state = events.reduce(fold, snapshot ? (snapshot.state as S) : initial)
 * if (events.length > 100) await eventStore.storeSnapshot(`course:${courseId}`, { state, position })
 * ```
 *
 * BOTH HALVES COME OFF THE SAME OBJECT, and that is the whole ergonomic gain of
 * the capability tier over the old separate seam: the read verb you already had
 * grew an overload, and the write is a member of the log the slice already
 * holds. There is no second resource to wire and no second field to forget.
 */
export type SnapshottedSource = {
  /**
   * The cached fold, or nothing when the key missed. FITNESS IS YOURS: nothing
   * in the framework judges whether this value is still usable at the raw
   * layer, because nothing in the framework knows what your fold folds into.
   * Check it, or change your key when its meaning changes, or both.
   */
  readonly snapshot: Snapshot | undefined
  /**
   * The matching events AFTER `snapshot.position`, in stream order — the whole
   * matching history when there was no snapshot. Fold these on top.
   */
  readonly events: ReadonlyArray<EventMessage>
  /**
   * The consistency marker's position for this read — what a snapshot you write
   * from this fold should record as its own `position`, and the same value the
   * append condition was stamped with.
   */
  readonly position: bigint
}

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
export function loadFunction<E extends EventStore = EventStore>(deps: {
  uow: UnitOfWork
  eventStore?: E
}): LoadFunction<E> {
  return (async <Id, S>(state: State<Id, S, any>, id: Id): Promise<S> => {
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
  }) as LoadFunction<E>
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
}): SourceFunction & FusedSourceFunction {
  return (async (
    query: EventQuery,
    opts?: { snapshot?: string },
  ): Promise<ReadonlyArray<EventMessage> | SnapshottedSource> => {
    const uow = requireLive(deps.uow)
    const eventStore = requireLog(deps.eventStore, "ctx.source(…)")

    const key = opts?.snapshot

    // THE PLAIN READ, byte for byte what it always was. No key, no fusion, and
    // the return is the array — a caller who never heard of snapshotting is
    // unaffected by any of this.
    if (key === undefined) {
      const { events, marker } = await eventStore.source(sourcingCondition(query))
      recordSourcing(uow, { query, markerPosition: marker.position })
      return events
    }

    // THE FUSED READ. The key goes onto the CONDITION, which is the one place
    // the whole mechanism is addressed from — and by the time control gets
    // here the TYPES have already established that this log can serve it: the
    // overload that accepts a second argument only exists on a context whose
    // `E` is capable. A store that somehow still ignores the key hands back the
    // full history, which is correct, just not accelerated.
    const { events, marker, snapshot } = await eventStore.source(
      sourcingCondition(query, undefined, { key }),
    )

    // THE BOOKKEEPING IS IDENTICAL. Fusing the read narrows which events come
    // back; it does not narrow what was READ, so the append condition is
    // stamped with exactly the query and marker a plain read would have
    // stamped. A fold seeded from a snapshot has the same DCB guarantee a fold
    // that replayed everything has.
    recordSourcing(uow, { query, markerPosition: marker.position })

    return { snapshot, events, position: marker.position }
  }) as SourceFunction & FusedSourceFunction
}
