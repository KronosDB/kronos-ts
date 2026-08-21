import { qualifiedNameToString, type EventMessage } from "../messaging/messages.js"
import type { SourcingInfo } from "../unit-of-work/unit-of-work.js"
import type { State } from "./state.js"
import type { EventStore, SnapshotCapableEventStore } from "./event-store.js"
import { sourcingCondition, type SnapshotKey } from "./sourcing-condition.js"
import {
  type EvolutionResult,
  snapshotIdentifier,
} from "./snapshot.js"
import { matchesInitialStructure } from "./structural-fitness.js"

// ---------------------------------------------------------------------------
// The FOLD, at a site.
//
// A repository is what a state VALUE plus a LOG add up to: the state says which
// events it folds and how, the log says where they are. Neither is registered
// anywhere — `ctx.load(Course, id)` names the state at the call site and the
// entry's `eventStore` names the log, so the pair is complete without anybody
// having declared it in advance.
//
// Which is why the only thing kept between calls is a CACHE, not a registry:
// `repositoryFor` remembers what it has already built, keyed on the objects it
// was built from, so a thousand loads over one log build one repository per
// state. Forget the cache and nothing breaks; forgetting a registry used to
// break everything. That difference is the whole point.
// ---------------------------------------------------------------------------

/**
 * Result of loading state — the state plus sourcing metadata.
 *
 * The sourcing info is what a `ctx.append` in the same task turns into its DCB
 * append condition: what was read is what the write is checked against.
 */
export type LoadResult<S = unknown> = {
  readonly state: S
  readonly sourcingInfo: SourcingInfo
}

/**
 * A repository that knows how to load state of a specific state
 * by sourcing events from the event store and folding them through evolvers.
 */
export type StateRepository<Id = unknown, S = unknown> = {
  load(id: Id): Promise<LoadResult<S>>
  /**
   * Load state, creating the initial state if no events exist.
   * Unlike `load()`, this never fails for a new state — it returns
   * the initial state with empty sourcing info.
   */
  loadOrCreate(id: Id): Promise<LoadResult<S>>
}

/**
 * Creates a repository for a state sourced from events.
 *
 * When `load(id)` is called, the repository:
 * 1. Resolves the sourcing query from the state + id
 * 2. Sources it — ASKING for the snapshot strategy when this state and this
 *    site both say snapshots are on (see below)
 * 3. Starts from the leading snapshot's state if one came back AND still fits
 *    the shape `initial(id)` describes, from `initial(id)` if not
 * 4. Folds the sourced events on top
 * 5. Writes a new cache entry if the state's policy says this load earned one
 * 6. Returns the state AND sourcing info (query + marker)
 *
 * SNAPSHOTS NEED BOTH HALVES. The POLICY rides on the state value
 * (`state({ snapshot })`) because how often a state is worth caching is a
 * property of its event volume; the CAPABILITY is a site fact, carried by the
 * entry's log — a store wrapped in its family's `…SnapshottingEventStore`. With
 * either half missing this repository never touches a snapshot.
 *
 * AND THE COMPILER ALREADY CHECKED. A state with a policy cannot reach a
 * `ctx.load` whose log is bare — see `IfSnapshotCapable` in `load.ts` — so by
 * the time control arrives here the pair is guaranteed. The check below is a
 * DEFENSIVE assertion for callers who compiled nothing at all, not a branch the
 * design depends on.
 *
 * ALL OF THIS IS SUGAR. `ctx.source(query, { snapshot })` plus an
 * `eventStore.storeSnapshot(key, …)` call is the same mechanism with the policy
 * and the key composition written by hand, and it is four lines. What `state()`
 * adds is that the fold, the query, the policy and the key sit in one value.
 *
 * ── THE READ IS NOT THIS FUNCTION'S JOB ──────────────────────────────────────
 *
 * It used to be. The repository used to load the snapshot itself, compute a
 * start position and hand that to `source`. It does not any more: it says WHAT
 * IT WANTS on the condition — `{ snapshot: { key } }` — and the log serves it.
 * A wrapped log fuses the lookup with the query (`postgresSnapshottingEventStore`
 * in one round trip, the others in two); an unwrapped one ignores the key and
 * replays in full, which is still correct.
 *
 * ── THE WRITE IS ───────────────────────────────────────────────────────────
 *
 * The fold writes its own cache entry. That is not "core knowing about an
 * optimization" — the split is exact and worth saying out loud. The MECHANISM
 * owns the read fusion, because fusing a lookup with a query is a property of
 * the store you are reading from, and only the store can do it in one round
 * trip. The FOLD owns the write, because the thing being cached is the fold's
 * own output at the fold's own position, and there is nobody else in the system
 * who is holding both. A decorator on the read path cannot write this entry: it
 * sees events go past, not the state they added up to.
 *
 * The write is fire-and-forget and its failure is swallowed. A cache write that
 * takes a load down with it would make the cache load-bearing, which is the one
 * thing this mechanism is not allowed to be.
 */
export function eventSourcedRepository<Id, S>(
  module: State<Id, S, any>,
  eventStore: EventStore,
): StateRepository<Id, S> {
  // THE CACHE KEY IS THE ONE THE STATE DECLARED — a string somebody wrote, not
  // a name the framework assigned. Per id, entries are filed under
  // `"<key>:<flattened id>"`, so one declared key serves every instance of the
  // state without them colliding.
  const snapshotConfig: SnapshotConfigOf = module.snapshot
  const snapshotPolicy = snapshotConfig?.when

  // BOTH HALVES, settled once: a policy on the state, and the capability on the
  // log. THE TYPES ALREADY GUARANTEE THE PAIR — this is the one line of runtime
  // left in the whole demand, and it exists for a caller who reached this
  // function from JavaScript, where there was no compiler to hold to it.
  const snapshots = snapshotPolicy !== undefined ? capableOrThrow(module, eventStore) : undefined

  /**
   * The strategy this state asks a read for, or nothing. The identifier is
   * flattened HERE, by the side that knows the id's shape, so every store is
   * handed the one string rather than six of them inventing an encoding for an
   * `unknown`.
   */
  function keyFor(id: Id): string {
    return `${snapshotConfig!.key}:${snapshotIdentifier(id)}`
  }

  function strategyFor(id: Id): SnapshotKey | undefined {
    if (!snapshots) return undefined
    return { key: keyFor(id) }
  }

  async function doLoad(id: Id): Promise<LoadResult<S>> {
    const startTime = performance.now()
    const query = module.query(id)

    // ASK for the strategy; whoever serves the read decides whether it can.
    let { events, marker, snapshot } = await eventStore.source(
      sourcingCondition(query, undefined, strategyFor(id)),
    )

    // THE ZEROTH STATE, told which thing it is the zeroth state OF. An initial
    // written with no parameter simply ignores the argument. It is computed
    // even when a snapshot came back, because it is also the SPECIMEN the
    // fitness check is made against — the current code's own example of the
    // shape this fold works in, which is why nothing has to describe that shape
    // separately and nothing can drift from it.
    const zeroth = module.initial(id)

    // STRUCTURAL FITNESS, judged HERE because this is the only place that holds
    // both the cached value and the shape it would be folded into. Unfit means
    // DISCARD AND REPLAY — never migrate — so the read is simply run again
    // without the strategy, and the policy below writes a fresh entry of the
    // current shape on the way out. Silent: an unfit entry is a cache miss that
    // took one extra round trip to notice, not a fault anybody should hear about.
    if (snapshot !== undefined && !matchesInitialStructure(zeroth, snapshot.state)) {
      const replay = await eventStore.source(sourcingCondition(query))
      events = replay.events
      marker = replay.marker
      snapshot = undefined
    }

    // A leading snapshot REPLACES the zeroth state — that is the whole
    // optimization, and it is one line because the condition did the work.
    let state = snapshot !== undefined ? (snapshot.state as S) : zeroth

    const lifecycle = module.lifecycle
    let isFirstEvent = snapshot === undefined // first event only if no snapshot
    let wasDeleted = lifecycle?.isDeleted?.(state) ?? false

    let eventsApplied = 0
    for (const event of events) {
      const previousState = state
      state = await applyEvent(module, state, event)
      eventsApplied++

      // Lifecycle hooks
      if (lifecycle && state !== previousState) {
        // onCreate: first event transitions from the initial state
        if (isFirstEvent && eventsApplied === 1) {
          await lifecycle.onCreate?.(state, id)
        }

        // onStateChange: after each evolving event
        await lifecycle.onStateChange?.(previousState, state, event, id)

        // onDelete: when isDeleted transitions from false to true
        if (lifecycle.isDeleted) {
          const nowDeleted = lifecycle.isDeleted(state)
          if (nowDeleted && !wasDeleted) {
            await lifecycle.onDelete?.(state, id)
          }
          wasDeleted = nowDeleted
        }
      }
    }

    const sourcingTimeMs = performance.now() - startTime

    // THE FOLD WRITING ITS OWN CACHE. Fire-and-forget, and its failure is
    // swallowed on purpose — a cache write that could fail a load would make
    // the cache load-bearing.
    //
    // `eventsApplied > 0` is not a policy, it is arithmetic: a load that folded
    // nothing computed nothing new, so there is nothing to cache that is not
    // already cached.
    if (snapshots && snapshotPolicy && eventsApplied > 0) {
      const result: EvolutionResult = { eventsApplied, sourcingTimeMs }
      if (snapshotPolicy.shouldSnapshot(result)) {
        const key = keyFor(id)
        snapshots
          .storeSnapshot(key, { state, position: marker.position })
          .catch((err) => {
            console.warn(`Failed to store snapshot for ${key}:`, err)
          })
      }
    }

    return {
      state,
      sourcingInfo: {
        query,
        markerPosition: marker.position,
      },
    }
  }

  return {
    load: doLoad,
    loadOrCreate: doLoad, // Same implementation — the initial state always provides a starting point
  }
}

/**
 * What `module.snapshot` is once the state's snapshot-ness has been widened
 * away. `State<Id, S, any>` types the field as the config or nothing, and this
 * is that, named once so the destructure above reads.
 */
type SnapshotConfigOf = { readonly key: string; readonly when: EvolutionPolicy } | undefined
type EvolutionPolicy = { shouldSnapshot(result: EvolutionResult): boolean }

/**
 * THE DEFENSIVE ASSERTION — the entire runtime footprint of the compile-time
 * demand, and it exists for JavaScript callers only.
 *
 * A TypeScript host cannot get here: `ctx.load` refuses a state carrying a
 * snapshot policy unless the entry's log is a {@link SnapshotCapableEventStore},
 * so the pair was settled at the call site. Somebody calling this from plain JS
 * had no such conversation, and a silent full replay that also writes to
 * nothing would be a performance mystery rather than a mistake — so it says so.
 */
function capableOrThrow(
  module: { readonly identity: string },
  eventStore: EventStore,
): SnapshotCapableEventStore {
  if (typeof (eventStore as Partial<SnapshotCapableEventStore>).storeSnapshot !== "function") {
    throw new Error(
      `${module.identity} declares a snapshot policy, but this entry's \`eventStore\` cannot serve one. ` +
        "Wrap it at the composition root in the snapshotting wrapper for its persistence " +
        "family — `<family>SnapshottingEventStore(store, …)`.",
    )
  }
  return eventStore as SnapshotCapableEventStore
}

async function applyEvent<Id, S>(
  module: State<Id, S, any>,
  state: S,
  event: EventMessage,
): Promise<S> {
  const eventType = qualifiedNameToString(event.name)

  for (const [descriptor, evolve] of module.evolvers) {
    const evolverType = qualifiedNameToString(descriptor.name)
    if (evolverType === eventType) {
      return await evolve(state, event)
    }
  }

  return state
}

// ---------------------------------------------------------------------------
// The lazy per-site cache. Nothing here is registered, declared or counted.
// ---------------------------------------------------------------------------

/**
 * eventStore → state identity → repository.
 *
 * TWO LEVELS, not three. There used to be a snapshot store in the middle,
 * because a site was a PAIR of objects and either one changing meant a
 * different fold. A site is ONE object now — the log, capability and all — so
 * the cache is keyed on the one thing it was always really keyed on.
 *
 * WEAK on the store, so a site that goes out of scope takes its repositories
 * with it — a fixture per test case leaks nothing. STRONG on the state
 * identity, because a state is a module-level value that outlives everything
 * anyway, and the inner map dies with the site above it.
 */
const repositories = new WeakMap<EventStore, Map<string, StateRepository<any, any>>>()

/**
 * The repository for ONE state at ONE site — built on first use, remembered by
 * the identity of the objects it was built from.
 *
 * This is what replaced registration. `ctx.load(Course, id)` is a pure function
 * of the state value it is handed and the site the entry carries, so there is
 * nothing for a host to declare: the first load builds the fold, every later
 * one finds it. Two entries pointing at the same `eventStore` OBJECT share it
 * for the same reason they always did — it is the same object — and two
 * entries pointing at different logs get different folds, without anybody
 * having grouped anything.
 *
 * The per-task dedupe is a different concern and lives where it always did, on
 * `uow.stateCache`: this cache remembers the MACHINERY, that one remembers the
 * ANSWER.
 */
export function repositoryFor<Id, S>(
  state: State<Id, S, any>,
  eventStore: EventStore,
): StateRepository<Id, S> {
  let byState = repositories.get(eventStore)
  if (!byState) {
    byState = new Map()
    repositories.set(eventStore, byState)
  }

  // Keyed on the definition's `identity` — the handle `state()` assigns per
  // definition — not on the object reference, which a spread would break.
  const existing = byState.get(state.identity)
  if (existing) return existing as StateRepository<Id, S>

  const created = eventSourcedRepository(state, eventStore)
  byState.set(state.identity, created)
  return created
}
