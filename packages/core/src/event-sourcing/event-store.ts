import type { EventMessage } from "../messaging/messages.js"
import type { StreamableEventSource } from "../event-processing/source.js"
import type { SourcingCondition } from "./sourcing-condition.js"
import type { AppendCondition } from "./append-condition.js"
import type { ConsistencyMarker } from "./consistency-marker.js"
import type { Snapshot } from "./snapshot.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * Result of sourcing events — the events plus a consistency marker
 * representing the position up to which events were read.
 *
 * And, when the condition carried a snapshot strategy AND something served it,
 * the cached fold the events run ON TOP OF.
 */
export type SourcingResult = {
  readonly events: ReadonlyArray<EventMessage>
  readonly marker: ConsistencyMarker
  /**
   * THE LEADING ELEMENT of the result: a fold somebody already computed, which
   * `events` continues from. Present only when the condition asked for the
   * snapshotting strategy and whoever served the read had a usable entry.
   *
   * IT IS NOT AN EVENT, and it is deliberately a separate field rather than a
   * synthetic first element of `events`. An event is a fact with a name, a
   * version, tags and a payload, and every reader of `events` is entitled to
   * treat it as one — folding it, matching it with `is()`, counting it.
   * A snapshot is a folded STATE. Smuggling one in as a fake event would make
   * every consumer of a sourcing result complicit in the lie; a field they can
   * ignore costs the ones that do not care exactly nothing.
   *
   * The reader that DOES care is the repository: it starts the fold from
   * `snapshot.state` instead of `initial(id)`, and `events` — sourced strictly
   * after `snapshot.position` — is folded on top.
   */
  readonly snapshot?: Snapshot
}

/**
 * An append in two halves: stage, then commit or roll back. `afterCommit`
 * resolves the marker the write settled at. Used by the stores' own `append`
 * and by the test recorder; a handler never sees it.
 */
export type AppendTransaction = {
  commit(): Promise<void>
  afterCommit(): Promise<ConsistencyMarker>
  rollback(): void
}

/**
 * THE LOG. A source you can read by condition, append to under a condition,
 * and open as a stream from a position.
 *
 * ONE TYPE. It used to be `EventStorageEngine & EventBus`, with the bus half
 * carrying `publish` and `subscribe` — Axon 4's decomposition. Nothing read
 * through `subscribe`: processors open a stream and are woken by the store
 * (LISTEN/NOTIFY, a gRPC stream, an in-memory listener), and `publish` was an
 * append without a condition. Both are gone; what is left is what the DCB
 * model needs.
 *
 * ── THIS CONTRACT MENTIONS SNAPSHOTS NOWHERE, AND THAT IS THE POINT ─────────
 *
 * Snapshotting is a CAPABILITY TIER on top of it, added by WRAPPING a store:
 * `postgresSnapshottingEventStore(postgresEventStore(pg, …), pg, …)`. See
 * {@link SnapshotCapableEventStore}. A store that never gets wrapped has no
 * snapshot surface to misuse.
 */
export type EventStore = StreamableEventSource & {
  /** Read the events matching a condition, with the marker the read reached. */
  source(condition: SourcingCondition): Promise<SourcingResult>
  /** Stage an append; the returned transaction commits or rolls it back. */
  appendEvents(
    events: ReadonlyArray<EventMessage>,
    condition?: AppendCondition,
    uow?: UnitOfWork,
  ): Promise<AppendTransaction>
  /** Append under a condition and settle. The common path. */
  append(
    events: ReadonlyArray<EventMessage>,
    condition?: AppendCondition,
    uow?: UnitOfWork,
  ): Promise<ConsistencyMarker>
}

/**
 * A log that ALSO caches folds — the capability tier, and the only place
 * snapshotting exists.
 *
 * ONE MEMBER IS ADDED, and it is the WRITE. `storeSnapshot` replaces whatever
 * was filed under `key`; there is no `loadSnapshot`, because reading is not a
 * second call — a capable store honours `condition.snapshot` inside `source()`
 * and leads its {@link SourcingResult} with the cached fold, which is what lets
 * a store that owns its query serve the whole thing in ONE round trip. The read
 * is therefore already in `EventStore`'s shape; only the write needed a name.
 *
 * IT IS AN INTERSECTION, spelled by hand rather than derived from one wrapper
 * with `ReturnType` (the way `CorrelatingUnitOfWork` is derived from
 * `correlating`). Correlation has ONE composer, so the function can be the
 * source of truth. This capability has FOUR, one per storage family, in four
 * packages — `inMemorySnapshottingEventStore`, `postgresSnapshottingEventStore`,
 * `kronosDbSnapshottingEventStore`, `axonServerSnapshottingEventStore` — and
 * deriving the contract from any one of them would make three packages
 * downstream of a fourth's implementation detail. So the CONTRACT is written
 * here, and each wrapper's return type is annotated with it; the type probe
 * asserts all four still satisfy it.
 *
 * HOW A HOST GETS ONE: by wrapping, at the composition root, once.
 *
 * ```ts
 * const eventStore = postgresSnapshottingEventStore(
 *   postgresEventStore(pg, { tagResolver }),
 *   pg,
 *   { serializer },
 * )
 * ```
 *
 * Nothing on a handler's context names this tier. `state({ snapshot })` says a
 * state wants caching, the wrapped log serves it through `ctx.load`, and a
 * policy loaded through a bare log is refused at runtime by `capableOrThrow`
 * in `repository.ts`, on the first load.
 */
export type SnapshotCapableEventStore = EventStore & SnapshotStoreCapability

/**
 * WHAT A SNAPSHOTTING WRAPPER ADDS, named on its own — so a wrapper can be
 * ADDITIVE rather than collapsing.
 *
 * A wrapper typed `(next: EventStore) => SnapshotCapableEventStore` would
 * LAUNDER every other capability its input carried: the runtime object still
 * delegates them, but the type says only "a capable event store", and anything
 * the inner store had — a second tier, a family-specific member — is gone from
 * the caller's view. So the four family wrappers are spelled
 * `<E extends EventStore>(next: E, …) => E & SnapshotStoreCapability`, which is the
 * general rule for a capability adder: preserve the input's type, intersect the
 * addition.
 */
export type SnapshotStoreCapability = {
  /**
   * Replace the cached fold filed under `key`.
   *
   * The unit of work is a TRAILING parameter, matching `TokenStore` and
   * `SequencedDeadLetterQueue`: pass it and the write joins that task's adapter
   * transaction, omit it and the store opens its own.
   *
   * THE WRITE IS THE FOLD'S, which is why it is a plain member rather than a
   * `ctx` capability. The thing being cached is a fold's own output at a fold's
   * own position, and the only parties holding both are the repository (for a
   * `state()` with a policy) and a hand-rolled `ctx.source` fold — and the
   * latter already has the store in its resources.
   */
  storeSnapshot(key: string, snapshot: Snapshot, uow?: UnitOfWork): Promise<void>
}
