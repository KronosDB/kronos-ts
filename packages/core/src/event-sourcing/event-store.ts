import type { EventMessage } from "../messaging/messages.js"
import type { EventBus } from "./event-bus.js"
import type { EventStorageEngine } from "./event-storage-engine.js"
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
 * The event store — dual-role component that combines event storage
 * with event distribution.
 *
 * Extends:
 * - `EventStorageEngine` — raw storage (source, append, stream)
 * - `EventBus` — event publication + push-based subscription
 *
 * In an event sourcing context, the EventStore persists events durably while
 * simultaneously distributing them to subscribed event handlers, eliminating
 * the need for a separate EventBus component.
 *
 * ── THIS CONTRACT MENTIONS SNAPSHOTS NOWHERE, AND THAT IS THE POINT ─────────
 *
 * It is COMPLETE for event sourcing: a log you can source from, append to,
 * stream and subscribe to is everything the DCB model needs, and most
 * well-designed projects never need one line more. Snapshotting is not a
 * missing piece of this contract and never was — it is a CAPABILITY TIER on
 * top of it, and if it exists at all it exists ON THE EVENT STORE, added by
 * WRAPPING one: `postgresSnapshottingEventStore(postgresEventStore(pg, …), pg, …)`.
 * See {@link SnapshotCapableEventStore}.
 *
 * A store that never gets wrapped has no snapshot surface to misuse, no seam
 * to leave unwired, and no field on an entry to forget. That is not an
 * omission; it is the base being the right size.
 */
export type EventStore = EventStorageEngine & EventBus

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
 * And the compiler makes you: a handler that `ctx.load`s a state declaring a
 * snapshot policy does not typecheck against an entry whose `eventStore` is
 * bare. See `IfSnapshotCapable` in `load.ts` — the one demand every read
 * surface derives from.
 */
export type SnapshotCapableEventStore = EventStore & SnapshotCapability

/**
 * WHAT A SNAPSHOTTING WRAPPER ADDS, named on its own — so a wrapper can be
 * ADDITIVE rather than collapsing.
 *
 * A wrapper typed `(next: EventStore) => SnapshotCapableEventStore` would
 * LAUNDER every other capability its input carried: the runtime object still
 * delegates them, but the type says only "a capable event store", and anything
 * the inner store had — a second tier, a family-specific member — is gone from
 * the caller's view. So the four family wrappers are spelled
 * `<E extends EventStore>(next: E, …) => E & SnapshotCapability`, which is the
 * general rule for a capability adder: preserve the input's type, intersect the
 * addition.
 */
export type SnapshotCapability = {
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
