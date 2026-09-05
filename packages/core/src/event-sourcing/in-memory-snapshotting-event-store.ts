// ---------------------------------------------------------------------------
// IN-MEMORY SNAPSHOTS — the capability tier's null implementation, and what
// every fixture runs on.
//
// A `Map` and a narrowed read. Nothing about it is clever, which is the point:
// if the capability needed cleverness to be implementable in memory it would be
// the wrong capability.
//
// THIS IS THE CLIENT-SIDE FUSION, and it is the shape every family that does
// not own its query ends up with: look the entry up, resume after its position,
// and lead the result with what was found. Two round trips where a store that
// owns its query needs one — see `postgresSnapshottingEventStore`, which fuses
// both halves into a single statement because it holds the connection.
// ---------------------------------------------------------------------------

import type { EventStore, SnapshotStoreCapability } from "./event-store.js"
import type { Snapshot } from "./snapshot.js"
import { withoutSnapshotKey } from "./sourcing-condition.js"

/**
 * Add the snapshotting capability to any event store, served from memory.
 *
 * THING FIRST, capability second — the same order every wrapper here uses. What
 * comes back is the store you passed in, PLUS `storeSnapshot`, so nothing the
 * inner store carried is laundered on the way through: wrap an upcasting store
 * and the result is still an upcasting store, wrap this in an upcasting store
 * and the result is still snapshot-capable. Both orders typecheck, and both are
 * pinned by the type probe.
 *
 * ```ts
 * const eventStore = inMemorySnapshottingEventStore(inMemoryEventStore())
 * ```
 *
 * ONE READ PATH CHANGES, and only when asked. `source()` serves a condition
 * that carries `snapshot`; a condition without one is passed down untouched,
 * which is every `ctx.source(query)`, every state without a policy, and every
 * load in a process that never wrapped anything. Everything else delegates —
 * `open()`, `subscribe()`, every write member, the token and position members —
 * and nothing here can change what is in the log, because the cache is
 * downstream of the truth, always.
 *
 * COMPOSED WITH UPCASTING, THE SANE ORDER IS UPCASTING OUTERMOST:
 *
 * ```ts
 * upcastingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore()), upcast)
 * ```
 *
 * Read it from the inside out. The snapshot layer decides WHICH events are
 * read; the upcast layer decides what each of them MEANS. Put upcasting
 * outermost and every event that reaches the fold has been converted, whether
 * it came from the log this time or the last time. The other order gives the
 * same answers today — the snapshot layer only ever narrows the range, and a
 * cached STATE was never an `(event) => event`'s business — so the preference
 * is for the spelling that stays true if that ever changes. Both orders keep
 * both capabilities in the type; that is the wrappers being additive.
 *
 * THE CACHE IS NEVER LOAD-BEARING. A miss falls back to full sourcing, as does
 * an entry the fold later judges unfit. Nothing in this one can fail, which is
 * exactly why it is the null implementation.
 */
export function inMemorySnapshottingEventStore<E extends EventStore>(
  next: E,
): E & SnapshotStoreCapability {
  const snapshots = new Map<string, Snapshot>()

  return {
    ...next,

    async storeSnapshot(key: string, snapshot: Snapshot): Promise<void> {
      // REPLACE. A cache has a current entry, not a history — which is also why
      // there is no delete: writing IS how you invalidate.
      snapshots.set(key, snapshot)
    },

    async source(condition) {
      const key = condition.snapshot
      if (key === undefined) return next.source(condition)

      // THE STRATEGY IS CONSUMED HERE. Whatever happens below, the store we
      // delegate to gets a plain condition — it is this wrapper's job now, and
      // a store that would also have handled it must not handle it twice.
      const plain = withoutSnapshotKey(condition)
      const snapshot = snapshots.get(key.key)
      if (snapshot === undefined) return next.source(plain)

      // Resume AFTER the position the snapshot already folded. A condition that
      // independently asked to start later keeps its own floor, so the two
      // narrowings compose rather than fight.
      const resumeFrom = snapshot.position + 1n
      const start =
        plain.start !== undefined && plain.start > resumeFrom ? plain.start : resumeFrom

      const result = await next.source({ ...plain, start })
      return { ...result, snapshot }
    },
    // The spread of a generic is opaque to the checker, so the shape it
    // produces is asserted rather than inferred. The probe is what makes the
    // assertion honest: it pins that the result still satisfies BOTH the
    // capability and whatever `E` was.
  } as E & SnapshotStoreCapability
}
