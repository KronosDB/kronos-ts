/**
 * The CAPABILITY TIER, end to end over `inMemoryEventStore`.
 *
 * Everything here is judged through a real repository over a real log, because
 * the claim being made is about a READ, and a read is only interesting when
 * something folds it. The one exception is the composition-order block, which
 * is about two wrappers and needs to see the events themselves.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata, event, is, type EventMessage } from "../../messaging/messages.js"
import { generateIdentifier } from "../../messaging/identifier.js"
import { state } from "../../event-sourcing/state.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { eventSourcedRepository } from "../../event-sourcing/repository.js"
import { sourcingCondition } from "../../event-sourcing/sourcing-condition.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import { upcastingEventStore } from "../../upcasting/upcasting-event-store.js"
import type { SnapshotCapableEventStore } from "../../event-sourcing/event-store.js"
import { afterEvents, snapshotIdentifier } from "../snapshot.js"
import { inMemorySnapshottingEventStore } from "../in-memory-snapshotting-event-store.js"

// ---------------------------------------------------------------------------
// Domain — a counter, so "how many events did this load fold" is countable.
// ---------------------------------------------------------------------------

const Bumped = event({
  name: qn("counting", "Bumped"),
  payload: z.object({ counterId: z.string(), by: z.number() }),
  tags: { counterId: (p) => p.counterId },
})

type CounterState = { count: number }

/** Every fold this suite runs is counted, so "events after the snapshot" is provable. */
let folds = 0

/**
 * NO `name` ANYWHERE. A state that snapshots declares WHERE and WHEN —
 * `snapshot: { key, when }` — and `key` is a string this file wrote. Entries
 * land under `"<key>:<flattened id>"`, so the tests below compose that same
 * string by hand: there is nothing to ask the framework for.
 */
const counter = (initial: () => CounterState = () => ({ count: 0 })) =>
  state({
    id: { counterId: z.string() },
    tags: ({ counterId }) => ({ counterId }),
    evolve: [
      initial,
      [Bumped, (s, { payload }) => {
        folds++
        return { count: s.count + payload.by }
      }],
    ],
    snapshot: { key: "counter-v1", when: afterEvents(2) },
  })

const Counter = counter()
const KEY = "counter-v1"

function bumped(counterId: string, by: number): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: qn("counting", "Bumped"),
    version: "1.0",
    payload: { counterId, by },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags: [{ key: "counterId", value: counterId }],
  } as EventMessage
}

async function appendBumps(store: EventStore, counterId: string, n: number): Promise<void> {
  for (let i = 0; i < n; i++) await store.append([bumped(counterId, 1)])
}

/**
 * What a capable store has filed under a key — read the only way there is, by
 * making a read that asks for it.
 */
async function cached(store: SnapshotCapableEventStore, composed: string) {
  const result = await store.source(
    sourcingCondition({ tags: { counterId: "no-such-counter" } }, undefined, { key: composed }),
  )
  return result.snapshot
}

/** Snapshot writes are fire-and-forget, so poll rather than assume. */
async function waitForSnapshot(
  store: SnapshotCapableEventStore,
  key: string,
  id: unknown,
  timeoutMs = 1000,
) {
  const composed = `${key}:${snapshotIdentifier(id)}`
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const found = await cached(store, composed)
    if (found) return found
    await new Promise((r) => setTimeout(r, 5))
  }
  return undefined
}

// ---------------------------------------------------------------------------

describe("inMemorySnapshottingEventStore — the capability, client-side", () => {
  it("the policy writes an entry, and the NEXT load leads with it", async () => {
    // given — a log, a cache, and the two composed into one store
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(log)
    const repo = eventSourcedRepository(Counter, eventStore)

    await appendBumps(log, "c-1", 5)

    // when — the first load replays everything, which is what earns the entry
    const first = await repo.load({ counterId: "c-1" })
    expect(first.state.count).toBe(5)

    const entry = await waitForSnapshot(eventStore, KEY, { counterId: "c-1" })

    // then — what was cached is the FOLD, at the position it was folded to
    expect(entry).toBeDefined()
    expect(entry!.state).toEqual({ count: 5 })
    expect(entry!.position).toBe(4n)

    // and the next load starts from it
    const second = await repo.load({ counterId: "c-1" })
    expect(second.state.count).toBe(5)
  })

  it("sources events strictly AFTER the snapshot position — the fold count proves it", async () => {
    // given — a cache entry that already covers the first three events
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(log)
    const repo = eventSourcedRepository(Counter, eventStore)

    await appendBumps(log, "c-2", 5)
    await eventStore.storeSnapshot(`${KEY}:${snapshotIdentifier({ counterId: "c-2" })}`, {
      state: { count: 3 },
      position: 2n,
    })

    // when
    folds = 0
    const result = await repo.load({ counterId: "c-2" })

    // then — three from the cache, two folded, and the two are all that were read
    expect(result.state.count).toBe(5)
    expect(folds).toBe(2)
  })

  it("a MISS falls back to a full replay", async () => {
    // given — a composed store over an empty cache
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(log)
    const repo = eventSourcedRepository(Counter, eventStore)

    await appendBumps(log, "c-3", 4)

    // when
    folds = 0
    const result = await repo.load({ counterId: "c-3" })

    // then
    expect(result.state.count).toBe(4)
    expect(folds).toBe(4)
  })

  it("a capability that THROWS on WRITE does not take the load down with it", async () => {
    // given — a log whose cache cannot be written to at all. The read half is
    // the base store's, so it simply never leads with anything.
    const log = inMemoryEventStore()
    const broken: SnapshotCapableEventStore = {
      ...log,
      async storeSnapshot() {
        throw new Error("cache is down")
      },
    }
    const repo = eventSourcedRepository(Counter, broken)

    await appendBumps(log, "c-4", 3)

    // when / then — the load is correct; it just costs what it always cost, and
    // the swallowed write is the rule that a cache is never load-bearing.
    folds = 0
    const result = await repo.load({ counterId: "c-4" })
    expect(result.state.count).toBe(3)
    expect(folds).toBe(3)
  })

  it("an UNFIT entry is DISCARDED and the load replays in full", async () => {
    // given — an entry written before the fold grew a `total` field. The
    // current initial state is the specimen; the cached value lacks a key it
    // declares, which is exactly the hazard: new code would read `undefined`.
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(log)
    const grown = counter(() => ({ count: 0, total: 0 }) as never)
    const repo = eventSourcedRepository(grown, eventStore)

    await appendBumps(log, "c-5", 4)
    await eventStore.storeSnapshot(`${"counter-v1"}:${snapshotIdentifier({ counterId: "c-5" })}`, {
      state: { count: 999 },        // no `total` — a shape this fold cannot use
      position: 2n,
    })

    // when
    folds = 0
    const result = await repo.load({ counterId: "c-5" })

    // then — nothing was migrated; everything was replayed
    expect(result.state.count).toBe(4)
    expect(folds).toBe(4)
  })

  it("and the policy then writes a FRESH entry of the current shape", async () => {
    // given — the same unfit entry
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(log)
    const grown = counter(() => ({ count: 0, total: 0 }) as never)
    const repo = eventSourcedRepository(grown, eventStore)

    await appendBumps(log, "c-5b", 4)
    await eventStore.storeSnapshot(`${"counter-v1"}:${snapshotIdentifier({ counterId: "c-5b" })}`, {
      state: { count: 999 },
      position: 2n,
    })

    // when — the replay folded 4 events, so afterEvents(2) fires
    await repo.load({ counterId: "c-5b" })
    const fresh = await waitForSnapshot(eventStore, "counter-v1", { counterId: "c-5b" })

    // then — the cache heals itself, with no migration written anywhere: the
    // poisoned `count: 999` is gone, replaced by what the replay actually
    // folded. (The fold rebuilds its state from `count` alone, so `total` is
    // not carried — which is the fold's business, not the cache's.)
    expect(fresh).toBeDefined()
    expect((fresh!.state as CounterState).count).toBe(4)
  })

  it("a FIT entry STARTS the fold — extra keys on it are tolerated", async () => {
    // given — an entry carrying a field the fold has since dropped
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(log)
    const repo = eventSourcedRepository(Counter, eventStore)

    await appendBumps(log, "c-6", 4)
    await eventStore.storeSnapshot(`${KEY}:${snapshotIdentifier({ counterId: "c-6" })}`, {
      state: { count: 3, retired: true },
      position: 2n,
    })

    // when
    folds = 0
    const result = await repo.load({ counterId: "c-6" })

    // then — leftovers are harmless; the fold read what it needed
    expect(result.state.count).toBe(4)
    expect(folds).toBe(1)
  })

  it("a RENAMED key orphans old entries — the whole invalidation story", async () => {
    // given — an entry filed under the key the state used to declare
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(log)

    await appendBumps(log, "c-7", 4)
    await eventStore.storeSnapshot(`counter-v1:${snapshotIdentifier({ counterId: "c-7" })}`, {
      state: { count: 3 },
      position: 2n,
    })

    // when — the fold's meaning changed, so the author changed the key. One
    // character in a diff; no migration, no version column, no heuristic.
    const renamed = state({
      id: { counterId: z.string() },
      tags: ({ counterId }) => ({ counterId }),
      evolve: [
        (): CounterState => ({ count: 0 }),
        [Bumped, (st, { payload }) => { folds++; return { count: st.count + payload.by } }],
      ],
      snapshot: { key: "counter-v2", when: afterEvents(2) },
    })

    folds = 0
    const result = await eventSourcedRepository(renamed, eventStore)
      .load({ counterId: "c-7" })

    // then — the old entry is simply not found, and the load replays
    expect(result.state.count).toBe(4)
    expect(folds).toBe(4)
    // it was never migrated, only orphaned
    expect((await cached(eventStore, `counter-v1:${snapshotIdentifier({ counterId: "c-7" })}`))!.state)
      .toEqual({ count: 3 })
  })

  it("ctx.source is NEVER fused — the raw verb carries no key, so nothing is served", async () => {
    // given — a cache entry that WOULD have been used by a state load
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(log)

    await appendBumps(log, "c-9", 4)
    await eventStore.storeSnapshot(`${KEY}:${snapshotIdentifier({ counterId: "c-9" })}`, {
      state: { count: 3 },
      position: 2n,
    })

    // when — a raw read, which is what `sourceFunction` builds: no snapshot key
    const condition = sourcingCondition({ tags: { counterId: "c-9" } })
    expect(condition.snapshot).toBeUndefined()
    const result = await eventStore.source(condition)

    // then — every event, no leading snapshot
    expect(result.events.length).toBe(4)
    expect(result.snapshot).toBeUndefined()
  })

  it("delegates every other read path untouched", async () => {
    // given
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(log)

    const seen: EventMessage[] = []
    const unsubscribe = eventStore.subscribe(async (events) => {
      seen.push(...events)
    })

    // when — a write through the decorator, and a stream off it
    await eventStore.append([bumped("c-10", 1)])
    const stream = eventStore.open({ position: 0n })
    const first = stream.next()

    // then
    expect(seen.length).toBe(1)
    expect(first?.event.name.name).toBe("Bumped")
    expect(await eventStore.getHeadPosition()).toBe(1n)

    stream.close()
    unsubscribe()
  })
})

// ---------------------------------------------------------------------------
// Composition with the OTHER log-boundary mechanism.
// ---------------------------------------------------------------------------

describe("composed with upcastingEventStore", () => {
  const BumpedV0 = event({
    name: qn("counting", "Bumped"),
    version: "0.9",
    payload: z.object({ counterId: z.string() }),   // no `by` back then
    tags: { counterId: (p) => p.counterId },
  })

  /** The documented idiom: a typed switch, target version off the CURRENT descriptor. */
  const byAdded = (e: EventMessage): EventMessage =>
    is(e, BumpedV0)
      ? ({ ...e, version: Bumped.version, payload: { ...e.payload, by: 1 } } as EventMessage)
      : e

  function legacyBump(counterId: string): EventMessage {
    return {
      kind: "event",
      identifier: generateIdentifier(),
      name: qn("counting", "Bumped"),
      version: "0.9",
      payload: { counterId },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [{ key: "counterId", value: counterId }],
    } as EventMessage
  }

  it("upcasting OUTERMOST — the documented order — converts everything the fold sees", async () => {
    // given
    const log = inMemoryEventStore()
    const eventStore = upcastingEventStore(inMemorySnapshottingEventStore(log), byAdded)
    const repo = eventSourcedRepository(Counter, eventStore)

    await log.append([legacyBump("c-a")])
    await appendBumps(log, "c-a", 2)
    await eventStore.storeSnapshot(`${KEY}:${snapshotIdentifier({ counterId: "c-a" })}`, {
      state: { count: 1 },
      position: 0n,
    })

    // when — the leading snapshot covers the legacy event; the two after it are
    // upcast on the way out
    const result = await repo.load({ counterId: "c-a" })

    // then
    expect(result.state.count).toBe(3)
  })

  it("upcasting INNERMOST gives the same answer today — a snapshot is a state, not an event", async () => {
    // given — the reverse composition, same log, same cache
    const log = inMemoryEventStore()
    const eventStore = inMemorySnapshottingEventStore(upcastingEventStore(log, byAdded))
    const repo = eventSourcedRepository(Counter, eventStore)

    await log.append([legacyBump("c-b")])
    await appendBumps(log, "c-b", 2)
    await eventStore.storeSnapshot(`${KEY}:${snapshotIdentifier({ counterId: "c-b" })}`, {
      state: { count: 1 },
      position: 0n,
    })

    // when
    const result = await repo.load({ counterId: "c-b" })

    // then — identical, because the snapshot layer only ever NARROWS the range
    // and an `Upcast` is `(event) => event`, which a folded state was never
    // going to be. The outermost spelling is preferred for staying true if that
    // ever stops holding, not because the two differ now.
    expect(result.state.count).toBe(3)
  })

  it("an upcaster still reaches every event a full replay reads, in either order", async () => {
    // given — no cache entry at all, so both compositions replay
    const log = inMemoryEventStore()
    await log.append([legacyBump("c-c")])

    // when
    const outer = await upcastingEventStore(
      inMemorySnapshottingEventStore(log), byAdded,
    ).source(sourcingCondition({ tags: { counterId: "c-c" } }))
    const inner = await inMemorySnapshottingEventStore(
      upcastingEventStore(log, byAdded),
    ).source(sourcingCondition({ tags: { counterId: "c-c" } }))

    // then
    expect(outer.events[0]!.version).toBe("1.0")
    expect(inner.events[0]!.version).toBe("1.0")
  })
})
