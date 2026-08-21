/**
 * THE RAW SNAPSHOTTING LAYER — `ctx.source(query, { snapshot })` plus an
 * `eventStore.storeSnapshot(key, …)` call, which together ARE the capability.
 *
 * BOTH HALVES COME OFF ONE OBJECT now: the read verb grew an overload, and the
 * write is a member of the log the slice already holds.
 *
 * Everything `state({ snapshot })` does is these four lines with the key
 * composition and the policy written for you. That is the layering, and it is
 * why this file comes before the sugar's.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata, event, is, type EventMessage } from "../../messaging/messages.js"
import { generateIdentifier } from "../../messaging/identifier.js"
import { inMemoryEventStore } from "../in-memory.js"
import { sourceFunction } from "../load.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { inMemorySnapshottingEventStore } from "../in-memory-snapshotting-event-store.js"
import { sourcingCondition } from "../sourcing-condition.js"
import type { EventStore, SnapshotCapableEventStore } from "../event-store.js"

const Bumped = event({
  name: qn("counting", "Bumped"),
  payload: z.object({ counterId: z.string(), by: z.number() }),
  tags: { counterId: (p) => p.counterId },
})

type CounterState = { count: number }

function bumped(counterId: string, by = 1): EventMessage {
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

/** The site a handler's `ctx.source` is built from. */
function site(eventStore: EventStore) {
  const uow = unitOfWork()
  return { uow, source: sourceFunction({ uow, eventStore }) }
}

async function appendBumps(store: EventStore, counterId: string, n: number) {
  for (let i = 0; i < n; i++) await store.append([bumped(counterId)])
}

/** THE RAW FOLD, written by hand — `is()` + `reduce`, exactly as documented. */
const fold = (events: ReadonlyArray<EventMessage>, from: CounterState): CounterState =>
  events.reduce(
    (s, e) => (is(e, Bumped) ? { count: s.count + e.payload.by } : s),
    from,
  )

describe("ctx.source(query) — the plain read is untouched", () => {
  it("still answers with the events array and nothing else", async () => {
    const log = inMemoryEventStore()
    await appendBumps(log, "c-1", 3)

    const events = await site(log).source({ tags: { counterId: "c-1" } })

    expect(Array.isArray(events)).toBe(true)
    expect(events.length).toBe(3)
  })

  it("carries no snapshot key onto the condition", async () => {
    const seen: unknown[] = []
    const log = inMemoryEventStore()
    const spy: EventStore = {
      ...log,
      async source(condition) { seen.push(condition.snapshot); return log.source(condition) },
    }
    await site(spy).source({ tags: { counterId: "c-x" } })
    expect(seen).toEqual([undefined])
  })
})

/** A log with the capability on it — the one line of client-side wiring. */
function wired() {
  const log = inMemoryEventStore()
  const eventStore = inMemorySnapshottingEventStore(log)
  return { log, snapshots: eventStore, eventStore }
}

/** What a capable store has filed under a key, read the only way there is. */
async function cached(store: SnapshotCapableEventStore, key: string) {
  const result = await store.source(
    sourcingCondition({ tags: { counterId: "no-such-counter" } }, undefined, { key }),
  )
  return result.snapshot
}

describe("ctx.source(query, { snapshot }) — the fused read", () => {
  it("a MISS hands back no snapshot and the whole matching history", async () => {
    const { log, eventStore } = wired()
    await appendBumps(log, "c-2", 3)

    const result = await site(eventStore).source(
      { tags: { counterId: "c-2" } },
      { snapshot: "counter:c-2" },
    )

    expect(result.snapshot).toBeUndefined()
    expect(result.events.length).toBe(3)
    expect(fold(result.events, { count: 0 })).toEqual({ count: 3 })
  })

  it("a HIT leads with the cached fold and only the events after it", async () => {
    const { log, snapshots, eventStore } = wired()
    await appendBumps(log, "c-3", 5)
    await snapshots.storeSnapshot("counter:c-3", { state: { count: 3 }, position: 2n })

    const result = await site(eventStore).source(
      { tags: { counterId: "c-3" } },
      { snapshot: "counter:c-3" },
    )

    expect(result.snapshot!.state).toEqual({ count: 3 })
    expect(result.events.length).toBe(2)
    expect(fold(result.events, result.snapshot!.state as CounterState)).toEqual({ count: 5 })
  })

  it("WRITE then FUSED READ roundtrips, and the position is the one to store", async () => {
    const { log, snapshots, eventStore } = wired()
    await appendBumps(log, "c-4", 4)

    // First pass: full replay, then the user decides to cache it.
    const first = await site(eventStore).source(
      { tags: { counterId: "c-4" } },
      { snapshot: "counter:c-4" },
    )
    const state = fold(first.events, { count: 0 })
    await snapshots.storeSnapshot("counter:c-4", { state, position: first.position })

    // More events arrive.
    await appendBumps(log, "c-4", 2)

    // Second pass: the cache is used, and only the new events come back.
    const second = await site(eventStore).source(
      { tags: { counterId: "c-4" } },
      { snapshot: "counter:c-4" },
    )

    expect(second.snapshot!.position).toBe(first.position)
    expect(second.events.length).toBe(2)
    expect(fold(second.events, second.snapshot!.state as CounterState)).toEqual({ count: 6 })
  })

  it("the position advances with the log, so a re-store keeps the cache current", async () => {
    const { log, snapshots, eventStore } = wired()
    await appendBumps(log, "c-5", 3)

    const first = await site(eventStore).source(
      { tags: { counterId: "c-5" } }, { snapshot: "counter:c-5" },
    )
    await snapshots.storeSnapshot("counter:c-5", { state: fold(first.events, { count: 0 }), position: first.position })

    await appendBumps(log, "c-5", 3)
    const second = await site(eventStore).source(
      { tags: { counterId: "c-5" } }, { snapshot: "counter:c-5" },
    )
    expect(second.position).toBeGreaterThan(first.position)

    await snapshots.storeSnapshot("counter:c-5", {
      state: fold(second.events, second.snapshot!.state as CounterState),
      position: second.position,
    })

    const third = await site(eventStore).source(
      { tags: { counterId: "c-5" } }, { snapshot: "counter:c-5" },
    )
    expect(third.events.length).toBe(0)
    expect(third.snapshot!.state).toEqual({ count: 6 })
  })

  it("RENAMING THE KEY orphans the old entry — the whole invalidation story", async () => {
    const { log, snapshots, eventStore } = wired()
    await appendBumps(log, "c-6", 4)
    await snapshots.storeSnapshot("counter-v1:c-6", { state: { count: 99 }, position: 2n })

    // The fold's meaning changed, so the user changed the key.
    const result = await site(eventStore).source(
      { tags: { counterId: "c-6" } },
      { snapshot: "counter-v2:c-6" },
    )

    expect(result.snapshot).toBeUndefined()
    expect(result.events.length).toBe(4)
    // and the old entry is untouched, not migrated — just unreachable
    expect((await cached(snapshots, "counter-v1:c-6"))!.state).toEqual({ count: 99 })
  })

  it("FITNESS IS THE USER'S CODE — a value they reject costs them one full read", async () => {
    const { log, snapshots, eventStore } = wired()
    await appendBumps(log, "c-7", 4)
    await snapshots.storeSnapshot("counter:c-7", { state: { totallyWrong: true }, position: 2n })

    const ctx = site(eventStore)
    const fused = await ctx.source({ tags: { counterId: "c-7" } }, { snapshot: "counter:c-7" })

    // The framework handed the value over without an opinion; the user checks.
    const usable = (v: unknown): v is CounterState =>
      typeof v === "object" && v !== null && typeof (v as CounterState).count === "number"

    const state = usable(fused.snapshot?.state)
      ? fold(fused.events, fused.snapshot.state)
      : fold(await ctx.source({ tags: { counterId: "c-7" } }), { count: 0 })

    expect(state).toEqual({ count: 4 })
  })

  it("an UNREACHABLE cache is a miss, never a failed read", async () => {
    const log = inMemoryEventStore()
    // A capability whose lookup cannot be reached at all — what a transport
    // wrapper looks like when its snapshot service is down.
    const broken: SnapshotCapableEventStore = {
      ...log,
      async storeSnapshot() { throw new Error("cache is down") },
      async source(condition) {
        if (condition.snapshot === undefined) return log.source(condition)
        // The lookup threw; the rule is that a cache is never load-bearing, so
        // the read falls through to the whole matching history.
        return log.source(sourcingCondition(condition.query, condition.start))
      },
    }
    await appendBumps(log, "c-8", 3)

    const result = await site(broken).source(
      { tags: { counterId: "c-8" } }, { snapshot: "counter:c-8" },
    )
    expect(result.snapshot).toBeUndefined()
    expect(result.events.length).toBe(3)
  })

  it("a store that IGNORES the key is still correct — just not accelerated", async () => {
    const log = inMemoryEventStore()   // bare: never wrapped, so nothing serves the key
    await appendBumps(log, "c-9", 3)

    const result = await site(log).source(
      { tags: { counterId: "c-9" } }, { snapshot: "counter:c-9" },
    )
    expect(result.snapshot).toBeUndefined()
    expect(result.events.length).toBe(3)
  })
})

describe("the append condition is stamped identically either way", () => {
  it("a fused read records the same query and marker a plain read records", async () => {
    const { log, snapshots, eventStore } = wired()
    await appendBumps(log, "c-10", 4)
    await snapshots.storeSnapshot("counter:c-10", { state: { count: 2 }, position: 1n })

    const plainCtx = site(eventStore)
    await plainCtx.source({ tags: { counterId: "c-10" } })
    const plain = plainCtx.uow.events.sourcingInfos.at(-1)!

    const fusedCtx = site(eventStore)
    await fusedCtx.source({ tags: { counterId: "c-10" } }, { snapshot: "counter:c-10" })
    const fused = fusedCtx.uow.events.sourcingInfos.at(-1)!

    // Fusing narrows which EVENTS come back; it does not narrow what was READ.
    expect(fused.query).toEqual(plain.query)
    expect(fused.markerPosition).toBe(plain.markerPosition)
  })
})

/**
 * THE DOCUMENTED IDIOM, run as written. This is the whole mechanism at the raw
 * layer — a fused source under a key you wrote, your own fold, your own fitness
 * judgement, an `if` for the policy, and `seam.store` with the position the read
 * came back with. `state({ snapshot })` is these lines with the key composition
 * and the policy filled in for you, and nothing else.
 */
describe("the raw idiom, end to end", () => {
  it("caches on the first pass and rides the cache on the second", async () => {
    const { log, snapshots, eventStore } = wired()
    await appendBumps(log, "c-11", 120)

    const run = async () => {
      const ctx = site(eventStore)
      const key = "counter:c-11"
      const { snapshot, events, position } = await ctx.source(
        { tags: { counterId: "c-11" } },
        { snapshot: key },
      )
      const state = fold(events, (snapshot?.state as CounterState) ?? { count: 0 })
      if (events.length > 100) await snapshots.storeSnapshot(key, { state, position })
      return { state, read: events.length }
    }

    const first = await run()
    expect(first.state).toEqual({ count: 120 })
    expect(first.read).toBe(120)          // nothing cached yet: a full replay

    const second = await run()
    expect(second.state).toEqual({ count: 120 })
    expect(second.read).toBe(0)           // the cache covered everything
  })
})
