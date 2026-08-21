/**
 * Snapshot configuration, now that it rides on the STATE VALUE.
 *
 * `state({ snapshot })` says how often; the entry's `eventStore` says where —
 * because a log that caches folds is a log that was WRAPPED.
 * Both halves have to be present for anything to be read or written, and
 * neither is registered anywhere — the state reaches `ctx.load` because the
 * handler passed it, and the store reaches `ctx.load` because the host attached
 * it to the entry.
 *
 * The regression this still guards: every repository used to be built with
 * `snapshotPolicy: undefined`, so a policy could only be applied by re-wiring
 * the repository by hand.
 */
import { describe, expect, it } from "bun:test"
import { emptyMetadata, qn, command, event } from "../messaging/messages.js"
import {
  afterEvents,
  noSnapshotPolicy,
  snapshotIdentifier,
} from "../event-sourcing/snapshot.js"
import { inMemoryEventStore } from "../event-sourcing/in-memory.js"
import { inMemorySnapshottingEventStore } from "../event-sourcing/in-memory-snapshotting-event-store.js"
import { sourcingCondition } from "../event-sourcing/sourcing-condition.js"
import { commandHandler } from "../command-handling/handler.js"
import { state } from "../event-sourcing/state.js"
import { z } from "zod"
import { kronos } from "../kronos.js"
import { correlation, interceptingCommandBus, interceptingQueryBus, send, unitOfWork, localCommandBus, localQueryBus } from "../index.js"

/**
 * The two buses each entry names. The UoW runner is named once and handed to
 * both, which is what makes the sharing checkable at a glance.
 */
function inMemoryBuses() {
  const uow = unitOfWork
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(uow), correlation),
  }
}


// ---------------------------------------------------------------------------
// Domain: a counter that appends one event per Bump, so the number of events
// a load observes is exactly the number of Bumps that came before it.
// ---------------------------------------------------------------------------

const Bumped = event({
  name: qn("counting", "Bumped"),
  payload: z.object({ counterId: z.string() }),
  tags: { counterId: (p) => p.counterId },
})

const Bump = command({
  name: qn("counting", "Bump"),
  payload: z.object({ counterId: z.string() }),
})

/** NO `name`: the cache key is derived from this fold's own data. */
const Counter = state({
  id: { counterId: z.string() },
  tags: ({ counterId }) => ({ counterId }),
  evolve: [() => ({ count: 0 }), [Bumped, (s) => ({ ...s, count: s.count + 1 })]],
  snapshot: { key: "counter-v1", when: afterEvents(1) },
})

/**
 * The same fold, the same log, NO policy — the other half of "per state".
 *
 * It keeps a `name`, and this is exactly what `name` is FOR now: it folds the
 * same event under the same id and tag keys as `Counter`, so derivation cannot
 * tell the two apart (a fold is a function, and functions are not data). One of
 * them says `name` and moves out of the way.
 */
const PlainCounter = state({
  id: { counterId: z.string() },
  tags: ({ counterId }) => ({ counterId }),
  evolve: [() => ({ count: 0 }), [Bumped, (s) => ({ ...s, count: s.count + 1 })]],
})

const bump = commandHandler(Bump, async ({ payload }, ctx) => {
  // The load is what a snapshot policy observes.
  await ctx.load(Counter, { counterId: payload.counterId })
  ctx.append(Bumped, { counterId: payload.counterId })
})

const bumpPlain = commandHandler(Bump, async ({ payload }, ctx) => {
  await ctx.load(PlainCounter, { counterId: payload.counterId })
  ctx.append(Bumped, { counterId: payload.counterId })
})

// A second state so "per-state" means something: same log, same store,
// different policy.
const Ticked = event({
  name: qn("counting", "Ticked"),
  payload: z.object({ tickerId: z.string() }),
  tags: { tickerId: (p) => p.tickerId },
})

const Tick = command({
  name: qn("counting", "Tick"),
  payload: z.object({ tickerId: z.string() }),
})

/** Different event, different id key — its derived key cannot collide. */
const Ticker = state({
  id: { tickerId: z.string() },
  tags: ({ tickerId }) => ({ tickerId }),
  evolve: [() => ({ ticks: 0 }), [Ticked, (s) => ({ ...s, ticks: s.ticks + 1 })]],
  snapshot: { key: "ticker-v1", when: noSnapshotPolicy() },
})

const tick = commandHandler(Tick, async ({ payload }, ctx) => {
  await ctx.load(Ticker, { tickerId: payload.tickerId })
  ctx.append(Ticked, { tickerId: payload.tickerId })
})

/** A log that caches folds — one line, exactly as a host writes it. */
function cachingLog() {
  return inMemorySnapshottingEventStore(inMemoryEventStore())
}

/** What a capable log has filed under a composed key, read the only way there is. */
async function cached(
  store: ReturnType<typeof cachingLog>,
  declaredKey: string,
  id: unknown,
) {
  const result = await store.source(
    sourcingCondition({ tags: { nothing: "matches-this" } }, undefined, {
      key: `${declaredKey}:${snapshotIdentifier(id)}`,
    }),
  )
  return result.snapshot
}

/** Snapshot writes are fire-and-forget, so poll rather than assume. */
async function waitForSnapshot(
  store: ReturnType<typeof cachingLog>,
  declaredKey: string,
  id: unknown,
  timeoutMs = 1000,
) {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const found = await cached(store, declaredKey, id)
    if (found) return found
    await new Promise((r) => setTimeout(r, 5))
  }
  return undefined
}

describe("snapshot config on the state value", () => {
  it("the key is the string the state declared — nothing derives one", () => {
    // `Counter` and `PlainCounter` fold the SAME event under the same id and
    // tag keys. Nothing in the framework tries to tell them apart, because
    // whether two folds share a cache is a judgement about MEANING and only the
    // author can make it. They say so, in one field.
    expect(Counter.snapshot!.key).toBe("counter-v1")
    expect(Ticker.snapshot!.key).toBe("ticker-v1")
    expect("name" in Counter).toBe(false)
  })

  it("a state carrying a policy snapshots at a site whose log is CAPABLE", async () => {
    const eventStore = cachingLog()

    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [
        { ...bump, ...buses, eventStore },
      ],
    })

    await send(buses.commandBus, Bump, { counterId: "c-1" }, emptyMetadata())
    await send(buses.commandBus, Bump, { counterId: "c-1" }, emptyMetadata())

    const { events } = await eventStore.source({
      query: { tags: { counterId: "c-1" } },
    } as never)
    expect(events.length).toBe(2)

    // The 3rd Bump's load sees 2 events, which is > 1, so afterEvents(1) fires.
    await send(buses.commandBus, Bump, { counterId: "c-1" }, emptyMetadata())
    expect(await waitForSnapshot(eventStore, "counter-v1", { counterId: "c-1" })).toBeDefined()

    await app.stop()
  })

  it("a state with NO policy is never snapshotted, capable log or not", async () => {
    const eventStore = cachingLog()
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [{ ...bumpPlain, ...buses, eventStore }],
    })

    for (let i = 0; i < 5; i++) {
      await send(buses.commandBus, Bump, { counterId: "c-bare" }, emptyMetadata())
    }

    expect(await cached(eventStore, "plaincounter-v1", { counterId: "c-bare" })).toBeUndefined()
    await app.stop()
  })

  it("a policy over a BARE log is refused — the demand, at run time", async () => {
    // The types refuse this pairing outright (see `snapshot-demand.types.ts`);
    // this is the same refusal for a caller who had no compiler. It surfaces
    // through the handling, naming the state and the fix.
    const eventStore = inMemoryEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [{ ...bump, ...buses, eventStore }],
    })

    await expect(
      send(buses.commandBus, Bump, { counterId: "c-nostore" }, emptyMetadata()),
    ).rejects.toThrow(/declares a snapshot policy/)

    await app.stop()
  })

  it("two states on one log get DIFFERENT policies", async () => {
    const eventStore = cachingLog()
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [
        { ...bump, ...buses, eventStore },
        { ...tick, ...buses, eventStore },
      ],
    })

    for (let i = 0; i < 4; i++) {
      await send(buses.commandBus, Bump, { counterId: "c-2" }, emptyMetadata())
      await send(buses.commandBus, Tick, { tickerId: "t-2" }, emptyMetadata())
    }

    expect(await waitForSnapshot(eventStore, "counter-v1", { counterId: "c-2" })).toBeDefined()
    // Same log, same cache, opposite policy — because the policy is on the state.
    expect(await cached(eventStore, "ticker-v1", { tickerId: "t-2" })).toBeUndefined()

    await app.stop()
  })

  it("two ENTRIES can point at DIFFERENT capable logs — the cache follows the log", async () => {
    // One store object per entry, capabilities and all. Two entries naming two
    // wrapped logs have two caches, and nobody had to say so twice.
    const counterLog = cachingLog()
    const tickerLog = cachingLog()

    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [
        { ...bump, ...buses, eventStore: counterLog },
        { ...tick, ...buses, eventStore: tickerLog },
      ],
    })

    for (let i = 0; i < 3; i++) {
      await send(buses.commandBus, Bump, { counterId: "c-3" }, emptyMetadata())
    }

    expect(await waitForSnapshot(counterLog, "counter-v1", { counterId: "c-3" })).toBeDefined()
    expect(await cached(tickerLog, "counter-v1", { counterId: "c-3" })).toBeUndefined()

    await app.stop()
  })

  // -- `name` is durable snapshot identity, and nothing else ----------------

  it("an UNNAMED state loads fine when nothing would ever write a snapshot", async () => {
    const eventStore = inMemoryEventStore()
    const Anonymous = state({
      id: { counterId: z.string() },
      tags: ({ counterId }) => ({ counterId }),
      evolve: [() => ({ count: 0 }), [Bumped, (s) => ({ ...s, count: s.count + 1 })]],
    })
    const seenCounts: number[] = []
    const bumpAnonymous = commandHandler(Bump, async ({ payload }, ctx) => {
      const before = await ctx.load(Anonymous, { counterId: payload.counterId })
      seenCounts.push(before.count)
      ctx.append(Bumped, { counterId: payload.counterId })
    })

    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [{ ...bumpAnonymous, ...buses, eventStore }],
    })

    await send(buses.commandBus, Bump, { counterId: "anon-1" }, emptyMetadata())
    await send(buses.commandBus, Bump, { counterId: "anon-1" }, emptyMetadata())

    // Sourcing works — the fold was built from the state's identity, not from a
    // name it does not have.
    expect(seenCounts).toEqual([0, 1])
    await app.stop()
  })

  it("a snapshotting state declares WHERE and WHEN, and nothing else", () => {
    const Anonymous = state({
      id: { counterId: z.string() },
      tags: ({ counterId }) => ({ counterId }),
      evolve: [() => ({ count: 0 }), [Bumped, (s) => ({ ...s, count: s.count + 1 })]],
      snapshot: { key: "anon-v1", when: afterEvents(1) },
    })
    // `name` used to be mandatory here, because somebody had to invent the
    // cache key. Nobody does any more: it comes off the id keys, the tag keys
    // and the folded events.
    expect(Anonymous.snapshot).toEqual({ key: "anon-v1", when: expect.anything() })
  })

  it("a state that folds NOTHING can still declare a key", () => {
    const Empty = state({
      id: { counterId: z.string() },
      tags: ({ counterId }) => ({ counterId }),
      evolve: [() => ({ count: 0 })],
      snapshot: { key: "empty-v1", when: afterEvents(1) },
    })
    expect(Empty.snapshot!.key).toBe("empty-v1")
  })
})
