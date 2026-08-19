/**
 * Per-state repository options — the `[state, options]` tuple in the flat
 * handler list.
 *
 * The regression this guards: `kronos` used to build EVERY repository with
 * `snapshotPolicy: undefined`, so a snapshot policy could only be applied by
 * re-wiring the repository by hand. These
 * tests assert the declarative form does the wiring, and that a tuple as the
 * FIRST argument is not mistaken for the optional overrides record.
 */
import { describe, expect, it } from "bun:test"
import { emptyMetadata } from "../../primitives/metadata.js"
import { qn } from "../../primitives/qualified-name.js"
import { afterEvents, noSnapshotPolicy } from "../../state/snapshot-policy.js"
import { inMemoryEventStore } from "../../stores/in-memory-event-store.js"
import { inMemorySnapshotStore } from "../../stores/snapshot-store.js"
import { command, event } from "../../messages/descriptor.js"
import { commandHandler } from "../../handlers/command-handler.js"
import { state } from "../../state/state.js"
import { z } from "zod"
import { kronos } from "../kronos.js"
import { lineage, interceptingCommandBus, interceptingQueryBus, send, unitOfWork, simpleCommandBus, simpleQueryBus } from "../../index.js"

/**
 * The two buses each entry names. The UoW runner is named once and handed to
 * both, which is what makes the sharing checkable at a glance.
 */
function inMemoryBuses() {
  const uow = unitOfWork
  return {
    commandBus: interceptingCommandBus(simpleCommandBus(uow), lineage),
    queryBus: interceptingQueryBus(simpleQueryBus(uow), lineage),
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

const Counter = state({
  name: "Counter",
  id: { counterId: z.string() },
  initial: () => ({ count: 0 }),
  tags: ({ counterId }) => ({ counterId }),
  evolve: [[Bumped, (s) => ({ ...s, count: s.count + 1 })]],
})

const bump = commandHandler(Bump, async ({ payload }, ctx) => {
  // The load is what a snapshot policy observes.
  await ctx.load(Counter, { counterId: payload.counterId })
  ctx.append(Bumped, { counterId: payload.counterId })
})

// A second state so "per-state" means something: same module, same stores,
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

const Ticker = state({
  name: "Ticker",
  id: { tickerId: z.string() },
  initial: () => ({ ticks: 0 }),
  tags: ({ tickerId }) => ({ tickerId }),
  evolve: [[Ticked, (s) => ({ ...s, ticks: s.ticks + 1 })]],
})

const tick = commandHandler(Tick, async ({ payload }, ctx) => {
  await ctx.load(Ticker, { tickerId: payload.tickerId })
  ctx.append(Ticked, { tickerId: payload.tickerId })
})

/** Snapshot writes are fire-and-forget, so poll rather than assume. */
async function waitForSnapshot(
  store: ReturnType<typeof inMemorySnapshotStore>,
  stateName: string,
  id: unknown,
  timeoutMs = 1000,
) {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const found = await store.load(stateName, id)
    if (found) return found
    await new Promise((r) => setTimeout(r, 5))
  }
  return undefined
}

describe("per-state options", () => {
  it("a [state, options] tuple as the FIRST handler wires correctly", async () => {
    // NOTE ON TEST INTENT: `kronos` now takes `handlers` as an explicit
    // named field, not a variadic list, so the historical ambiguity —
    // `module()` telling an optional overrides record apart from a leading
    // `[state, options]` tuple via `"kind" in first` — cannot occur here any
    // more; there is no positional argument to misread. A one-line
    // `Array.isArray` inside the `states` loop is all that still tells a tuple
    // apart from a bare state, so this test keeps its original assertions: a
    // tuple in the first slot must wire a real repository with its snapshot
    // policy, not get swallowed.
    const eventStore = inMemoryEventStore()
    const snapshotStore = inMemorySnapshotStore()

    const buses = inMemoryBuses()
    const app = kronos({
      states: [
        [{ ...Counter, eventStore, snapshotStore }, { snapshotPolicy: afterEvents(1) }],
      ],
      commandHandlers: [
        { ...bump, ...buses, eventStore, snapshotStore },
      ],
    })

    // The handler is subscribed — the tuple did not become the stores
    // component record.
    await send(buses.commandBus, Bump, { counterId: "c-1" }, emptyMetadata())
    await send(buses.commandBus, Bump, { counterId: "c-1" }, emptyMetadata())

    // The app's stores are still the ones supplied, not an empty record.
    const { events } = await eventStore.source({
      query: { tags: { counterId: "c-1" } },
    } as never)
    expect(events.length).toBe(2)

    // ...and the policy came through: the 3rd Bump's load sees 2 events, which
    // is > 1, so afterEvents(1) fires.
    await send(buses.commandBus, Bump, { counterId: "c-1" }, emptyMetadata())
    expect(await waitForSnapshot(snapshotStore, "Counter", { counterId: "c-1" })).toBeDefined()

    await app.stop()
  })

  it("a bare state still wires up with no policy — nothing is snapshotted", async () => {
    const snapshotStore = inMemorySnapshotStore()
    const eventStore = inMemoryEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      states: [{ ...Counter, eventStore, snapshotStore }],
      commandHandlers: [{ ...bump, ...buses, eventStore, snapshotStore }],
    })

    for (let i = 0; i < 5; i++) {
      await send(buses.commandBus, Bump, { counterId: "c-bare" }, emptyMetadata())
    }

    expect(await snapshotStore.load("Counter", { counterId: "c-bare" })).toBeUndefined()
    await app.stop()
  })

  it("two states in one module get DIFFERENT policies", async () => {
    const snapshotStore = inMemorySnapshotStore()
    const eventStore = inMemoryEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      states: [
        [{ ...Counter, eventStore, snapshotStore }, { snapshotPolicy: afterEvents(1) }],
        [{ ...Ticker, eventStore, snapshotStore }, { snapshotPolicy: noSnapshotPolicy() }],
      ],
      commandHandlers: [
        { ...bump, ...buses, eventStore, snapshotStore },
        { ...tick, ...buses, eventStore, snapshotStore },
      ],
    })

    for (let i = 0; i < 4; i++) {
      await send(buses.commandBus, Bump, { counterId: "c-2" }, emptyMetadata())
      await send(buses.commandBus, Tick, { tickerId: "t-2" }, emptyMetadata())
    }

    expect(await waitForSnapshot(snapshotStore, "Counter", { counterId: "c-2" })).toBeDefined()
    // Same module, same store, opposite policy.
    expect(await snapshotStore.load("Ticker", { tickerId: "t-2" })).toBeUndefined()

    await app.stop()
  })

  it("a per-state snapshotStore overrides the log's", async () => {
    const groupSnapshots = inMemorySnapshotStore()
    const counterSnapshots = inMemorySnapshotStore()

    const eventStore = inMemoryEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      states: [
        [{ ...Counter, eventStore, snapshotStore: groupSnapshots }, { snapshotPolicy: afterEvents(1), snapshotStore: counterSnapshots }],
      ],
      commandHandlers: [
        { ...bump, ...buses, eventStore, snapshotStore: groupSnapshots },
      ],
    })

    for (let i = 0; i < 3; i++) {
      await send(buses.commandBus, Bump, { counterId: "c-3" }, emptyMetadata())
    }

    expect(await waitForSnapshot(counterSnapshots, "Counter", { counterId: "c-3" })).toBeDefined()
    expect(await groupSnapshots.load("Counter", { counterId: "c-3" })).toBeUndefined()

    await app.stop()
  })

  it("tuples mix freely with bare states in the states field", async () => {
    const snapshotStore = inMemorySnapshotStore()
    const eventStore = inMemoryEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      states: [
        { ...Ticker, eventStore, snapshotStore },
        [{ ...Counter, eventStore, snapshotStore }, { snapshotPolicy: afterEvents(1) }],
      ],
      commandHandlers: [
        { ...bump, ...buses, eventStore, snapshotStore },
        { ...tick, ...buses, eventStore, snapshotStore },
      ],
    })

    for (let i = 0; i < 3; i++) {
      await send(buses.commandBus, Bump, { counterId: "c-4" }, emptyMetadata())
      await send(buses.commandBus, Tick, { tickerId: "t-4" }, emptyMetadata())
    }

    expect(await waitForSnapshot(snapshotStore, "Counter", { counterId: "c-4" })).toBeDefined()
    expect(await snapshotStore.load("Ticker", { tickerId: "t-4" })).toBeUndefined()

    await app.stop()
  })

  // -- `name` is durable snapshot identity, and nothing else ----------------

  it("an UNNAMED state boots fine when nothing would ever write a snapshot", async () => {
    const eventStore = inMemoryEventStore()
    const Anonymous = state({
      id: { counterId: z.string() },
      initial: () => ({ count: 0 }),
      tags: ({ counterId }) => ({ counterId }),
      evolve: [[Bumped, (s) => ({ ...s, count: s.count + 1 })]],
    })
    const bumpAnonymous = commandHandler(Bump, async ({ payload }, ctx) => {
      const before = await ctx.load(Anonymous, { counterId: payload.counterId })
      seenCounts.push(before.count)
      ctx.append(Bumped, { counterId: payload.counterId })
    })
    const seenCounts: number[] = []

    const buses = inMemoryBuses()
    const app = kronos({
      states: [{ ...Anonymous, eventStore }],
      commandHandlers: [{ ...bumpAnonymous, ...buses, eventStore }],
    })

    await send(buses.commandBus, Bump, { counterId: "anon-1" }, emptyMetadata())
    await send(buses.commandBus, Bump, { counterId: "anon-1" }, emptyMetadata())

    // Sourcing works — the repository was found by the state's identity, not
    // by a name it does not have.
    expect(seenCounts).toEqual([0, 1])
    await app.stop()
  })

  it("an UNNAMED state with a snapshot policy is a BOOT ERROR that says which state", () => {
    const eventStore = inMemoryEventStore()
    const Anonymous = state({
      id: { counterId: z.string() },
      initial: () => ({ count: 0 }),
      tags: ({ counterId }) => ({ counterId }),
      evolve: [[Bumped, (s) => ({ ...s, count: s.count + 1 })]],
    })

    expect(() =>
      kronos({
        ...inMemoryBuses(),
        states: [
          { ...Ticker, eventStore },
          [{ ...Anonymous, eventStore }, { snapshotPolicy: afterEvents(1) }],
        ],
      }),
    ).toThrow(/state at index 1 of `states` \(folds counting\.Bumped\).*no `name`/s)
  })

  it("an UNNAMED state given its own snapshot STORE is the same boot error", () => {
    const eventStore = inMemoryEventStore()
    const Anonymous = state({
      id: { counterId: z.string() },
      initial: () => ({ count: 0 }),
      tags: ({ counterId }) => ({ counterId }),
      evolve: [],
    })

    expect(() =>
      kronos({
        ...inMemoryBuses(),
        states: [[{ ...Anonymous, eventStore }, { snapshotStore: inMemorySnapshotStore() }]],
      }),
    ).toThrow(/a snapshot store, but has no `name`/)
  })
})
