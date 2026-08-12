/**
 * Per-state repository options — the `[state, options]` tuple in the flat
 * registration list.
 *
 * The regression this guards: `kronos` used to build EVERY repository with
 * `snapshotPolicy: undefined`, so a snapshot policy could only be applied by
 * re-registering the repository by hand through `app.stateManagers`. These
 * tests assert the declarative form does the wiring, and that a tuple as the
 * FIRST argument is not mistaken for the optional overrides record.
 */
import { describe, expect, it } from "bun:test"
import { emptyMetadata, qn } from "@kronos-ts/common"
import {
  afterEvents,
  inMemoryEventStore,
  inMemorySnapshotStore,
  noSnapshotPolicy,
} from "@kronos-ts/eventsourcing"
import { command, commandHandler, EventCriteria, event } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { z } from "zod"
import { kronos, module } from "../kronos.js"

// ---------------------------------------------------------------------------
// Domain: a counter that appends one event per Bump, so the number of events
// a load observes is exactly the number of Bumps that came before it.
// ---------------------------------------------------------------------------

const Bumped = event({
  name: qn("counting", "Bumped"),
  payload: z.object({ counterId: z.string() }),
  tags: (p) => [{ key: "counterId", value: p.counterId }],
})

const Bump = command({
  name: qn("counting", "Bump"),
  payload: z.object({ counterId: z.string() }),
})

const Counter = state({
  name: "Counter",
  id: { counterId: z.string() },
  initial: () => ({ count: 0 }),
  criteria: ({ counterId }) => EventCriteria.havingTags({ counterId }),
  evolve: (on) => [on(Bumped, (s) => ({ ...s, count: s.count + 1 }))],
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
  tags: (p) => [{ key: "tickerId", value: p.tickerId }],
})

const Tick = command({
  name: qn("counting", "Tick"),
  payload: z.object({ tickerId: z.string() }),
})

const Ticker = state({
  name: "Ticker",
  id: { tickerId: z.string() },
  initial: () => ({ ticks: 0 }),
  criteria: ({ tickerId }) => EventCriteria.havingTags({ tickerId }),
  evolve: (on) => [on(Ticked, (s) => ({ ...s, ticks: s.ticks + 1 }))],
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
  it("a [state, options] tuple as the FIRST registration is not read as overrides", async () => {
    // THE AMBIGUITY: `module()` told the optional overrides record apart from
    // registrations with `"kind" in first`. A tuple is an array and has no
    // `kind`, so this exact call — tuple first, NO overrides record — used to
    // swallow the state as a component record: no repository, no handler
    // reachable, and the failure only shows up at dispatch.
    const eventStore = inMemoryEventStore()
    const snapshotStore = inMemorySnapshotStore()

    const app = kronos({
      components: { eventStore, snapshotStore },
      modules: [module("counting", [Counter, { snapshotPolicy: afterEvents(1) }], bump)],
    })

    // The handler is registered — the tuple did not become the module's
    // component record.
    await app.commandGateway.send(Bump, { counterId: "c-1" }, emptyMetadata())
    await app.commandGateway.send(Bump, { counterId: "c-1" }, emptyMetadata())

    // The app's stores are still the ones supplied, not an empty record.
    const { events } = await eventStore.source({
      criteria: EventCriteria.havingTags({ counterId: "c-1" }),
    } as never)
    expect(events.length).toBe(2)

    // ...and the policy came through: the 3rd Bump's load sees 2 events, which
    // is > 1, so afterEvents(1) fires.
    await app.commandGateway.send(Bump, { counterId: "c-1" }, emptyMetadata())
    expect(await waitForSnapshot(snapshotStore, "Counter", { counterId: "c-1" })).toBeDefined()

    await app.stop()
  })

  it("a bare state still registers with no policy — nothing is snapshotted", async () => {
    const snapshotStore = inMemorySnapshotStore()
    const app = kronos({
      components: { eventStore: inMemoryEventStore(), snapshotStore },
      modules: [module("counting", Counter, bump)],
    })

    for (let i = 0; i < 5; i++) {
      await app.commandGateway.send(Bump, { counterId: "c-bare" }, emptyMetadata())
    }

    expect(await snapshotStore.load("Counter", { counterId: "c-bare" })).toBeUndefined()
    await app.stop()
  })

  it("two states in one module get DIFFERENT policies", async () => {
    const snapshotStore = inMemorySnapshotStore()
    const app = kronos({
      components: { eventStore: inMemoryEventStore(), snapshotStore },
      modules: [
        module(
          "counting",
          [Counter, { snapshotPolicy: afterEvents(1) }],
          [Ticker, { snapshotPolicy: noSnapshotPolicy() }],
          bump,
          tick,
        ),
      ],
    })

    for (let i = 0; i < 4; i++) {
      await app.commandGateway.send(Bump, { counterId: "c-2" }, emptyMetadata())
      await app.commandGateway.send(Tick, { tickerId: "t-2" }, emptyMetadata())
    }

    expect(await waitForSnapshot(snapshotStore, "Counter", { counterId: "c-2" })).toBeDefined()
    // Same module, same store, opposite policy.
    expect(await snapshotStore.load("Ticker", { tickerId: "t-2" })).toBeUndefined()

    await app.stop()
  })

  it("a per-state snapshotStore overrides the module's", async () => {
    const moduleSnapshots = inMemorySnapshotStore()
    const counterSnapshots = inMemorySnapshotStore()

    const app = kronos({
      components: { eventStore: inMemoryEventStore(), snapshotStore: inMemorySnapshotStore() },
      modules: [
        module(
          "counting",
          { eventStore: inMemoryEventStore(), snapshotStore: moduleSnapshots },
          [Counter, { snapshotPolicy: afterEvents(1), snapshotStore: counterSnapshots }],
          bump,
        ),
      ],
    })

    for (let i = 0; i < 3; i++) {
      await app.commandGateway.send(Bump, { counterId: "c-3" }, emptyMetadata())
    }

    expect(await waitForSnapshot(counterSnapshots, "Counter", { counterId: "c-3" })).toBeDefined()
    expect(await moduleSnapshots.load("Counter", { counterId: "c-3" })).toBeUndefined()

    await app.stop()
  })

  it("tuples mix freely with bare states, handlers and an overrides record", async () => {
    const snapshotStore = inMemorySnapshotStore()
    const app = kronos({
      modules: [
        module(
          "counting",
          { eventStore: inMemoryEventStore(), snapshotStore },
          Ticker,
          [Counter, { snapshotPolicy: afterEvents(1) }],
          bump,
          tick,
        ),
      ],
    })

    for (let i = 0; i < 3; i++) {
      await app.commandGateway.send(Bump, { counterId: "c-4" }, emptyMetadata())
      await app.commandGateway.send(Tick, { tickerId: "t-4" }, emptyMetadata())
    }

    expect(await waitForSnapshot(snapshotStore, "Counter", { counterId: "c-4" })).toBeDefined()
    expect(await snapshotStore.load("Ticker", { tickerId: "t-4" })).toBeUndefined()

    await app.stop()
  })
})
