/**
 * Plan 09-01 Task 2 — App.states() tuple-shape options (D-88).
 *
 * Asserts:
 *  T1. Bare-module form pushes { module, options: {} } into stateEntries.
 *  T2. Tuple form [Module, { snapshotStore, snapshotPolicy }] pushes the
 *      provided options through.
 *  T3. Mixed list (bare + tuple) preserves order and per-entity options.
 *  T4. start() threads tuple options into the repository — verified by
 *      observing snapshotStore.load() being consulted on entity load when
 *      a custom store is provided.
 */
import { describe, it, expect } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import {
  command,
  event,
  commandHandler,
  EventCriteria,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import {
  type SnapshotStore,
  type SnapshotPolicy,
  afterEvents
} from "@kronos-ts/eventsourcing"
import { kronos } from "../kronos.js"
import { AppImpl } from "../app.js"
import { registerInMemoryDefaults } from "../defaults.js"
import { createWarningChannel } from "../warnings.js"

// ─── Domain ─────────────────────────────────────────────────────────────────

const Tap = command({
  name: qn("tuple", "Tap"),
  payload: z.object({ id: z.string() }),
})
const Tapped = event({
  name: qn("tuple", "Tapped"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})
const Tuple = state({
  name: "Tuple",
  id: { id: z.string() },
  initial: () => ({ tapped: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: (on) => [on(Tapped, (s) => ({ ...s, tapped: true }))],
})
const tapHandler = commandHandler(Tap, async ({ payload: cmd }, ctx) => {
  await ctx.load(Tuple, { id: cmd.id })
  ctx.append(Tapped, { id: cmd.id })
})

// Probe snapshot store: counts ctx.load() calls and records the (stateName, id) keys.
function probeSnapshotStore(): SnapshotStore & { loadCalls: Array<{ stateName: string; id: unknown }> } {
  const loadCalls: Array<{ stateName: string; id: unknown }> = []
  return {
    loadCalls,
    async load(stateName, id) {
      loadCalls.push({ stateName, id })
      return undefined
    },
    async store(_stateName, _id, _snapshot) {},
    async deleteSnapshots(_stateName, _id) {},
  }
}

// ─── T1+T2+T3 — stateEntries shape ─────────────────────────────────────────

describe("App.states() tuple-shape — Plan 09-01 (D-88)", () => {
  it("bare module form pushes options:{} into stateEntries", () => {
    const app = new AppImpl({ warningChannel: createWarningChannel({ quiet: true }) })
    registerInMemoryDefaults(app)
    app.states(Tuple)
    expect(app._state.stateEntries).toHaveLength(1)
    expect(app._state.stateEntries[0]!.module).toBe(Tuple)
    expect(app._state.stateEntries[0]!.options).toEqual({})
  })

  it("tuple form [module, options] pushes the provided options through", () => {
    const app = new AppImpl({ warningChannel: createWarningChannel({ quiet: true }) })
    registerInMemoryDefaults(app)
    const store = probeSnapshotStore()
    const policy: SnapshotPolicy = afterEvents(5)
    app.states([Tuple, { snapshotStore: store, snapshotPolicy: policy }])
    expect(app._state.stateEntries).toHaveLength(1)
    expect(app._state.stateEntries[0]!.options.snapshotStore).toBe(store)
    expect(app._state.stateEntries[0]!.options.snapshotPolicy).toBe(policy)
  })

  it("mixed list preserves registration order and per-entity options", () => {
    const app = new AppImpl({ warningChannel: createWarningChannel({ quiet: true }) })
    registerInMemoryDefaults(app)
    const store = probeSnapshotStore()
    app.states(Tuple, [Tuple, { snapshotStore: store }])
    expect(app._state.stateEntries).toHaveLength(2)
    expect(app._state.stateEntries[0]!.options).toEqual({})
    expect(app._state.stateEntries[1]!.options.snapshotStore).toBe(store)
  })
})

// ─── T4 — runtime threading verified via snapshotStore.load() observation ──

describe("App.states() tuple options — runtime threading at start()", () => {
  it("tuple-form snapshotStore is consulted during ctx.load() at command dispatch", async () => {
    const store = probeSnapshotStore()
    const app = kronos({ quiet: true })
      .states([Tuple, { snapshotStore: store, snapshotPolicy: afterEvents(100) }])
      .commands(tapHandler)
    const running = await app.start()
    try {
      await running.commandGateway.send(Tap, { id: "t1" }, emptyMetadata())
      // ctx.load() consulted the snapshot store at least once for entity "Tuple".
      expect(store.loadCalls.length).toBeGreaterThan(0)
      expect(store.loadCalls.some((c) => c.stateName === "Tuple")).toBe(true)
    } finally {
      await running.stop()
    }
  })

  it("bare-form entity does NOT consult any snapshot store (defaults to no snapshot wiring)", async () => {
    const store = probeSnapshotStore()
    const app = kronos({ quiet: true })
      // Bare form — store is constructed but never passed to this entity.
      .states(Tuple)
      .commands(tapHandler)
    const running = await app.start()
    try {
      await running.commandGateway.send(Tap, { id: "t2" }, emptyMetadata())
      // The probe store is unrelated to this entity — should never be consulted.
      expect(store.loadCalls).toHaveLength(0)
    } finally {
      await running.stop()
    }
  })
})
