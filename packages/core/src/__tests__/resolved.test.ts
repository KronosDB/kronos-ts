import { describe, it, expect } from "bun:test"
import { SlotRegistry } from "../slot-registry.js"
import { buildResolved } from "../resolved.js"
import { CircularSlotDependencyError, SlotNotRegisteredError } from "../errors.js"
import type { KronosComponents } from "../components.js"
import type { Serializer } from "@kronos-ts/common"
import type { CommandBus, QueryBus } from "@kronos-ts/messaging"

const stubSerializer = { serialize: () => ({ type: "t", data: {}, revision: 0 }), deserialize: () => ({}) } as unknown as Serializer

describe("buildResolved — lazy + memoized Proxy", () => {
  it("Test 1: factory called once and memoized (factory call count = 1 across multiple property accesses)", () => {
    const registry = new SlotRegistry()
    let callCount = 0
    registry.forceSet("serializer", (_resolved) => {
      callCount++
      return stubSerializer
    })
    const resolved = buildResolved(registry)
    const a = resolved.serializer
    const b = resolved.serializer
    const c = resolved.serializer
    expect(callCount).toBe(1)
    expect(a).toBe(stubSerializer)
    expect(b).toBe(stubSerializer)
    expect(c).toBe(stubSerializer)
  })

  it("Test 2: factory accessing a sibling slot via Resolved resolves the dependency lazily", () => {
    const registry = new SlotRegistry()
    // eventStore factory depends on serializer
    registry.forceSet("serializer", (_resolved) => stubSerializer)
    registry.forceSet("eventStore", ({ serializer }) => {
      // uses serializer dependency
      return { serialize: serializer.serialize } as unknown as KronosComponents["eventStore"]
    })
    const resolved = buildResolved(registry)
    const eventStore = resolved.eventStore
    // eventStore was built using the resolved serializer
    expect(typeof eventStore.serialize).toBe("function")
  })

  it("Test 3: Cycle detection — slotA factory calls resolved.slotB, slotB factory calls resolved.slotA", () => {
    const registry = new SlotRegistry()
    // commandBus factory reads queryBus, queryBus factory reads commandBus → cycle
    registry.forceSet("commandBus", ({ queryBus }) => queryBus as unknown as CommandBus)
    registry.forceSet("queryBus", ({ commandBus }) => commandBus as unknown as QueryBus)
    const resolved = buildResolved(registry)
    let thrown: unknown
    try {
      resolved.commandBus
    } catch (e) {
      thrown = e
    }
    expect(thrown).toBeInstanceOf(CircularSlotDependencyError)
    const err = thrown as CircularSlotDependencyError
    // commandBus is the re-entry slot (it was resolving when queryBus tried to resolve it again)
    expect(err.slot).toBe("commandBus")
    expect(err.chain).toContain("commandBus")
    expect(err.chain).toContain("queryBus")
  })

  it("Test 4: Slot with no registered factory throws SlotNotRegisteredError", () => {
    const registry = new SlotRegistry()
    const resolved = buildResolved(registry)
    let thrown: unknown
    try {
      resolved.unitOfWorkFactory
    } catch (e) {
      thrown = e
    }
    expect(thrown).toBeInstanceOf(SlotNotRegisteredError)
    const err = thrown as SlotNotRegisteredError
    expect(err.slot).toBe("unitOfWorkFactory")
  })

  it("Test 5: After cycle throw, the resolving set is cleaned up (try/finally) — a subsequent non-cyclic access still works", () => {
    const registry = new SlotRegistry()
    // Set up a cycle between commandBus and queryBus
    registry.forceSet("commandBus", ({ queryBus }) => queryBus as unknown as CommandBus)
    registry.forceSet("queryBus", ({ commandBus }) => commandBus as unknown as QueryBus)
    // Also register serializer (non-cyclic)
    registry.forceSet("serializer", (_resolved) => stubSerializer)
    const resolved = buildResolved(registry)
    // Trigger cycle
    try { resolved.commandBus } catch { /* expected */ }
    // Now access a non-cyclic slot — should work fine
    expect(resolved.serializer).toBe(stubSerializer)
  })
})
