import { describe, it, expect } from "bun:test"
import { ALL_SLOTS, type KronosComponents, type SlotName } from "../components.js"
import { CircularSlotDependencyError, SlotNotRegisteredError } from "../errors.js"

describe("KronosComponents interface", () => {
  it("ALL_SLOTS contains exactly 8 slot names", () => {
    expect(ALL_SLOTS).toHaveLength(8)
    expect(ALL_SLOTS).toContain("eventStore")
    expect(ALL_SLOTS).toContain("snapshotStore")
    expect(ALL_SLOTS).toContain("commandBus")
    expect(ALL_SLOTS).toContain("queryBus")
    expect(ALL_SLOTS).toContain("eventBus")
    expect(ALL_SLOTS).toContain("serializer")
    expect(ALL_SLOTS).toContain("unitOfWorkFactory")
    expect(ALL_SLOTS).toContain("tagResolver")
  })
})

describe("CircularSlotDependencyError", () => {
  it("extends Error, carries slot and chain, instanceof checks pass", () => {
    const err = new CircularSlotDependencyError("eventStore", ["commandBus", "eventStore"])
    expect(err instanceof Error).toBe(true)
    expect(err instanceof CircularSlotDependencyError).toBe(true)
    expect(err.slot).toBe("eventStore")
    expect(err.chain).toEqual(["commandBus", "eventStore"])
    expect(err.name).toBe("CircularSlotDependencyError")
    expect(err.message).toContain("eventStore")
  })
})

describe("SlotNotRegisteredError", () => {
  it("extends Error, carries slot property, instanceof checks pass", () => {
    const err = new SlotNotRegisteredError("serializer")
    expect(err instanceof Error).toBe(true)
    expect(err instanceof SlotNotRegisteredError).toBe(true)
    expect(err.slot).toBe("serializer")
    expect(err.name).toBe("SlotNotRegisteredError")
    expect(err.message).toContain("serializer")
  })
})
