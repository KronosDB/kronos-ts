import { describe, it, expect, afterEach } from "bun:test"
import { SlotRegistry, type SlotFactory } from "../slot-registry.js"
import type { KronosComponents } from "../components.js"
import type { Serializer } from "@kronos-ts/common"
import type { CommandBus } from "@kronos-ts/messaging"

// Stub helpers to avoid importing real implementations in unit tests
const stubSerializer = { serialize: () => ({ type: "t", data: {}, revision: 0 }), deserialize: () => ({}) } as unknown as Serializer
const stubCommandBus = { dispatch: async () => void 0 } as unknown as CommandBus

describe("SlotRegistry — setDefault verb", () => {
  it("Test 1: setDefault on empty slot stores the entry; getEntry returns it", () => {
    const registry = new SlotRegistry()
    registry.setDefault("serializer", stubSerializer)
    const entry = registry.getEntry("serializer")
    expect(entry).toBeDefined()
    expect(typeof entry!.factory).toBe("function")
  })

  it("Test 2: setDefault on occupied slot is a no-op (entry unchanged)", () => {
    const registry = new SlotRegistry()
    const first = stubSerializer
    const second = { serialize: () => ({ type: "other", data: {}, revision: 0 }), deserialize: () => ({}) } as unknown as Serializer
    registry.setDefault("serializer", first)
    const entryBefore = registry.getEntry("serializer")
    registry.setDefault("serializer", second) // no-op
    const entryAfter = registry.getEntry("serializer")
    // The entry factory should still produce the first instance
    expect(entryAfter!.factory({} as KronosComponents)).toBe(first)
    expect(entryBefore!.factory).toBe(entryAfter!.factory)
  })
})

describe("SlotRegistry — set verb", () => {
  const warnCalls: string[] = []
  const originalWarn = console.warn

  afterEach(() => {
    console.warn = originalWarn
    warnCalls.length = 0
  })

  it("Test 3: set on empty slot stores entry, no warning", () => {
    console.warn = (msg: string) => warnCalls.push(msg)
    const registry = new SlotRegistry()
    registry.set("serializer", stubSerializer)
    expect(warnCalls).toHaveLength(0)
    expect(registry.getEntry("serializer")).toBeDefined()
  })

  it("Test 4: set on occupied slot stores new entry AND emits exactly one console.warn with slot name and 'override'", () => {
    console.warn = (msg: string) => warnCalls.push(msg)
    const registry = new SlotRegistry()
    registry.set("serializer", stubSerializer)
    const second = { serialize: () => ({ type: "other", data: {}, revision: 0 }), deserialize: () => ({}) } as unknown as Serializer
    registry.set("serializer", second)
    expect(warnCalls).toHaveLength(1)
    expect(warnCalls[0]).toContain("serializer")
    expect(warnCalls[0]).toContain("override")
    // The entry now has the new factory
    expect(registry.getEntry("serializer")!.factory({} as KronosComponents)).toBe(second)
  })
})

describe("SlotRegistry — forceSet verb", () => {
  const warnCalls: string[] = []
  const originalWarn = console.warn

  afterEach(() => {
    console.warn = originalWarn
    warnCalls.length = 0
  })

  it("Test 5: forceSet on occupied slot stores new entry, NO console.warn", () => {
    console.warn = (msg: string) => warnCalls.push(msg)
    const registry = new SlotRegistry()
    registry.set("serializer", stubSerializer)
    const second = { serialize: () => ({ type: "other", data: {}, revision: 0 }), deserialize: () => ({}) } as unknown as Serializer
    registry.forceSet("serializer", second)
    expect(warnCalls).toHaveLength(0)
    expect(registry.getEntry("serializer")!.factory({} as KronosComponents)).toBe(second)
  })
})

describe("SlotRegistry — factory normalization (SLT-03)", () => {
  it("Test 6: passing a plain instance wraps it as () => instance; subsequent calls return the same instance", () => {
    const registry = new SlotRegistry()
    registry.set("serializer", stubSerializer)
    const entry = registry.getEntry("serializer")!
    // factory is a function wrapping the instance
    expect(typeof entry.factory).toBe("function")
    // returns the same instance each time
    expect(entry.factory({} as KronosComponents)).toBe(stubSerializer)
    expect(entry.factory({} as KronosComponents)).toBe(stubSerializer)
  })

  it("Test 7: passing a function stores it as-is (typeof factoryOrInstance === 'function')", () => {
    const registry = new SlotRegistry()
    const factoryFn: SlotFactory<"serializer"> = (_resolved) => stubSerializer
    registry.set("serializer", factoryFn)
    const entry = registry.getEntry("serializer")!
    expect(entry.factory).toBe(factoryFn)
  })
})

describe("SlotRegistry — meta side table", () => {
  it("Test 8: setDefault with meta stores it on the entry; getEntry(slot).meta returns it", () => {
    const registry = new SlotRegistry()
    registry.setDefault("eventStore", {} as KronosComponents["eventStore"], {
      inMemory: true,
      warning: "not durable",
    })
    const entry = registry.getEntry("eventStore")
    expect(entry).toBeDefined()
    expect(entry!.meta).toBeDefined()
    expect(entry!.meta!.inMemory).toBe(true)
    expect(entry!.meta!.warning).toBe("not durable")
  })
})

describe("SlotRegistry — set verb provenance", () => {
  const warnCalls: string[] = []
  const originalWarn = console.warn

  afterEach(() => {
    console.warn = originalWarn
    warnCalls.length = 0
  })

  it("Test A: set() over a prior setDefault() WITH meta is SILENT (extension overriding an in-memory default)", () => {
    const registry = new SlotRegistry()
    registry.setDefault("eventStore", {} as KronosComponents["eventStore"], { inMemory: true, warning: "x" })
    console.warn = (msg: string) => warnCalls.push(msg)
    registry.set("eventStore", {} as KronosComponents["eventStore"])
    expect(warnCalls).toHaveLength(0)
  })

  it("Test B: set() over a prior setDefault() WITHOUT meta is SILENT (stateless default — meta key present but undefined)", () => {
    // serializer, unitOfWorkFactory, tagResolver are registered via setDefault with no meta arg.
    // setDefault writes { factory, meta } where meta is undefined — the `meta` KEY is present.
    // A guard of `"meta" in existing` correctly identifies this as a default override and stays silent.
    // The task-brief's `existing.meta === undefined` guard would incorrectly warn here.
    const registry = new SlotRegistry()
    registry.setDefault("serializer", stubSerializer) // no meta arg
    console.warn = (msg: string) => warnCalls.push(msg)
    registry.set("serializer", stubSerializer)
    expect(warnCalls).toHaveLength(0)
  })

  it("Test C: set() over a prior forceSet() WARNS (genuine collision — forceSet never writes meta key)", () => {
    const registry = new SlotRegistry()
    registry.forceSet("serializer", stubSerializer)
    console.warn = (msg: string) => warnCalls.push(msg)
    registry.set("serializer", stubSerializer)
    expect(warnCalls).toHaveLength(1)
    expect(warnCalls[0]).toContain("serializer")
    expect(warnCalls[0]).toContain("override")
  })
})
