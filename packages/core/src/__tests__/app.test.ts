import { describe, it, expect, afterEach, beforeEach } from "bun:test"
import { AppImpl, AppAlreadyStartedError } from "../app.js"
import { registerInMemoryDefaults } from "../defaults.js"
import { createWarningChannel } from "../warnings.js"
import type { CommandBus } from "@kronos-ts/messaging"

function makeApp() {
  return new AppImpl({ warningChannel: createWarningChannel() })
}

describe("AppImpl — fluent API", () => {
  it("fluent methods return the same App instance", () => {
    const app = makeApp()
    const dummyEntity = {} as any
    const dummyHandler = {} as any
    const dummyQueryHandler = {} as any
    const dummyProcessor = {} as any

    expect(app.entities(dummyEntity)).toBe(app)
    expect(app.commands(dummyHandler)).toBe(app)
    expect(app.queries(dummyQueryHandler)).toBe(app)
    expect(app.processors(dummyProcessor)).toBe(app)
    expect(app.use(() => {})).toBe(app)
  })

  it(".use(fn) buffers extensions — does NOT invoke fn at registration time", () => {
    const app = makeApp()
    let callCount = 0
    const spy = () => { callCount++ }

    app.use(spy)
    expect(callCount).toBe(0)
  })

  it("throws AppAlreadyStartedError after _started is true", () => {
    const app = makeApp()
    // Manually mark started to simulate post-start state
    app.markStarted()

    expect(() => app.entities({} as any)).toThrow(AppAlreadyStartedError)
    expect(() => app.commands({} as any)).toThrow(AppAlreadyStartedError)
    expect(() => app.queries({} as any)).toThrow(AppAlreadyStartedError)
    expect(() => app.processors({} as any)).toThrow(AppAlreadyStartedError)
    expect(() => app.use(() => {})).toThrow(AppAlreadyStartedError)
    expect(() => app.set("serializer", {} as any)).toThrow(AppAlreadyStartedError)
    expect(() => app.setDefault("serializer", {} as any)).toThrow(AppAlreadyStartedError)
    expect(() => app.forceSet("serializer", {} as any)).toThrow(AppAlreadyStartedError)
  })

  it("registerInMemoryDefaults flags eventStore/snapshotStore/commandBus/queryBus/eventBus as inMemory", () => {
    const app = makeApp()
    registerInMemoryDefaults(app)

    const registry = app.getRegistry()
    expect(registry.getEntry("eventStore")?.meta?.inMemory).toBe(true)
    expect(registry.getEntry("snapshotStore")?.meta?.inMemory).toBe(true)
    expect(registry.getEntry("commandBus")?.meta?.inMemory).toBe(true)
    expect(registry.getEntry("queryBus")?.meta?.inMemory).toBe(true)
    expect(registry.getEntry("eventBus")?.meta?.inMemory).toBe(true)

    // These three do NOT have inMemory flag
    expect(registry.getEntry("serializer")?.meta?.inMemory).toBeUndefined()
    expect(registry.getEntry("unitOfWorkFactory")?.meta?.inMemory).toBeUndefined()
    expect(registry.getEntry("tagResolver")?.meta?.inMemory).toBeUndefined()
  })

  it("app.set() on an already-defaulted slot emits console.warn; forceSet() does NOT", () => {
    let warnCalled = false
    const originalWarn = console.warn
    console.warn = () => { warnCalled = true }

    try {
      const app = makeApp()
      registerInMemoryDefaults(app)

      const stubBus = {} as unknown as CommandBus
      app.set("commandBus", () => stubBus)
      expect(warnCalled).toBe(true)

      warnCalled = false
      app.forceSet("commandBus", () => stubBus)
      expect(warnCalled).toBe(false)
    } finally {
      console.warn = originalWarn
    }
  })

  it("app.setDefault() on an already-defaulted slot is a no-op (ifAbsent semantics)", () => {
    const app = makeApp()
    registerInMemoryDefaults(app)

    const registry = app.getRegistry()
    const originalEntry = registry.getEntry("commandBus")

    // A second setDefault should NOT replace the existing default (ifAbsent semantics)
    const stubBus = {} as unknown as CommandBus
    app.setDefault("commandBus", () => stubBus)

    // The original entry remains unchanged
    expect(registry.getEntry("commandBus")).toBe(originalEntry)
  })
})
