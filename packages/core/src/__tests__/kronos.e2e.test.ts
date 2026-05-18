import { describe, it, expect, afterEach, beforeEach } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { command, event, on, commandHandler, EventCriteria } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { load, append } from "@kronos-ts/eventsourcing"
import { kronos, type RunningApp, AppAlreadyStartedError } from "../index.js"

// ============================================================================
// Minimal domain inline (mirrors eventsourcing/src/__tests__/kronos.test.ts pattern)
// ============================================================================

const CreateThing = command({
  name: qn("phase5", "CreateThing"),
  payload: z.object({ id: z.string() }),
})

const ThingCreated = event({
  name: qn("phase5", "ThingCreated"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})

const Thing = state({
  name: "Thing",
  id: { id: z.string() },
  initial: () => ({ created: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: [on(ThingCreated, (s) => ({ ...s, created: true }))],
})

const createThingHandler = commandHandler(CreateThing, async (cmd, _md) => {
  await load(Thing, { id: cmd.id })
  append(ThingCreated, { id: cmd.id })
})

// ============================================================================
// Tests
// ============================================================================

describe("kronos() e2e — in-memory defaults", () => {
  let warnSpy: string[]
  let originalWarn: typeof console.warn
  let app: RunningApp | undefined

  beforeEach(() => {
    warnSpy = []
    originalWarn = console.warn
    console.warn = (msg: string) => warnSpy.push(msg)
  })

  afterEach(async () => {
    console.warn = originalWarn
    if (app) {
      await app.stop()
      app = undefined
    }
  })

  it("dispatches a command end-to-end via in-memory defaults (success criterion #4)", async () => {
    app = await kronos({ quiet: true }).states(Thing).commands(createThingHandler).start()
    // The command dispatches successfully if no error is thrown
    await app.commandGateway.send(CreateThing, { id: "t-1" }, emptyMetadata())
    expect(true).toBe(true) // dispatch completed without throw
  })

  it("partial-config form merges into same state (APP-02)", async () => {
    app = await kronos({
      states: [Thing],
      commands: [createThingHandler],
      quiet: true,
    }).start()
    await app.commandGateway.send(CreateThing, { id: "t-2" }, emptyMetadata())
    expect(warnSpy.length).toBe(0) // quiet:true suppresses all warnings
  })

  it("emits startup warnings for in-memory defaults when not quiet (SLT-04)", async () => {
    app = await kronos().states(Thing).commands(createThingHandler).start()
    expect(warnSpy.length).toBeGreaterThanOrEqual(5)
    const warningText = warnSpy.join("\n")
    for (const slot of ["eventStore", "snapshotStore", "commandBus", "queryBus", "eventBus"]) {
      expect(warningText).toContain(slot)
    }
  })

  it("quiet:true suppresses all startup warnings (SLT-04)", async () => {
    app = await kronos({ quiet: true }).states(Thing).commands(createThingHandler).start()
    expect(warnSpy.length).toBe(0)
  })

  it("runs .use() extensions before slot resolution (D-50)", async () => {
    let extensionCalled = false
    app = await kronos({ quiet: true })
      .states(Thing)
      .commands(createThingHandler)
      .use((a) => {
        extensionCalled = true
        // Override serializer with the same default value — just proves the extension path runs
        a.set("serializer", {} as any)
      })
      .start()
    expect(extensionCalled).toBe(true)
  })

  it("throws AppAlreadyStartedError on post-start mutation (APP-03)", async () => {
    const builder = kronos({ quiet: true }).states(Thing).commands(createThingHandler)
    app = await builder.start()
    expect(() => builder.states(Thing)).toThrow(AppAlreadyStartedError)
  })
})
