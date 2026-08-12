import { describe, it, expect } from "bun:test"
import { z } from "zod"
import { qn, tag, emptyMetadata } from "@kronos-ts/common"
import { command, commandHandler, EventCriteria, event } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { kronos, inMemoryComponents, module } from "@kronos-ts/app"
import {
  recordings,
  recordingComponents,
  recordingEventStore,
  type Recordings,
} from "../recording.js"

// ============================================================================
// Minimal domain inline — self-contained
// ============================================================================

const DoTestThing = command({
  name: qn("rec-test", "DoTestThing"),
  payload: z.object({ id: z.string() }),
})

const TestThingHappened = event({
  name: qn("rec-test", "TestThingHappened"),
  payload: z.object({ id: z.string() }),
  tags: (p) => [tag("id", p.id)],
})

type ThingState = { exists: boolean }

const Thing = state({
  name: "RecThing",
  id: { id: z.string() },
  initial: (_id) => ({ exists: false }) as ThingState,
  criteria: (id) => EventCriteria.havingTags(tag("id", id.id)),
  evolve: (on) => [on(TestThingHappened, (s) => ({ ...s, exists: true }))],
})

const doTestThingHandler = commandHandler(DoTestThing, async ({ payload: cmd }, ctx) => {
  await ctx.load(Thing, { id: cmd.id })
  ctx.append(TestThingHappened, { id: cmd.id })
})

function bootWithRecording(recordings: Recordings) {
  return kronos({
    components: recordingComponents(inMemoryComponents(), recordings),
    modules: [module("rec-test", Thing, doTestThingHandler)],
  })
}

// ============================================================================
// Tests
// ============================================================================

describe("recording wrappers", () => {
  it("starts with empty recordings", async () => {
    const rec = recordings()
    const app = bootWithRecording(rec)
    try {
      expect(rec.events()).toHaveLength(0)
      expect(rec.commands()).toHaveLength(0)
    } finally {
      await app.stop()
    }
  })

  it("records events appended via the event store after a command dispatch", async () => {
    const rec = recordings()
    const app = bootWithRecording(rec)
    try {
      await app.commandGateway.send(DoTestThing, { id: "thing-1" }, emptyMetadata())
      const recorded = rec.events()
      expect(recorded.length).toBeGreaterThanOrEqual(1)
      const evt = recorded[0]!
      expect(evt.name.name).toBe("TestThingHappened")
      expect((evt.payload as any).id).toBe("thing-1")
    } finally {
      await app.stop()
    }
  })

  it("records dispatched commands", async () => {
    const rec = recordings()
    const app = bootWithRecording(rec)
    try {
      await app.commandGateway.send(DoTestThing, { id: "thing-2" }, emptyMetadata())
      const recordedCmds = rec.commands()
      expect(recordedCmds.length).toBeGreaterThanOrEqual(1)
      const cmd = recordedCmds[recordedCmds.length - 1]!
      expect(cmd.name.name).toBe("DoTestThing")
      expect((cmd.payload as any).id).toBe("thing-2")
    } finally {
      await app.stop()
    }
  })

  it("reset() clears both events and commands", async () => {
    const rec = recordings()
    const app = bootWithRecording(rec)
    try {
      await app.commandGateway.send(DoTestThing, { id: "thing-3" }, emptyMetadata())
      expect(rec.events().length).toBeGreaterThan(0)
      expect(rec.commands().length).toBeGreaterThan(0)

      rec.reset()
      expect(rec.events()).toHaveLength(0)
      expect(rec.commands()).toHaveLength(0)

      // Subsequent activity records into a clean array
      await app.commandGateway.send(DoTestThing, { id: "thing-4" }, emptyMetadata())
      expect(rec.events().length).toBeGreaterThan(0)
      expect(rec.commands().length).toBeGreaterThan(0)
    } finally {
      await app.stop()
    }
  })

  it("recording sits INNERMOST: a store wrapped around the recording store sees it record", async () => {
    const rec = recordings()
    let observedInnerRecords = false

    const base = inMemoryComponents()
    // Recording first (innermost), then the probe on top — wrapping order IS
    // the composition order; there is no registry deciding it for you.
    const recorded = recordingEventStore(base.eventStore, rec)
    const probed = {
      ...recorded,
      async append(events: any, condition?: any) {
        const before = rec.events().length
        const marker = await recorded.append(events, condition)
        if (rec.events().length > before) observedInnerRecords = true
        return marker
      },
    }

    const app = kronos({
      components: { ...base, eventStore: probed },
      modules: [module("rec-test", Thing, doTestThingHandler)],
    })
    try {
      await app.commandGateway.send(DoTestThing, { id: "thing-inner" }, emptyMetadata())
      expect(observedInnerRecords).toBe(true)
    } finally {
      await app.stop()
    }
  })

  it("rejects a recordings handle that did not come from recordings()", () => {
    const fake = { events: () => [], commands: () => [], reset: () => {} }
    expect(() => recordingEventStore(inMemoryComponents().eventStore, fake)).toThrow(
      /missing internal writers/,
    )
  })
})
