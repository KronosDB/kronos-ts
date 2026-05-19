import { describe, it, expect } from "bun:test"
import { z } from "zod"
import { qn, tag, emptyMetadata } from "@kronos-ts/common"
import {
  command,
  event,
  on,
  commandHandler,
  EventCriteria,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { load, append } from "@kronos-ts/eventsourcing"
import { kronos } from "@kronos-ts/app"
import {
  testRecordingExtension,
  createRecordings,
  type Recordings,
} from "../recording-enhancer.js"

// ============================================================================
// Minimal domain inline — self-contained, does NOT depend on integrationtests
// ============================================================================

const DoTestThing = command({
  name: qn("rec-ext-test", "DoTestThing"),
  payload: z.object({ id: z.string() }),
})

const TestThingHappened = event({
  name: qn("rec-ext-test", "TestThingHappened"),
  payload: z.object({ id: z.string() }),
  tags: (p) => [tag("id", p.id)],
})

type ThingState = { exists: boolean }

const Thing = state({
  name: "RecExtThing",
  id: { id: z.string() },
  initial: (_id) => ({ exists: false }) as ThingState,
  criteria: (id) => EventCriteria.havingTags(tag("id", id.id)),
  evolve: [on(TestThingHappened, (s: ThingState) => ({ ...s, exists: true }))],
})

const doTestThingHandler = commandHandler(DoTestThing, async (cmd, _md) => {
  await load(Thing, { id: cmd.id })
  append(TestThingHappened, { id: cmd.id })
})

// ============================================================================
// Tests — Wave 0 RED prior to Task 2 implementation
// ============================================================================

describe("testRecordingExtension", () => {
  it("returns an Extension function", () => {
    const recordings: Recordings = createRecordings()
    const ext = testRecordingExtension(recordings)
    expect(typeof ext).toBe("function")
    expect(ext.length).toBe(1)
  })

  it("starts with empty recordings", async () => {
    const recordings = createRecordings()
    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(doTestThingHandler)
    app.use(testRecordingExtension(recordings))
    const running = await app.start()
    try {
      expect(recordings.events()).toHaveLength(0)
      expect(recordings.commands()).toHaveLength(0)
    } finally {
      await running.stop()
    }
  })

  it("records events appended via the event store after a command dispatch", async () => {
    const recordings = createRecordings()
    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(doTestThingHandler)
    app.use(testRecordingExtension(recordings))
    const running = await app.start()
    try {
      await running.commandGateway.send(DoTestThing, { id: "thing-1" }, emptyMetadata())
      const recorded = recordings.events()
      expect(recorded.length).toBeGreaterThanOrEqual(1)
      const evt = recorded[0]!
      expect(evt.name.name).toBe("TestThingHappened")
      expect((evt.payload as any).id).toBe("thing-1")
    } finally {
      await running.stop()
    }
  })

  it("records dispatched commands", async () => {
    const recordings = createRecordings()
    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(doTestThingHandler)
    app.use(testRecordingExtension(recordings))
    const running = await app.start()
    try {
      await running.commandGateway.send(DoTestThing, { id: "thing-2" }, emptyMetadata())
      const recordedCmds = recordings.commands()
      expect(recordedCmds.length).toBeGreaterThanOrEqual(1)
      const cmd = recordedCmds[recordedCmds.length - 1]!
      expect(cmd.name.name).toBe("DoTestThing")
      expect((cmd.payload as any).id).toBe("thing-2")
    } finally {
      await running.stop()
    }
  })

  it("reset() clears both events and commands", async () => {
    const recordings = createRecordings()
    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(doTestThingHandler)
    app.use(testRecordingExtension(recordings))
    const running = await app.start()
    try {
      await running.commandGateway.send(DoTestThing, { id: "thing-3" }, emptyMetadata())
      expect(recordings.events().length).toBeGreaterThan(0)
      expect(recordings.commands().length).toBeGreaterThan(0)

      recordings.reset()
      expect(recordings.events()).toHaveLength(0)
      expect(recordings.commands()).toHaveLength(0)

      // Subsequent activity records into a clean array
      await running.commandGateway.send(DoTestThing, { id: "thing-4" }, emptyMetadata())
      expect(recordings.events().length).toBeGreaterThan(0)
      expect(recordings.commands().length).toBeGreaterThan(0)
    } finally {
      await running.stop()
    }
  })

  it("recording decorators land at INNERMOST position (user decorator wraps recording wrapper)", async () => {
    const recordings = createRecordings()
    let observedInnerHasRecordingMarker = false

    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(doTestThingHandler)

    // Apply recording extension SYNCHRONOUSLY first so its decorators land in
    // `decoratorRegistrations` before the user-decorate call below. (Going via
    // `app.use` would defer execution to start(), placing user decorate FIRST
    // in the registration order.) This mirrors what the fixture does internally.
    testRecordingExtension(recordings)(app)

    // User decorator registered AFTER — should see the recording-wrapped store as `inner`.
    // The recording wrapper is identifiable because its `append` is NOT the in-memory base
    // (we cannot capture the exact base reference cleanly, but we can confirm that the
    // probe sees an `inner.append` whose call ALSO populates `recordings.events()` —
    // i.e., the user decorator's `inner` IS already a recording wrapper).
    app.decorate("eventStore", (inner) => {
      // Probe: snapshot the recordings length before/after delegating an append-equivalent.
      // We mark the test true if invoking inner.append populates recordings (= recording is innermost).
      const probeAppend = inner.append.bind(inner)
      return {
        ...inner,
        async append(events: any, condition?: any) {
          const before = recordings.events().length
          const r = await probeAppend(events, condition)
          const after = recordings.events().length
          if (after > before) observedInnerHasRecordingMarker = true
          return r
        },
      }
    })

    const running = await app.start()
    try {
      await running.commandGateway.send(DoTestThing, { id: "thing-inner" }, emptyMetadata())
      expect(observedInnerHasRecordingMarker).toBe(true)
    } finally {
      await running.stop()
    }
  })
})
