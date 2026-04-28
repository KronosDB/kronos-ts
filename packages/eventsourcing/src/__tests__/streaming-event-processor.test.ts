import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn, tag, type Metadata } from "@kronos-ts/common"
import {
  command,
  event,
  on,
  commandHandler,
  eventHandlers,
  EventCriteria,
  createInMemoryTokenStore,
  globalSequenceToken,
  createStreamingEventProcessor,
  type StreamableEventSource,
  isReplay,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { createInMemoryEventStore } from "../in-memory-event-store.js"
import { load, append } from "../index.js"

// -- Domain --

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

type CourseState = { created: boolean; name: string }

const CourseEntity = eventSourcedEntity({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "" }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: [
    on(CourseCreated, (s: CourseState, e) => ({ created: true, name: e.name })),
  ],
})

const createCourse = commandHandler(CreateCourse, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name })
})

async function waitFor(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error("Timed out")
}

describe("StreamingEventProcessor", () => {
  describe("push-based streaming", () => {
    it("receives events via push notification from InMemoryEventStore", async () => {
      const eventStore = createInMemoryEventStore()
      const processed: string[] = []

      const projection = eventHandlers({
        name: "course-projection",
        handlers: [
          on(CourseCreated, async (e) => { processed.push(e.courseId) }),
        ],
      })

      // Create processor directly using the streaming API
      const processor = createStreamingEventProcessor({
        name: "course-projection",
        eventSource: eventStore as StreamableEventSource,
        handlerGroups: [projection],
        contextFactory: (metadata: Metadata) => ({
          load: async () => { throw new Error("not needed") },
          send: async () => {},
          emitUpdate: () => {},
          metadata,
        }),
      })

      await processor.start()

      // Append events directly to the store
      await eventStore.append([{
        identifier: "e1",
        name: qn("university", "CourseCreated"),
        version: "1.0",
        payload: { courseId: "cs-101", name: "Intro" },
        metadata: {},
        timestamp: Date.now(),
        tags: [tag("courseId", "cs-101")],
      }])

      await waitFor(() => processed.length >= 1)
      expect(processed).toContain("cs-101")

      // Second event — should also be pushed
      await eventStore.append([{
        identifier: "e2",
        name: qn("university", "CourseCreated"),
        version: "1.0",
        payload: { courseId: "cs-201", name: "Advanced" },
        metadata: {},
        timestamp: Date.now(),
        tags: [tag("courseId", "cs-201")],
      }])

      await waitFor(() => processed.length >= 2)
      expect(processed).toContain("cs-201")

      processor.stop()
    })
  })

  describe("replay support", () => {
    it("replays events and detects replay state in handlers", async () => {
      const eventStore = createInMemoryEventStore()
      const tokenStore = createInMemoryTokenStore()
      const processed: Array<{ courseId: string; replayed: boolean }> = []

      // Store some events first
      await eventStore.append([{
        identifier: "e1",
        name: qn("university", "CourseCreated"),
        version: "1.0",
        payload: { courseId: "cs-101", name: "Intro" },
        metadata: {},
        timestamp: Date.now(),
        tags: [tag("courseId", "cs-101")],
      }])
      await eventStore.append([{
        identifier: "e2",
        name: qn("university", "CourseCreated"),
        version: "1.0",
        payload: { courseId: "cs-201", name: "Advanced" },
        metadata: {},
        timestamp: Date.now(),
        tags: [tag("courseId", "cs-201")],
      }])

      // Mark processor as having processed up to position 2
      await tokenStore.store("course-projection", 0, globalSequenceToken(2n))

      const projection = eventHandlers({
        name: "course-projection",
        handlers: [
          on(CourseCreated, async (e) => {
            processed.push({
              courseId: e.courseId,
              replayed: isReplay(),
            })
          }),
        ],
      })

      const processor = createStreamingEventProcessor({
        name: "course-projection",
        eventSource: eventStore as StreamableEventSource,
        handlerGroups: [projection],
        contextFactory: (metadata: Metadata) => ({
          load: async () => { throw new Error("not needed") },
          send: async () => {},
          emitUpdate: () => {},
          metadata,
        }),
        tokenStore,
      })

      // Reset to replay from beginning
      await processor.resetTokens(0n)

      // Start processing
      await processor.start()
      await waitFor(() => processed.length >= 2)

      // Both events should be marked as replayed
      expect(processed.find(p => p.courseId === "cs-101")?.replayed).toBe(true)
      expect(processed.find(p => p.courseId === "cs-201")?.replayed).toBe(true)

      // After replay, processor should not be replaying
      await waitFor(() => !processor.replaying, 2000)

      processor.stop()
    })

    it("calls onReset handlers when resetting", async () => {
      const eventStore = createInMemoryEventStore()
      let resetCalled = false

      const projection = eventHandlers({
        name: "resettable",
        handlers: [on(CourseCreated, async () => {})],
        onReset: async () => { resetCalled = true },
      })

      const processor = createStreamingEventProcessor({
        name: "resettable",
        eventSource: eventStore as StreamableEventSource,
        handlerGroups: [projection],
        contextFactory: (metadata: Metadata) => ({
          load: async () => { throw new Error("not needed") },
          send: async () => {},
          emitUpdate: () => {},
          metadata,
        }),
      })

      await processor.resetTokens()

      expect(resetCalled).toBe(true)
    })

    it("throws if reset called while running", async () => {
      const eventStore = createInMemoryEventStore()

      const processor = createStreamingEventProcessor({
        name: "test",
        eventSource: eventStore as StreamableEventSource,
        handlerGroups: [],
        contextFactory: (metadata: Metadata) => ({
          load: async () => { throw new Error("not needed") },
          send: async () => {},
          emitUpdate: () => {},
          metadata,
        }),
      })

      await processor.start()

      expect(processor.resetTokens()).rejects.toThrow("must be stopped")

      processor.stop()
    })
  })
})
