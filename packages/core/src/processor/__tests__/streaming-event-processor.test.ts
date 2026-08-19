import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn } from "../../primitives/qualified-name.js"
import { tag } from "../../primitives/tag.js"
import { command, event } from "../../messages/descriptor.js"
import { commandHandler } from "../../handlers/command-handler.js"
import { eventHandler } from "../../handlers/event-handler.js"
import { inMemoryTokenStore } from "../../stores/token-store.js"
import { globalSequenceToken } from "../tracking-token.js"
import { eventProcessor } from "../event-processor.js"
import { runEventProcessor } from "../running-processor.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { state } from "../../state/state.js"
import { inMemoryEventStore } from "../../stores/in-memory-event-store.js"

// -- Domain --

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

type CourseState = { created: boolean; name: string }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "" }) as CourseState,
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    [CourseCreated, (s, { payload: e }) => ({ created: true, name: e.name })],
  ],
})

const createCourse = commandHandler(CreateCourse, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  ctx.append(CourseCreated, { courseId: cmd.courseId, name: cmd.name })
})

async function waitFor(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error("Timed out")
}

/**
 * One delivery over a REAL in-memory event store — the push path. The store
 * wakes the stream callback on append, so this exercises the notification
 * route that a synthetic source cannot.
 */
function runOver(config: {
  name: string
  eventStore: ReturnType<typeof inMemoryEventStore>
  eventHandlers: ReadonlyArray<Parameters<typeof runEventProcessor>[0]["handlers"][number]["definition"]>
  tokenStore?: ReturnType<typeof inMemoryTokenStore>
}) {
  return runEventProcessor({
    processor: eventProcessor({
      name: config.name,
      eventStore: config.eventStore,
      tokenStore: config.tokenStore ?? inMemoryTokenStore(),
      unitOfWork,
    }),
    handlers: config.eventHandlers.map((definition) => ({ definition })),
  })
}

describe("event processor over a live event store", () => {
  describe("push-based streaming", () => {
    it("receives events via push notification from InMemoryEventStore", async () => {
      const eventStore = inMemoryEventStore()
      const processed: string[] = []

      const onCourseCreated = eventHandler(CourseCreated, async ({ payload: e }) => {
        processed.push(e.courseId)
      })

      // Create processor directly using the streaming API
      const processor = runOver({
        name: "course-projection",
        eventStore,
        eventHandlers: [onCourseCreated],
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
      const eventStore = inMemoryEventStore()
      const tokenStore = inMemoryTokenStore()
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

      const onCourseCreated = eventHandler(CourseCreated, async ({ payload: e }, ctx) => {
        processed.push({
          courseId: e.courseId,
          replayed: ctx.isReplay(),
        })
      })

      const processor = runOver({
        name: "course-projection",
        eventStore,
        eventHandlers: [onCourseCreated],
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

    it("throws if reset called while running", async () => {
      const eventStore = inMemoryEventStore()

      const processor = runOver({
        name: "test",
        eventStore,
        eventHandlers: [],
      })

      await processor.start()

      expect(processor.resetTokens()).rejects.toThrow("must be stopped")

      processor.stop()
    })
  })
})
