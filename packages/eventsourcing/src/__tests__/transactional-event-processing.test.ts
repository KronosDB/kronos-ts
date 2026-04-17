import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn, tag, ComponentKeys } from "@kronos-ts/common"
import {
  command,
  event,
  on,
  commandHandler,
  eventHandlers,
  EventCriteria,
  createInMemoryTokenStore,
  globalSequenceToken,
  type TransactionManager,
  trackingProcessor,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { EventSourcingConfigurer } from "../eventsourcing-configurer.js"

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

const createCourse = commandHandler(CreateCourse, async (cmd, { load, append }) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name })
})

// -- Helpers --

async function waitFor(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise((r) => setTimeout(r, 50))
  }
  throw new Error("Timed out")
}

// ============================================================================
// Tests
// ============================================================================

describe("Transactional event processing", () => {
  let app: any

  afterEach(async () => {
    await app?.stop()
  })

  it("persists token position via TokenStore", async () => {
    const tokenStore = createInMemoryTokenStore()
    const courseViews = new Map<string, any>()

    const courseProjection = eventHandlers({
      name: "course-projection",
      handlers: [
        on(CourseCreated, async (e) => {
          courseViews.set(e.courseId, { courseId: e.courseId, name: e.name })
        }),
      ],
    })

    app = EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createCourse)
        m.registerEventProcessor(config =>
          trackingProcessor("course-projection")
            .registerEventHandler(courseProjection)
            .build()
        )
      })
      .componentRegistry((registry) => {
        registry.register(ComponentKeys.TOKEN_STORE, () => tokenStore)
      })
      .build()

    await app.start()

    // when
    await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro" })

    // then — wait for projection to process
    await waitFor(() => courseViews.has("cs-101"))

    // Token should be stored
    const token = await tokenStore.get("course-projection", 0)
    expect(token).toBeDefined()
    expect(token!.position()).toBeGreaterThan(0n)
  })

  it("resumes from stored token position on restart", async () => {
    const tokenStore = createInMemoryTokenStore()
    const processedEvents: string[] = []

    // Pre-store a token at position 1 — processor should skip event at position 0
    await tokenStore.store("course-projection", 0, globalSequenceToken(1n))

    const courseProjection = eventHandlers({
      name: "course-projection",
      handlers: [
        on(CourseCreated, async (e) => {
          processedEvents.push(e.courseId)
        }),
      ],
    })

    app = EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createCourse)
        m.registerEventProcessor(config =>
          trackingProcessor("course-projection")
            .registerEventHandler(courseProjection)
            .build()
        )
      })
      .componentRegistry((registry) => {
        registry.register(ComponentKeys.TOKEN_STORE, () => tokenStore)
      })
      .build()

    await app.start()

    // Send two commands — first event at position 0, second at position 1
    await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro" })
    await app.commandGateway.send(CreateCourse, { courseId: "cs-201", name: "Advanced" })

    // Wait for processing
    await waitFor(() => processedEvents.length >= 1, 3000)
    // Give a bit more time to make sure cs-101 is NOT processed
    await new Promise((r) => setTimeout(r, 200))

    // cs-101 should be skipped (at position 0, token was at 1)
    // cs-201 should be processed (at position 1, matching token)
    expect(processedEvents).not.toContain("cs-101")
    expect(processedEvents).toContain("cs-201")
  })

  it("wraps event processing in transaction when TransactionManager is configured", async () => {
    const txLog: string[] = []
    const txManager: TransactionManager<string> = {
      begin: async () => { txLog.push("begin"); return "tx" },
      commit: async () => { txLog.push("commit") },
      rollback: async () => { txLog.push("rollback") },
    }

    const courseViews = new Map<string, any>()
    const courseProjection = eventHandlers({
      name: "course-projection",
      handlers: [
        on(CourseCreated, async (e) => {
          courseViews.set(e.courseId, { courseId: e.courseId, name: e.name })
        }),
      ],
    })

    app = EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createCourse)
        m.registerEventProcessor(config =>
          trackingProcessor("course-projection")
            .registerEventHandler(courseProjection)
            .build()
        )
      })
      .componentRegistry((registry) => {
        registry.register(ComponentKeys.TRANSACTION_MANAGER, () => txManager)
      })
      .build()

    await app.start()

    // when
    await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro" })

    // then — wait for projection
    await waitFor(() => courseViews.has("cs-101"))

    // Transaction should have been used for both command handling and event processing
    // Command dispatch: begin → handler → commit
    // Event processing: begin → handler → commit
    expect(txLog.filter(e => e === "begin").length).toBeGreaterThanOrEqual(2)
    expect(txLog.filter(e => e === "commit").length).toBeGreaterThanOrEqual(2)
    expect(txLog).not.toContain("rollback")
  })
})
