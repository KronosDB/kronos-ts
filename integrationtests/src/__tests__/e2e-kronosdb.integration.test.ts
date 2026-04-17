/**
 * Full-stack E2E integration test for KronosDB.
 *
 * Connects to a local KronosDB instance at localhost:50051.
 * Start KronosDB before running: cargo run --release --package kronosdb-server
 *
 * Tests the full CQRS/ES pipeline:
 *   command → event store → tracking processor → projection → query
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  query,
  on,
  commandHandler,
  eventHandlers,
  queryHandlers,
  EventCriteria,
  trackingProcessor,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { EventSourcingConfigurer } from "@kronos-ts/eventsourcing"
import { kronosDbConfigurationEnhancer } from "@kronos-ts/kronosdb"

// ============================================================================
// Domain — same university model as the Axon Server E2E test
// ============================================================================

const CreateCourse = command({
  name: qn("kronosdb-e2e", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  routingKey: "courseId",
})

const SubscribeStudent = command({
  name: qn("kronosdb-e2e", "SubscribeStudent"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  routingKey: "courseId",
})

const GetCourse = query({
  name: qn("kronosdb-e2e", "GetCourse"),
  payload: z.object({ courseId: z.string() }),
})

const CourseCreated = event({
  name: qn("kronosdb-e2e", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const StudentSubscribed = event({
  name: qn("kronosdb-e2e", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[] }

const CourseEntity = eventSourcedEntity({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0, enrolled: [] }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: [
    on(CourseCreated, (s: CourseState, e) => ({ ...s, created: true, name: e.name, capacity: e.capacity })),
    on(StudentSubscribed, (s: CourseState, e) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })),
  ],
})

const createCourse = commandHandler(CreateCourse, async (cmd, { load, append }) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async (cmd, { load, append }) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
  append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// -- Projection --
type CourseView = { courseId: string; name: string; capacity: number; enrolledCount: number }
const courseViews = new Map<string, CourseView>()

const courseProjection = eventHandlers({
  name: "course-projection",
  handlers: [
    on(CourseCreated, async (e, ctx) => {
      courseViews.set(e.courseId, { courseId: e.courseId, name: e.name, capacity: e.capacity, enrolledCount: 0 })
      ctx.emitUpdate(GetCourse, (q) => q.courseId === e.courseId, courseViews.get(e.courseId))
    }),
    on(StudentSubscribed, async (e, ctx) => {
      const view = courseViews.get(e.courseId)
      if (view) {
        view.enrolledCount++
        ctx.emitUpdate(GetCourse, (q) => q.courseId === e.courseId, view)
      }
    }),
  ],
})

const courseQueries = queryHandlers({
  name: "course-queries",
  handlers: [
    on(GetCourse, async (q) => {
      const view = courseViews.get(q.courseId)
      if (!view) throw new Error("Course not found")
      return view
    }),
  ],
})

// ============================================================================
// Helpers
// ============================================================================

async function waitFor(check: () => boolean, timeoutMs = 30000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise(r => setTimeout(r, 100))
  }
  throw new Error("Timed out waiting for condition")
}

// Use unique IDs per test run to avoid conflicts with previous runs
const runId = Math.random().toString(36).slice(2, 8)
function id(name: string) { return `${name}-${runId}` }

// ============================================================================
// Tests
// ============================================================================

describe("E2E: KronosDB full stack", () => {
  let app: Awaited<ReturnType<typeof EventSourcingConfigurer.prototype.start>>

  beforeAll(async () => {
    courseViews.clear()

    app = await EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .registerCommandHandler(() => createCourse)
      .registerCommandHandler(() => subscribeStudent)
      .registerEventProcessor(config =>
        trackingProcessor("kronosdb-course-projection")
          .registerEventHandler(courseProjection)
          .build()
      )
      .registerQueryHandlers(() => courseQueries)
      .registerEnhancer(kronosDbConfigurationEnhancer({
        componentName: "kronosdb-e2e-test",
        host: "localhost",
        port: 50051,
        context: "default",
      }))
      .start()

    // Give KronosDB time to process handler subscriptions
    await new Promise(r => setTimeout(r, 2000))
  }, 30_000)

  afterAll(async () => {
    await app?.stop()
  })

  it("command persists events to KronosDB event store", async () => {
    const courseId = id("cs-101")

    await app.commandGateway.send(CreateCourse, {
      courseId,
      name: "Full Stack Course",
      capacity: 30,
    })

    const { events } = await app.eventStore.source({
      criteria: EventCriteria.havingTags(tag("courseId", courseId)),
    })
    expect(events.length).toBe(1)
    expect((events[0]!.payload as any).name).toBe("Full Stack Course")
  }, 30_000)

  it("command handler sources state from KronosDB", async () => {
    const courseId = id("cs-101")

    // Second command on same aggregate — state must be sourced from first event
    await app.commandGateway.send(SubscribeStudent, {
      courseId,
      studentId: "stu-1",
    })

    const { events } = await app.eventStore.source({
      criteria: EventCriteria.havingTags(tag("courseId", courseId)),
    })
    expect(events.length).toBe(2)
  }, 30_000)

  it("business rules enforced via event-sourced state", async () => {
    const courseId = id("cs-101")

    // Duplicate creation should fail
    await expect(
      app.commandGateway.send(CreateCourse, { courseId, name: "Duplicate", capacity: 5 }),
    ).rejects.toThrow()
  }, 30_000)

  it("capacity enforcement across multiple commands", async () => {
    const courseId = id("cs-cap")

    await app.commandGateway.send(CreateCourse, {
      courseId,
      name: "Small Class",
      capacity: 1,
    })

    await app.commandGateway.send(SubscribeStudent, {
      courseId,
      studentId: "stu-1",
    })

    // Course is full
    await expect(
      app.commandGateway.send(SubscribeStudent, { courseId, studentId: "stu-2" }),
    ).rejects.toThrow()

    const { events } = await app.eventStore.source({
      criteria: EventCriteria.havingTags(tag("courseId", courseId)),
    })
    expect(events.length).toBe(2) // CourseCreated + StudentSubscribed
  }, 30_000)

  it("multiple aggregates sourced independently", async () => {
    const courseA = id("cs-a")
    const courseB = id("cs-b")

    await app.commandGateway.send(CreateCourse, { courseId: courseA, name: "Course A", capacity: 10 })
    await app.commandGateway.send(CreateCourse, { courseId: courseB, name: "Course B", capacity: 20 })

    const eventsA = await app.eventStore.source({ criteria: EventCriteria.havingTags(tag("courseId", courseA)) })
    const eventsB = await app.eventStore.source({ criteria: EventCriteria.havingTags(tag("courseId", courseB)) })

    expect(eventsA.events.length).toBe(1)
    expect(eventsB.events.length).toBe(1)
    expect((eventsA.events[0]!.payload as any).name).toBe("Course A")
    expect((eventsB.events[0]!.payload as any).name).toBe("Course B")
  }, 30_000)

  it("tracking processor streams events to projection", async () => {
    const courseA = id("cs-a")

    await waitFor(() => courseViews.has(courseA), 30000)

    const view = courseViews.get(courseA)!
    expect(view.name).toBe("Course A")
    expect(view.capacity).toBe(10)
  }, 60_000)

  it("query gateway returns projected view", async () => {
    const courseA = id("cs-a")

    await waitFor(() => courseViews.has(courseA), 10000)

    const result = await app.queryGateway.query(GetCourse, { courseId: courseA })
    expect((result as CourseView).name).toBe("Course A")
  }, 30_000)
})
