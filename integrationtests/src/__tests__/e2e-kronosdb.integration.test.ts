/**
 * Full-stack E2E integration test for KronosDB.
 *
 * Spins up ghcr.io/kronosdb/kronosdb:latest via testcontainers — no local
 * server needed. The image's entrypoint runs kronosdb-server, which listens
 * for gRPC on 50051 and admin on 9240.
 *
 * Tests the full CQRS/ES pipeline:
 *   command → event store → tracking processor → projection → query
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  jsonSerializer,
  command,
  event,
  query,
  commandHandler,
  eventHandler,
  queryHandler,
  EventCriteria,
  trackingProcessor,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import {
  type EventStore,
} from "@kronos-ts/eventsourcing"
import { kronos, inMemoryComponents, module, type App } from "@kronos-ts/app"
import { kronosDb, type KronosDbBackend } from "@kronos-ts/kronosdb"

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

const CloseEnrollment = command({
  name: qn("kronosdb-e2e", "CloseEnrollment"),
  payload: z.object({ courseId: z.string() }),
  routingKey: "courseId",
})

const EnrollmentClosed = event({
  name: qn("kronosdb-e2e", "EnrollmentClosed"),
  payload: z.object({ courseId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[]; closed: boolean }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0, enrolled: [], closed: false }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: (on) => [
    on(CourseCreated, (s, { payload: e }) => ({ ...s, created: true, name: e.name, capacity: e.capacity })),
    on(StudentSubscribed, (s, { payload: e }) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })),
    on(EnrollmentClosed, (s) => ({ ...s, closed: true })),
  ],
})

const createCourse = commandHandler(CreateCourse, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  ctx.append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
  ctx.append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// -- Stateful automation: an event handler that reacts to StudentSubscribed,
// sources the affected Course, and — if it is now full — issues a
// CloseEnrollment command via ctx.send(). The command runs in its own fresh
// UnitOfWork per the AF5-aligned model.

const closeEnrollment = commandHandler(CloseEnrollment, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created || course.closed) return
  ctx.append(EnrollmentClosed, { courseId: cmd.courseId })
})

const closeEnrollmentWhenFull = eventHandler(StudentSubscribed, async ({ payload: e }, ctx) => {
  const course = await ctx.load(Course, { courseId: e.courseId })
  if (course.created && !course.closed && course.enrolled.length >= course.capacity) {
    await ctx.send(CloseEnrollment, { courseId: e.courseId })
  }
})

// -- Projection --
type CourseView = { courseId: string; name: string; capacity: number; enrolledCount: number }
const courseViews = new Map<string, CourseView>()

const onCourseCreated = eventHandler(CourseCreated, async ({ payload: e }, ctx) => {
  courseViews.set(e.courseId, { courseId: e.courseId, name: e.name, capacity: e.capacity, enrolledCount: 0 })
  ctx.emitUpdate(GetCourse, (q) => q.courseId === e.courseId, courseViews.get(e.courseId))
})

const onStudentSubscribed = eventHandler(StudentSubscribed, async ({ payload: e }, ctx) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.enrolledCount++
    ctx.emitUpdate(GetCourse, (q) => q.courseId === e.courseId, view)
  }
})

const getCourse = queryHandler(GetCourse, async ({ payload: q }) => {
  const view = courseViews.get(q.courseId)
  if (!view) throw new Error("Course not found")
  return view
})

// ============================================================================
// Helpers
// ============================================================================

async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 30000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (await check()) return
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
  let container: StartedTestContainer
  let app: App
  let backend: KronosDbBackend
  let kronosHost: string
  let kronosPort: number

  beforeAll(async () => {
    courseViews.clear()

    // Spin up KronosDB. The server logs "KronosDB starting" once it binds
    // its gRPC listener; testcontainers also waits for port 50051 to accept
    // connections before returning.
    container = await new GenericContainer("ghcr.io/kronosdb/kronosdb:latest")
      .withExposedPorts(50051, 9240)
      .withWaitStrategy(Wait.forLogMessage(/KronosDB starting/))
      .start()

    kronosHost = container.getHost()
    kronosPort = container.getMappedPort(50051)

    // The projection processor is named once and used twice: it is registered
    // in the module AND handed to the backend, whose platform control plane may
    // pause / start / split / merge it. The container used to read this back
    // out of itself via app.processors(); now it is an ordinary argument.
    const courseProjection = trackingProcessor("kronosdb-course-projection")
      .eventHandlers(onCourseCreated, onStudentSubscribed)
      .build()

    // The app's serializer + UoW runner must be the SAME instances the
    // distributed buses use, so they are built first and passed in.
    const base = inMemoryComponents()

    backend = await kronosDb({
      componentName: "kronosdb-e2e-test",
      host: kronosHost,
      port: kronosPort,
      context: "default",
      serializer: jsonSerializer(),
      unitOfWorkFactory: base.unitOfWorkFactory,
    })

    app = kronos({
      components: { ...base, ...backend.components },
      modules: [
        module(
          "kronosdb-e2e",
          Course,
          createCourse, subscribeStudent,
          getCourse,
          courseProjection,
        ),
      ],
    })

    // Wait until KronosDB has acked this client's registration — the handler
    // subscribe frames are already on the wire by now.
    await backend.start()
    // Belt-and-braces: the legacy wait for KronosDB to process subscriptions.
    await new Promise(r => setTimeout(r, 2000))
  }, 120_000)

  afterAll(async () => {
    await app?.stop()
    await backend?.close()
    await container?.stop()
  })

  function eventStore(): EventStore {
    return backend.components.eventStore
  }

  it("command persists events to KronosDB event store", async () => {
    const courseId = id("cs-101")

    await app.commandGateway.send(CreateCourse, {
      courseId,
      name: "Full Stack Course",
      capacity: 30,
    })

    const { events } = await eventStore().source({
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

    const { events } = await eventStore().source({
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

    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", courseId)),
    })
    expect(events.length).toBe(2) // CourseCreated + StudentSubscribed
  }, 30_000)

  it("multiple aggregates sourced independently", async () => {
    const courseA = id("cs-a")
    const courseB = id("cs-b")

    await app.commandGateway.send(CreateCourse, { courseId: courseA, name: "Course A", capacity: 10 })
    await app.commandGateway.send(CreateCourse, { courseId: courseB, name: "Course B", capacity: 20 })

    const eventsA = await eventStore().source({ criteria: EventCriteria.havingTags(tag("courseId", courseA)) })
    const eventsB = await eventStore().source({ criteria: EventCriteria.havingTags(tag("courseId", courseB)) })

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

  it("stateful automation — an event handler sends a command in its own UoW", async () => {
    // A dedicated app isolates the automation processor so it cannot perturb
    // the event counts asserted by the tests above. It connects to the same
    // KronosDB instance.
    const automation = trackingProcessor("kronosdb-enrollment-automation")
      .eventHandlers(closeEnrollmentWhenFull)
      .build()
    const autoBase = inMemoryComponents()
    const autoBackend = await kronosDb({
      componentName: "kronosdb-automation-test",
      host: kronosHost,
      port: kronosPort,
      context: "default",
      serializer: jsonSerializer(),
      unitOfWorkFactory: autoBase.unitOfWorkFactory,
    })
    const autoEventStore: EventStore = autoBackend.components.eventStore
    const autoApp = kronos({
      components: { ...autoBase, ...autoBackend.components },
      modules: [
        module(
          "kronosdb-e2e-automation",
          Course,
          createCourse, subscribeStudent, closeEnrollment,
          automation,
        ),
      ],
    })
    await autoBackend.start()
    await new Promise(r => setTimeout(r, 2000))

    try {
      const courseId = id("auto-cap")
      await autoApp.commandGateway.send(CreateCourse, { courseId, name: "One Seat", capacity: 1 })
      await autoApp.commandGateway.send(SubscribeStudent, { courseId, studentId: "stu-1" })

      // The automation sources the now-full course and dispatches
      // CloseEnrollment; its handler appends EnrollmentClosed in its own UoW.
      const criteria = EventCriteria.havingTags(tag("courseId", courseId))
      await waitFor(async () => {
        const { events } = await autoEventStore.source({ criteria })
        return events.some((ev) => ev.name.name === "EnrollmentClosed")
      }, 30000)

      const { events } = await autoEventStore.source({ criteria })
      expect(events.map((ev) => ev.name.name)).toEqual([
        "CourseCreated",
        "StudentSubscribed",
        "EnrollmentClosed",
      ])
    } finally {
      await autoApp.stop()
      await autoBackend.close()
    }
  }, 60_000)
})
