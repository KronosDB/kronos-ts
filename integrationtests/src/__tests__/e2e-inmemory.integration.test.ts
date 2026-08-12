/**
 * End-to-end integration test — full CQRS/ES flow with in-memory infrastructure.
 *
 * Validates the complete framework without external dependencies:
 * - Command dispatch → event sourcing → state management
 * - Tracking event processor → projection updates
 * - Query dispatch → read model
 * - Snapshots with per-entity policy
 * - Correlation data propagation
 * - Business rule enforcement
 *
 * Wired against the functional composition root: `kronos({ components,
 * modules })`. There is no container, so nothing has to be probed back out of
 * one — the event store is an ordinary value the test creates and hands to
 * `inMemoryComponents({ eventStore })`, then asserts against directly.
 */
import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  query,
  commandHandler,
  eventHandler,
  queryHandler,
  EventCriteria,
  trackingProcessor,
  subscribingProcessor,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import {
  type SnapshotStore,
  inMemoryEventStore,
  inMemorySnapshotStore,
  afterEvents,
} from "@kronos-ts/eventsourcing"
import { kronos, inMemoryComponents, module, type App } from "@kronos-ts/app"

// ============================================================================
// Domain: University Course Management
// ============================================================================

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  routingKey: "courseId",
})

const ChangeCourseCapacity = command({
  name: qn("university", "ChangeCourseCapacity"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  routingKey: "courseId",
})

const SubscribeStudent = command({
  name: qn("university", "SubscribeStudent"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  routingKey: "courseId",
})

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const StudentSubscribed = event({
  name: qn("university", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const CloseEnrollment = command({
  name: qn("university", "CloseEnrollment"),
  payload: z.object({ courseId: z.string() }),
})

const EnrollmentClosed = event({
  name: qn("university", "EnrollmentClosed"),
  payload: z.object({ courseId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const GetCourseView = query({
  name: qn("university", "GetCourseView"),
  payload: z.object({ courseId: z.string() }),
})

const GetAllCourses = query({
  name: qn("university", "GetAllCourses"),
  payload: z.object({}),
})

type CourseState = {
  created: boolean
  name: string
  capacity: number
  enrolled: string[]
  closed: boolean
}

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0, enrolled: [], closed: false }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: (on) => [
    on(CourseCreated, (s, { payload: e }) => ({
      ...s, created: true, name: e.name, capacity: e.capacity,
    })),
    on(CourseCapacityChanged, (s, { payload: e }) => ({
      ...s, capacity: e.capacity,
    })),
    on(StudentSubscribed, (s, { payload: e }) => ({
      ...s, enrolled: [...s.enrolled, e.studentId],
    })),
    on(EnrollmentClosed, (s) => ({ ...s, closed: true })),
  ],
})

// -- Command handlers --

const createCourse = commandHandler(CreateCourse, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  ctx.append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const changeCourseCapacity = commandHandler(ChangeCourseCapacity, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  ctx.append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
  ctx.append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// -- Stateful automation: close enrolment once a course is full --
//
// AF5-style stateful event handler: this handler reacts to StudentSubscribed,
// sources the very Course it affected, and — if the course is now at capacity —
// issues a CloseEnrollment command via ctx.send(). Per the AF5-aligned model that
// command is handled in its own fresh UnitOfWork, independent of the event
// processor's UnitOfWork that ran the automation.

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

// -- Projection (read model) --

type CourseView = { courseId: string; name: string; capacity: number; enrolledCount: number }

function createProjection() {
  const courseViews = new Map<string, CourseView>()

  const projectionHandlers = [
    eventHandler(CourseCreated, async ({ payload: e }, ctx) => {
      const view: CourseView = {
        courseId: e.courseId,
        name: e.name,
        capacity: e.capacity,
        enrolledCount: 0,
      }
      courseViews.set(e.courseId, view)
      ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
    }),
    eventHandler(CourseCapacityChanged, async ({ payload: e }, ctx) => {
      const view = courseViews.get(e.courseId)
      if (view) {
        view.capacity = e.capacity
        ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
      }
    }),
    eventHandler(StudentSubscribed, async ({ payload: e }, ctx) => {
      const view = courseViews.get(e.courseId)
      if (view) {
        view.enrolledCount++
        ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
      }
    }),
  ]

  const getCourseView = queryHandler(GetCourseView, async ({ payload: q }) => {
    const view = courseViews.get(q.courseId)
    if (!view) throw new Error("Course not found")
    return view
  })

  const getAllCourses = queryHandler(GetAllCourses, async () => {
    return [...courseViews.values()]
  })

  const queryHandlersList = [getCourseView, getAllCourses]

  return { projectionHandlers, queryHandlers: queryHandlersList, courseViews }
}

// ============================================================================
// Helpers
// ============================================================================

async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (await check()) return
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error("Timed out waiting for condition")
}

// ============================================================================
// Tests
// ============================================================================

describe("E2E: In-memory full CQRS flow", () => {
  let running: App | undefined

  afterEach(async () => {
    await running?.stop()
    running = undefined
  })

  it("command → event → processor → projection → query", async () => {
    // given
    const { projectionHandlers, queryHandlers, courseViews } = createProjection()

    running = kronos({
      components: inMemoryComponents(),
      modules: [
        module(
          "university",
          Course,
          createCourse, changeCourseCapacity, subscribeStudent,
          ...queryHandlers,
          trackingProcessor("course-projection")
            .eventHandlers(...projectionHandlers)
            .build(),
        ),
      ],
    })

    // when
    await running.commandGateway.send(CreateCourse, {
      courseId: "cs-101",
      name: "Intro to CS",
      capacity: 30,
    })

    await waitFor(() => courseViews.has("cs-101"))

    // then
    const view = await running.queryGateway.query(GetCourseView, { courseId: "cs-101" })
    expect(view).toBeDefined()
    expect((view as CourseView).name).toBe("Intro to CS")
    expect((view as CourseView).capacity).toBe(30)
  })

  it("enforces business rules across commands", async () => {
    // given
    const { projectionHandlers, queryHandlers } = createProjection()

    running = kronos({
      components: inMemoryComponents(),
      modules: [
        module(
          "university",
          Course,
          createCourse, subscribeStudent,
          ...queryHandlers,
          trackingProcessor("course-projection")
            .eventHandlers(...projectionHandlers)
            .build(),
        ),
      ],
    })

    // when
    await running.commandGateway.send(CreateCourse, {
      courseId: "small-101",
      name: "Small Course",
      capacity: 2,
    })

    await running.commandGateway.send(SubscribeStudent, { courseId: "small-101", studentId: "stu-1" })

    // then — duplicate enrollment (before capacity is full)
    await expect(
      running.commandGateway.send(SubscribeStudent, { courseId: "small-101", studentId: "stu-1" }),
    ).rejects.toThrow("Already enrolled")

    // fill the course
    await running.commandGateway.send(SubscribeStudent, { courseId: "small-101", studentId: "stu-2" })

    // then — course is full (capacity 2, 2 enrolled)
    await expect(
      running.commandGateway.send(SubscribeStudent, { courseId: "small-101", studentId: "stu-3" }),
    ).rejects.toThrow("Course is full")
  })

  // Per-STATE snapshot config (policy + its own store), declared as a
  // [state, options] tuple in the registration list.
  it("snapshots accelerate entity loading", async () => {
    // given
    const eventStore = inMemoryEventStore()
    const snapshotStore: SnapshotStore = inMemorySnapshotStore()
    const { projectionHandlers, queryHandlers } = createProjection()

    running = kronos({
      components: inMemoryComponents({ eventStore, snapshotStore }),
      modules: [
        module(
          "university",
          [Course, { snapshotPolicy: afterEvents(3), snapshotStore }],
          createCourse, changeCourseCapacity,
          ...queryHandlers,
          trackingProcessor("course-projection")
            .eventHandlers(...projectionHandlers)
            .build(),
        ),
      ],
    })

    // when — create + 4 capacity changes (5 events total)
    // Snapshot triggers after 3+ events are replayed during a load.
    await running.commandGateway.send(CreateCourse, { courseId: "snap-101", name: "Snap Course", capacity: 10 })
    await running.commandGateway.send(ChangeCourseCapacity, { courseId: "snap-101", capacity: 20 })
    await running.commandGateway.send(ChangeCourseCapacity, { courseId: "snap-101", capacity: 30 })
    await running.commandGateway.send(ChangeCourseCapacity, { courseId: "snap-101", capacity: 40 })
    // This load replays 4 events → triggers snapshot with capacity=40
    await running.commandGateway.send(ChangeCourseCapacity, { courseId: "snap-101", capacity: 50 })

    // Wait for async snapshot storage
    await new Promise(r => setTimeout(r, 50))

    // then — snapshot exists with state from when it was triggered
    const snapshot = await snapshotStore.load("Course", { courseId: "snap-101" })
    expect(snapshot).toBeDefined()
    expect((snapshot!.payload as CourseState).capacity).toBeGreaterThanOrEqual(30)
  })

  it("subscribing processor delivers events synchronously", async () => {
    // given
    const received: string[] = []
    const onCourseCreated = eventHandler(CourseCreated, async ({ payload: e }, ctx) => {
      received.push(e.courseId)
    })

    running = kronos({
      components: inMemoryComponents(),
      modules: [
        module(
          "university",
          Course,
          createCourse,
          subscribingProcessor("sync-projection")
            .eventHandlers(onCourseCreated)
            .build(),
        ),
      ],
    })

    // when — subscribing processor delivers synchronously with append
    await running.commandGateway.send(CreateCourse, { courseId: "sync-1", name: "Sync", capacity: 10 })

    // then — delivered immediately, no polling delay
    expect(received).toContain("sync-1")
  })

  it("multiple processors operate independently", async () => {
    // given
    const { projectionHandlers, queryHandlers, courseViews } = createProjection()
    const auditLog: string[] = []

    const auditOnCourseCreated = eventHandler(CourseCreated, async ({ payload: e }, ctx) => { auditLog.push(`created:${e.courseId}`) })
    const auditOnStudentSubscribed = eventHandler(StudentSubscribed, async ({ payload: e }, ctx) => { auditLog.push(`enrolled:${e.studentId}`) })

    running = kronos({
      components: inMemoryComponents(),
      modules: [
        module(
          "university",
          Course,
          createCourse, subscribeStudent,
          ...queryHandlers,
          trackingProcessor("course-projection")
            .eventHandlers(...projectionHandlers)
            .build(),
          trackingProcessor("audit-log")
            .eventHandlers(auditOnCourseCreated, auditOnStudentSubscribed)
            .build(),
        ),
      ],
    })

    // when
    await running.commandGateway.send(CreateCourse, { courseId: "multi-1", name: "Multi", capacity: 10 })
    await running.commandGateway.send(SubscribeStudent, { courseId: "multi-1", studentId: "stu-1" })

    await waitFor(() => courseViews.has("multi-1") && auditLog.length >= 2)

    // then — both processors received all events
    const view = courseViews.get("multi-1")!
    expect(view.enrolledCount).toBe(1)
    expect(auditLog).toContain("created:multi-1")
    expect(auditLog).toContain("enrolled:stu-1")
  })

  it("correlation data propagates through message chain", async () => {
    // given
    // Verify that events inherit the command's metadata (basic propagation
    // mechanism). Cross-message correlation is tested via the Axon Server
    // distributed tests.
    const eventStore = inMemoryEventStore()

    running = kronos({
      components: inMemoryComponents({ eventStore }),
      modules: [module("university", Course, createCourse)],
    })

    // when — dispatch a command with custom metadata
    const metadata = { tenantId: "t-1", userId: "u-42" }
    await running.commandGateway.send(CreateCourse, {
      courseId: "corr-1",
      name: "Correlation Test",
      capacity: 10,
    }, metadata)

    // then — events inherit the command's metadata
    const { events } = await eventStore.source({
      criteria: EventCriteria.havingTags(tag("courseId", "corr-1")),
    })

    expect(events.length).toBe(1)
    expect(events[0]!.metadata.tenantId).toBe("t-1")
    expect(events[0]!.metadata.userId).toBe("u-42")
  })

  it("query returns all courses", async () => {
    // given
    const { projectionHandlers, queryHandlers, courseViews } = createProjection()

    running = kronos({
      components: inMemoryComponents(),
      modules: [
        module(
          "university",
          Course,
          createCourse,
          ...queryHandlers,
          trackingProcessor("course-projection")
            .eventHandlers(...projectionHandlers)
            .build(),
        ),
      ],
    })

    // when
    await running.commandGateway.send(CreateCourse, { courseId: "all-1", name: "Course A", capacity: 10 })
    await running.commandGateway.send(CreateCourse, { courseId: "all-2", name: "Course B", capacity: 20 })

    await waitFor(() => courseViews.size >= 2)

    // then
    const allCourses = await running.queryGateway.query(GetAllCourses, {}) as CourseView[]
    expect(allCourses.length).toBeGreaterThanOrEqual(2)
    expect(allCourses.some(c => c.courseId === "all-1")).toBe(true)
    expect(allCourses.some(c => c.courseId === "all-2")).toBe(true)
  })

  it("stateful automation — an event handler sends a command in its own UoW", async () => {
    // given — a "close enrolment when full" automation on its own processor
    const eventStore = inMemoryEventStore()

    running = kronos({
      components: inMemoryComponents({ eventStore }),
      modules: [
        module(
          "university",
          Course,
          createCourse, subscribeStudent, closeEnrollment,
          trackingProcessor("enrollment-automation")
            .eventHandlers(closeEnrollmentWhenFull)
            .build(),
        ),
      ],
    })

    // when — a one-seat course is filled
    await running.commandGateway.send(CreateCourse, { courseId: "auto-1", name: "One Seat", capacity: 1 })
    await running.commandGateway.send(SubscribeStudent, { courseId: "auto-1", studentId: "stu-1" })

    // then — the automation sources the now-full course and dispatches
    // CloseEnrollment, whose handler appends EnrollmentClosed in its own UoW
    const criteria = EventCriteria.havingTags(tag("courseId", "auto-1"))
    await waitFor(async () => {
      const { events } = await eventStore.source({ criteria })
      return events.some((ev) => ev.name.name === "EnrollmentClosed")
    })

    const { events } = await eventStore.source({ criteria })
    expect(events.map((ev) => ev.name.name)).toEqual([
      "CourseCreated",
      "StudentSubscribed",
      "EnrollmentClosed",
    ])
  })
})
