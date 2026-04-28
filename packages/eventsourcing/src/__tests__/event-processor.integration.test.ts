import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  query,
  on,
  commandHandler,
  queryHandlers,
  eventHandlers,
  EventCriteria,
  trackingProcessor,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { EventSourcingConfigurer } from "../eventsourcing-configurer.js"
import { load, append } from "../index.js"

// ============================================================================
// Domain
// ============================================================================

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
})

const SubscribeStudent = command({
  name: qn("university", "SubscribeStudent"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
})

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const StudentSubscribed = event({
  name: qn("university", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId), tag("studentId", p.studentId)],
})

const GetCourseView = query({
  name: qn("university", "GetCourseView"),
  payload: z.object({ courseId: z.string() }),
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

// -- Command handlers --

const createCourse = commandHandler(CreateCourse, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// -- Projection (event handler → read model) --

const courseViews = new Map<string, { courseId: string; name: string; capacity: number; enrolledCount: number }>()

const courseProjection = eventHandlers({
  name: "course-projection",
  handlers: [
    on(CourseCreated, async (e) => {
      courseViews.set(e.courseId, {
        courseId: e.courseId,
        name: e.name,
        capacity: e.capacity,
        enrolledCount: 0,
      })
    }),
    on(StudentSubscribed, async (e) => {
      const view = courseViews.get(e.courseId)
      if (view) view.enrolledCount++
    }),
  ],
})

// -- Query handlers --

const courseQueries = queryHandlers({
  name: "course-queries",
  handlers: [
    on(GetCourseView, async (q) => {
      const view = courseViews.get(q.courseId)
      if (!view) throw new Error("Course not found")
      return view
    }),
  ],
})

// ============================================================================
// Helper — wait for the event processor to catch up
// ============================================================================

async function waitForProjection(
  check: () => boolean,
  timeoutMs = 5000,
  intervalMs = 50,
): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise((r) => setTimeout(r, intervalMs))
  }
  throw new Error("Timed out waiting for projection to update")
}

// ============================================================================
// Tests
// ============================================================================

describe("Full flow: command → event → processor → projection → query", () => {
  let app: Awaited<ReturnType<typeof createApp>>

  function createApp() {
    courseViews.clear()
    return EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createCourse)
        m.registerCommandHandler(() => subscribeStudent)
        m.registerEventProcessor(config =>
          trackingProcessor("course-projection")
            .registerEventHandler(courseProjection)
            .build()
        )
        m.registerQueryHandlers(() => courseQueries)
      })
      .build()
  }

  afterEach(async () => {
    await app?.stop()
  })

  it("command produces events, processor delivers to projection, query reads it", async () => {
    app = createApp()
    await app.start()

    // when — send a command
    await app.commandGateway.send(CreateCourse, {
      courseId: "cs-101",
      name: "Intro to CS",
      capacity: 30,
    })

    // then — wait for the event processor to deliver the event to the projection
    await waitForProjection(() => courseViews.has("cs-101"))

    // query the read model
    const result = await app.queryGateway.query(GetCourseView, { courseId: "cs-101" })
    expect(result).toEqual({
      courseId: "cs-101",
      name: "Intro to CS",
      capacity: 30,
      enrolledCount: 0,
    })
  })

  it("multiple commands produce events that update the projection", async () => {
    app = createApp()
    await app.start()

    // given
    await app.commandGateway.send(CreateCourse, {
      courseId: "cs-201",
      name: "Data Structures",
      capacity: 25,
    })

    // when
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-201", studentId: "stu-001" })
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-201", studentId: "stu-002" })

    // then
    await waitForProjection(() => {
      const view = courseViews.get("cs-201")
      return view !== undefined && view.enrolledCount === 2
    })

    const result = await app.queryGateway.query(GetCourseView, { courseId: "cs-201" })
    expect(result).toEqual({
      courseId: "cs-201",
      name: "Data Structures",
      capacity: 25,
      enrolledCount: 2,
    })
  })

  it("processor handles events from multiple command handler slices", async () => {
    courseViews.clear()
    app = EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createCourse)
        m.registerCommandHandler(() => subscribeStudent)
        m.registerEventProcessor(config =>
          trackingProcessor("course-projection")
            .registerEventHandler(courseProjection)
            .build()
        )
        m.registerQueryHandlers(() => courseQueries)
      })
      .build()

    await app.start()

    // Commands from different slices
    await app.commandGateway.send(CreateCourse, { courseId: "cs-301", name: "Algorithms", capacity: 20 })
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-301", studentId: "stu-001" })

    await waitForProjection(() => {
      const view = courseViews.get("cs-301")
      return view !== undefined && view.enrolledCount === 1
    })

    const result = await app.queryGateway.query(GetCourseView, { courseId: "cs-301" })
    expect(result).toEqual({
      courseId: "cs-301",
      name: "Algorithms",
      capacity: 20,
      enrolledCount: 1,
    })
  })
})
