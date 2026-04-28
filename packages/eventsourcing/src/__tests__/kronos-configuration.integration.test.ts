import { describe, expect, it } from "bun:test"
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
  trackingProcessor,
  EventCriteria,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { EventSourcingConfigurer } from "../eventsourcing-configurer.js"
import { sourcingCondition } from "../sourcing-condition.js"
import { load, append } from "../index.js"

// ============================================================================
// Domain — University courses (same domain, configured via composition)
// ============================================================================

// -- Messages --

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
})

const ChangeCourseCapacity = command({
  name: qn("university", "ChangeCourseCapacity"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
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

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
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

// -- Entities --

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[] }

const CourseEntity = eventSourcedEntity({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0, enrolled: [] }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: [
    on(CourseCreated, (s: CourseState, e) => ({ ...s, created: true, name: e.name, capacity: e.capacity })),
    on(CourseCapacityChanged, (s: CourseState, e) => ({ ...s, capacity: e.capacity })),
    on(StudentSubscribed, (s: CourseState, e) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })),
  ],
})

// -- Command Handlers --

const createCourse = commandHandler(CreateCourse, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const changeCourseCapacity = commandHandler(ChangeCourseCapacity, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (cmd.capacity === course.capacity) return
  append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already subscribed")
  append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// -- Query Handlers --

const courseViews = new Map<string, any>()

const courseProjection = eventHandlers({
  name: "course-projection",
  handlers: [
    on(CourseCreated, async (e) => {
      courseViews.set(e.courseId, { courseId: e.courseId, name: e.name, capacity: e.capacity, enrolledCount: 0 })
    }),
    on(StudentSubscribed, async (e) => {
      const v = courseViews.get(e.courseId)
      if (v) v.enrolledCount++
    }),
  ],
})

const courseQueries = queryHandlers({
  name: "course-queries",
  handlers: [
    on(GetCourseView, async (q) => {
      const v = courseViews.get(q.courseId)
      if (!v) throw new Error("Course not found")
      return v
    }),
  ],
})

// ============================================================================
// Tests
// ============================================================================

describe("KronosConfiguration — composable configuration", () => {
  function createApp() {
    courseViews.clear()
    return EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createCourse)
        m.registerCommandHandler(() => changeCourseCapacity)
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

  it("composes multiple configurers into a working application", async () => {
    const app = createApp()
    await app.start()

    // when
    await app.commandGateway.send(CreateCourse, {
      courseId: "cs-101",
      name: "Intro to CS",
      capacity: 30,
    })

    // then — events stored
    const { events } = await app.eventStore.source(
      sourcingCondition(EventCriteria.havingTags(tag("courseId", "cs-101"))),
    )
    expect(events).toHaveLength(1)
    expect(events[0]!.payload).toEqual({ courseId: "cs-101", name: "Intro to CS", capacity: 30 })
  })

  it("command handlers from different slices all work", async () => {
    const app = createApp()
    await app.start()

    // given — command from courseSlice
    await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })

    // when — command from enrollmentSlice
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })

    // then
    const { events } = await app.eventStore.source(
      sourcingCondition(EventCriteria.havingTags(tag("courseId", "cs-101"))),
    )
    expect(events).toHaveLength(2)
  })

  it("queries work through the composition", async () => {
    const app = createApp()
    await app.start()

    courseViews.set("cs-101", { courseId: "cs-101", name: "Intro", capacity: 30, enrolledCount: 5 })

    const result = await app.queryGateway.query(GetCourseView, { courseId: "cs-101" })

    expect(result).toEqual({ courseId: "cs-101", name: "Intro", capacity: 30, enrolledCount: 5 })
  })

  it("enforces business rules across slices", async () => {
    const app = createApp()
    await app.start()

    await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 2 })
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" })

    expect(
      app.commandGateway.send(SubscribeStudent, { courseId: "cs-101", studentId: "stu-003" }),
    ).rejects.toThrow("Course is full")
  })

  it("supports custom component registry access", async () => {
    let registryAccessed = false

    const app = EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createCourse)
        m.registerCommandHandler(() => changeCourseCapacity)
      })
      .componentRegistry((registry) => {
        registryAccessed = true
      })
      .build()

    expect(registryAccessed).toBe(true)
  })

  it("bus can be replaced by registering a new component", async () => {
    const dispatchLog: string[] = []

    const app = EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createCourse)
        m.registerCommandHandler(() => changeCourseCapacity)
      })
      .componentRegistry((registry) => {
        // Decorate the command bus to log dispatches
        registry.registerDecorator("commandBus", 200, (config, _name, bus: any) => {
          const originalDispatch = bus.dispatch.bind(bus)
          return {
            ...bus,
            async dispatch(message: any) {
              dispatchLog.push(message.name.name)
              return originalDispatch(message)
            },
          }
        })
      })
      .build()

    await app.start()
    await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })

    expect(dispatchLog).toEqual(["CreateCourse"])
  })
})
