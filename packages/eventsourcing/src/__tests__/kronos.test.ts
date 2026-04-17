import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn, ComponentKeys } from "@kronos-ts/common"
import {
  command,
  event,
  query,
  on,
  commandHandler,
  eventHandlers,
  queryHandlers,
  EventCriteria,
  subscribingProcessor,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { kronos, type Kronos, type KronosApplication } from "../kronos.js"
import { createInMemorySnapshotStore, afterEvents } from "../index.js"

// ============================================================================
// Domain: University courses
// ============================================================================

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
  tags: (p) => ({ courseId: p.courseId }),
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: (p) => ({ courseId: p.courseId }),
})

const StudentSubscribed = event({
  name: qn("university", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => ({ courseId: p.courseId }),
})

const GetCourseView = query({
  name: qn("university", "GetCourseView"),
  payload: z.object({ courseId: z.string() }),
})

const GetAllCourses = query({
  name: qn("university", "GetAllCourses"),
  payload: z.object({}),
})

type CourseView = {
  courseId: string
  name: string
  capacity: number
  enrolledCount: number
}

// ============================================================================
// Helpers
// ============================================================================

async function waitFor(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error("Timed out waiting for condition")
}

// ============================================================================
// Tests
// ============================================================================

describe("Kronos API", () => {
  let app: KronosApplication

  afterEach(async () => {
    await app?.stop()
  })

  describe("inline plugin — define and register in one place", () => {
    it("full CQRS flow: command → event → processor → projection → query", async () => {
      // given
      const courseViews = new Map<string, CourseView>()

      function courses(k: Kronos) {
        const Course = k.state({
          name: "Course",
          id: { courseId: z.string() },
          initial: () => ({ created: false, name: "", capacity: 0, enrolled: [] as string[] }),
          criteria: (id) => EventCriteria.havingTags({ courseId: id.courseId }),
          evolve: [
            on(CourseCreated, (s, e) => ({
              ...s, created: true, name: e.name, capacity: e.capacity,
            })),
            on(CourseCapacityChanged, (s, e) => ({
              ...s, capacity: e.capacity,
            })),
            on(StudentSubscribed, (s, e) => ({
              ...s, enrolled: [...s.enrolled, e.studentId],
            })),
          ],
        })

        k.commandHandler(CreateCourse, async (cmd, { load, append }) => {
          const course = await load(Course, { courseId: cmd.courseId })
          if (course.created) throw new Error("Course already exists")
          append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
        })

        k.commandHandler(ChangeCourseCapacity, async (cmd, { load, append }) => {
          const course = await load(Course, { courseId: cmd.courseId })
          if (!course.created) throw new Error("Course does not exist")
          append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
        })

        k.commandHandler(SubscribeStudent, async (cmd, { load, append }) => {
          const course = await load(Course, { courseId: cmd.courseId })
          if (!course.created) throw new Error("Course does not exist")
          if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
          if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
          append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
        })

        k.trackingProcessor("course-projection", [
          on(CourseCreated, async (e) => {
            courseViews.set(e.courseId, {
              courseId: e.courseId,
              name: e.name,
              capacity: e.capacity,
              enrolledCount: 0,
            })
          }),
          on(CourseCapacityChanged, async (e) => {
            const view = courseViews.get(e.courseId)
            if (view) view.capacity = e.capacity
          }),
          on(StudentSubscribed, async (e) => {
            const view = courseViews.get(e.courseId)
            if (view) view.enrolledCount++
          }),
        ])

        k.queryHandlers("course-queries", [
          on(GetCourseView, async (q) => {
            const view = courseViews.get(q.courseId)
            if (!view) throw new Error("Course not found")
            return view
          }),
          on(GetAllCourses, async () => [...courseViews.values()]),
        ])
      }

      app = await kronos().register(courses).start()

      // when
      await app.commandGateway.send(CreateCourse, {
        courseId: "cs-101",
        name: "Intro to CS",
        capacity: 30,
      })

      await waitFor(() => courseViews.has("cs-101"))

      // then
      const view = await app.queryGateway.query(GetCourseView, { courseId: "cs-101" }) as CourseView
      expect(view.name).toBe("Intro to CS")
      expect(view.capacity).toBe(30)
    })

    it("enforces business rules", async () => {
      // given
      function courses(k: Kronos) {
        const Course = k.state({
          name: "Course",
          id: { courseId: z.string() },
          initial: () => ({ created: false, name: "", capacity: 0, enrolled: [] as string[] }),
          criteria: (id) => EventCriteria.havingTags({ courseId: id.courseId }),
          evolve: [
            on(CourseCreated, (s, e) => ({
              ...s, created: true, name: e.name, capacity: e.capacity,
            })),
            on(StudentSubscribed, (s, e) => ({
              ...s, enrolled: [...s.enrolled, e.studentId],
            })),
          ],
        })

        k.commandHandler(CreateCourse, async (cmd, { load, append }) => {
          const course = await load(Course, { courseId: cmd.courseId })
          if (course.created) throw new Error("Course already exists")
          append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
        })

        k.commandHandler(SubscribeStudent, async (cmd, { load, append }) => {
          const course = await load(Course, { courseId: cmd.courseId })
          if (!course.created) throw new Error("Course does not exist")
          if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
          if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
          append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
        })
      }

      app = await kronos().register(courses).start()

      // when
      await app.commandGateway.send(CreateCourse, {
        courseId: "small-1",
        name: "Small Course",
        capacity: 2,
      })

      await app.commandGateway.send(SubscribeStudent, {
        courseId: "small-1",
        studentId: "stu-1",
      })

      // then — duplicate enrollment (before course is full)
      await expect(
        app.commandGateway.send(SubscribeStudent, { courseId: "small-1", studentId: "stu-1" }),
      ).rejects.toThrow("Already enrolled")

      // fill it up
      await app.commandGateway.send(SubscribeStudent, {
        courseId: "small-1",
        studentId: "stu-2",
      })

      // then — course is full (capacity 2, 2 enrolled)
      await expect(
        app.commandGateway.send(SubscribeStudent, { courseId: "small-1", studentId: "stu-3" }),
      ).rejects.toThrow("Course is full")
    })
  })

  describe("pre-built definitions — define separately, register in plugin", () => {
    it("accepts pre-built entity, handlers, and processor", async () => {
      // given — define outside the plugin (uses initial + record tags)
      const CourseEntity = eventSourcedEntity({
        name: "Course",
        id: { courseId: z.string() },
        initial: () => ({ created: false, name: "", capacity: 0 }),
        criteria: (id) => EventCriteria.havingTags({ courseId: id.courseId }),
        evolve: [
          on(CourseCreated, (s, e) => ({
            ...s, created: true, name: e.name, capacity: e.capacity,
          })),
        ],
      })

      const createCourse = commandHandler(CreateCourse, async (cmd, { load, append }) => {
        const course = await load(CourseEntity, { courseId: cmd.courseId })
        if (course.created) throw new Error("Course already exists")
        append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
      })

      const courseViews = new Map<string, CourseView>()

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
        ],
      })

      const courseQueries = queryHandlers({
        name: "course-queries",
        handlers: [
          on(GetCourseView, async (q) => {
            const view = courseViews.get(q.courseId)
            if (!view) throw new Error("Not found")
            return view
          }),
        ],
      })

      // register in plugin — pre-built definitions
      app = await kronos()
        .register((k) => {
          k.state(CourseEntity)
          k.commandHandler(createCourse)
          k.trackingProcessor("course-projection", [courseProjection])
          k.queryHandlers(courseQueries)
        })
        .start()

      // when
      await app.commandGateway.send(CreateCourse, {
        courseId: "pre-1",
        name: "Pre-built Course",
        capacity: 10,
      })

      await waitFor(() => courseViews.has("pre-1"))

      // then
      const view = await app.queryGateway.query(GetCourseView, { courseId: "pre-1" }) as CourseView
      expect(view.name).toBe("Pre-built Course")
    })
  })

  describe("multiple plugins", () => {
    it("composes multiple domain slices", async () => {
      // given
      const courseViews = new Map<string, CourseView>()

      function courseCommands(k: Kronos) {
        const Course = k.state({
          name: "Course",
          id: { courseId: z.string() },
          initial: () => ({ created: false, name: "", capacity: 0 }),
          criteria: (id) => EventCriteria.havingTags({ courseId: id.courseId }),
          evolve: [
            on(CourseCreated, (s, e) => ({
              ...s, created: true, name: e.name, capacity: e.capacity,
            })),
          ],
        })

        k.commandHandler(CreateCourse, async (cmd, { load, append }) => {
          const course = await load(Course, { courseId: cmd.courseId })
          if (course.created) throw new Error("Course already exists")
          append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
        })
      }

      function courseProjections(k: Kronos) {
        k.trackingProcessor("course-projection", [
          on(CourseCreated, async (e) => {
            courseViews.set(e.courseId, {
              courseId: e.courseId,
              name: e.name,
              capacity: e.capacity,
              enrolledCount: 0,
            })
          }),
        ])

        k.queryHandlers("course-queries", [
          on(GetCourseView, async (q) => {
            const view = courseViews.get(q.courseId)
            if (!view) throw new Error("Not found")
            return view
          }),
        ])
      }

      app = await kronos()
        .register(courseCommands)
        .register(courseProjections)
        .start()

      // when
      await app.commandGateway.send(CreateCourse, {
        courseId: "multi-1",
        name: "Multi Course",
        capacity: 50,
      })

      await waitFor(() => courseViews.has("multi-1"))

      // then
      const view = await app.queryGateway.query(GetCourseView, { courseId: "multi-1" }) as CourseView
      expect(view.name).toBe("Multi Course")
    })
  })

  describe("subscribing processor", () => {
    it("delivers events synchronously via subscribing processor", async () => {
      // given
      const received: string[] = []

      function courses(k: Kronos) {
        const Course = k.state({
          name: "Course",
          id: { courseId: z.string() },
          initial: () => ({ created: false, name: "", capacity: 0 }),
          criteria: (id) => EventCriteria.havingTags({ courseId: id.courseId }),
          evolve: [
            on(CourseCreated, (s, e) => ({
              ...s, created: true, name: e.name, capacity: e.capacity,
            })),
          ],
        })

        k.commandHandler(CreateCourse, async (cmd, { load, append }) => {
          await load(Course, { courseId: cmd.courseId })
          append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
        })

        k.subscribingProcessor("sync-projection", [
          on(CourseCreated, async (e) => {
            received.push(e.courseId)
          }),
        ])
      }

      app = await kronos().register(courses).start()

      // when
      await app.commandGateway.send(CreateCourse, {
        courseId: "sync-1",
        name: "Sync Course",
        capacity: 10,
      })

      // then — subscribing processor delivers synchronously
      expect(received).toContain("sync-1")
    })
  })

  describe("infrastructure configuration", () => {
    it("supports component registry escape hatch", async () => {
      // given
      const snapshotStore = createInMemorySnapshotStore()

      function courses(k: Kronos) {
        const Course = k.state({
          name: "Course",
          id: { courseId: z.string() },
          initial: () => ({ created: false, name: "", capacity: 0 }),
          criteria: (id) => EventCriteria.havingTags({ courseId: id.courseId }),
          evolve: [
            on(CourseCreated, (s, e) => ({
              ...s, created: true, name: e.name, capacity: e.capacity,
            })),
            on(CourseCapacityChanged, (s, e) => ({
              ...s, capacity: e.capacity,
            })),
          ],
          snapshotPolicy: afterEvents(3),
        })

        k.commandHandler(CreateCourse, async (cmd, { load, append }) => {
          await load(Course, { courseId: cmd.courseId })
          append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
        })

        k.commandHandler(ChangeCourseCapacity, async (cmd, { load, append }) => {
          await load(Course, { courseId: cmd.courseId })
          append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
        })
      }

      app = await kronos()
        .register(courses)
        .componentRegistry((r) => {
          r.register(ComponentKeys.SNAPSHOT_STORE, () => snapshotStore)
        })
        .start()

      // when — 5 events total, snapshot triggers after 3+
      await app.commandGateway.send(CreateCourse, { courseId: "snap-1", name: "Snap", capacity: 10 })
      await app.commandGateway.send(ChangeCourseCapacity, { courseId: "snap-1", capacity: 20 })
      await app.commandGateway.send(ChangeCourseCapacity, { courseId: "snap-1", capacity: 30 })
      await app.commandGateway.send(ChangeCourseCapacity, { courseId: "snap-1", capacity: 40 })
      await app.commandGateway.send(ChangeCourseCapacity, { courseId: "snap-1", capacity: 50 })

      // Wait for async snapshot storage
      await new Promise((r) => setTimeout(r, 50))

      // then
      const snapshot = await snapshotStore.load("Course", { courseId: "snap-1" })
      expect(snapshot).toBeDefined()
      expect((snapshot!.payload as any).capacity).toBeGreaterThanOrEqual(30)
    })
  })
})
