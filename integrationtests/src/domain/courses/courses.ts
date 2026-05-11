import type { App } from "@kronos-ts/core"
import { z } from "zod"
import { tag } from "@kronos-ts/common"
import {
  withNamespace,
  EventCriteria,
  on,
  commandHandler,
  eventHandler,
  queryHandlers, // transitional — Plan 11-04 swaps for queryHandler singular
  trackingProcessor,
  emitUpdate,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { load, append } from "@kronos-ts/eventsourcing"

// ---------------------------------------------------------------------------
// Namespace + messages (private to the slice)
// ---------------------------------------------------------------------------

const ns = withNamespace("university.courses")

const CreateCourse = ns.command("CreateCourse", {
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
})

const ChangeCourseCapacity = ns.command("ChangeCourseCapacity", {
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
})

const SubscribeStudent = ns.command("SubscribeStudent", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
})

const UnsubscribeStudent = ns.command("UnsubscribeStudent", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
})

const CourseCreated = ns.event("CourseCreated", {
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const CourseCapacityChanged = ns.event("CourseCapacityChanged", {
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const StudentSubscribed = ns.event("StudentSubscribed", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId), tag("studentId", p.studentId)],
})

const StudentUnsubscribed = ns.event("StudentUnsubscribed", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId), tag("studentId", p.studentId)],
})

const GetCourseView = ns.query("GetCourseView", {
  payload: z.object({ courseId: z.string() }),
})

const GetAllCourses = ns.query("GetAllCourses", {
  payload: z.object({}),
})

// ---------------------------------------------------------------------------
// Entity (private)
// ---------------------------------------------------------------------------

type CourseState = {
  created: boolean
  name: string
  capacity: number
  enrolled: string[]
}

const CourseEntity = eventSourcedEntity({
  name: "Course",
  id: { courseId: z.string() },
  initial: () => ({ created: false, name: "", capacity: 0, enrolled: [] }),
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: [
    on(CourseCreated, (s: CourseState, e) => ({
      ...s, created: true, name: e.name, capacity: e.capacity,
    })),
    on(CourseCapacityChanged, (s: CourseState, e) => ({
      ...s, capacity: e.capacity,
    })),
    on(StudentSubscribed, (s: CourseState, e) => ({
      ...s, enrolled: [...s.enrolled, e.studentId],
    })),
    on(StudentUnsubscribed, (s: CourseState, e) => ({
      ...s, enrolled: s.enrolled.filter((sid) => sid !== e.studentId),
    })),
  ],
})

// ---------------------------------------------------------------------------
// Command handlers (private)
// ---------------------------------------------------------------------------

const createCourse = commandHandler(CreateCourse, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const changeCourseCapacity = commandHandler(ChangeCourseCapacity, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (cmd.capacity === course.capacity) return
  if (cmd.capacity < course.enrolled.length) throw new Error("Cannot reduce capacity below enrolled count")
  append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Student already subscribed")
  append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

const unsubscribeStudent = commandHandler(UnsubscribeStudent, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (!course.enrolled.includes(cmd.studentId)) throw new Error("Student not subscribed")
  append(StudentUnsubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// ---------------------------------------------------------------------------
// Read model + event handlers (private)
// ---------------------------------------------------------------------------

type CourseView = {
  courseId: string
  name: string
  capacity: number
  enrolledCount: number
  students: string[]
}

const courseViews = new Map<string, CourseView>()

const onCreated = eventHandler(CourseCreated, async (e, _metadata) => {
  courseViews.set(e.courseId, {
    courseId: e.courseId,
    name: e.name,
    capacity: e.capacity,
    enrolledCount: 0,
    students: [],
  })
  emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, courseViews.get(e.courseId))
})

const onCapChanged = eventHandler(CourseCapacityChanged, async (e, _metadata) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.capacity = e.capacity
    emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
  }
})

const onSubscribed = eventHandler(StudentSubscribed, async (e, _metadata) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.enrolledCount++
    view.students.push(e.studentId)
    emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
  }
})

const onUnsubscribed = eventHandler(StudentUnsubscribed, async (e, _metadata) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.enrolledCount--
    view.students = view.students.filter((id) => id !== e.studentId)
    emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
  }
})

// ---------------------------------------------------------------------------
// Query handlers (TRANSITIONAL grouped form — Plan 11-04 flips to singular `queryHandler`)
// ---------------------------------------------------------------------------

const courseQueries = queryHandlers({
  name: "course-queries",
  handlers: [
    on(GetCourseView, async (q) => {
      const view = courseViews.get(q.courseId)
      if (!view) throw new Error(`Course "${q.courseId}" not found`)
      return view
    }),
    on(GetAllCourses, async () => {
      return [...courseViews.values()].map((v) => ({
        courseId: v.courseId,
        name: v.name,
        enrolledCount: v.enrolledCount,
      }))
    }),
  ],
})

// ---------------------------------------------------------------------------
// Public export — slice configurer (the only production-facing surface)
// ---------------------------------------------------------------------------

/**
 * Course domain slice — Phase 11 canonical single-file slice convention.
 *
 * All domain primitives (messages, entity, command/event handlers, projection
 * state, query handlers) are module-private `const`. Only this configurer
 * is exposed to production callers. The re-exports at the bottom of this
 * file exist solely for integration-test inspection — they are not API.
 *
 * Compose via `app.use(configureCourses)` to honor the slice convention from
 * `.planning/phases/11-.../CONTEXT.md`.
 */
export function configureCourses(app: App): void {
  app.entities(CourseEntity)
  app.commands(createCourse, changeCourseCapacity, subscribeStudent, unsubscribeStudent)
  app.queries(courseQueries)
  app.processors(
    trackingProcessor("course-projection")
      .eventHandlers(onCreated, onCapChanged, onSubscribed, onUnsubscribed)
      .onReset(async () => {
        courseViews.clear()
      })
      .build(),
  )
}

// ---------------------------------------------------------------------------
// Test inspection helpers — used by integration tests only, not API
// ---------------------------------------------------------------------------

export function getCourseViews(): ReadonlyMap<string, CourseView> {
  return courseViews
}

export function clearCourseViews(): void {
  courseViews.clear()
}

// ---------------------------------------------------------------------------
// Re-exports — message descriptors + entity needed by integration tests
// to construct command / query messages and assert on entity behavior.
// Production code outside the test suite imports ONLY `configureCourses`.
// ---------------------------------------------------------------------------

export {
  CreateCourse,
  ChangeCourseCapacity,
  SubscribeStudent,
  UnsubscribeStudent,
  CourseCreated,
  CourseCapacityChanged,
  StudentSubscribed,
  StudentUnsubscribed,
  GetCourseView,
  GetAllCourses,
  CourseEntity,
}

export type { CourseView, CourseState }
