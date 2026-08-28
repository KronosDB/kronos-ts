import type {
  CommandHandler,
  EmitCapability,
  EventHandler,
  EventHandlerContext,
  EventProcessor,
  EventStore,
  QueryHandler,
  TokenStore,
  UnitOfWork,
} from "@kronos-ts/core"

/**
 * These projections push live updates, so they say so — and they say ONLY
 * that: the face is intersected, not spelled as a type argument, so nothing
 * here restates the log it never had an opinion about.
 */
type EmittingContext = EventHandlerContext & EmitCapability
import { eventProcessor } from "@kronos-ts/core"
import type { FixtureLists, FixtureResources, PartialProcessor } from "@kronos-ts/test"
import { z } from "zod"
import { withNamespace, commandHandler, eventHandler, queryHandler } from "@kronos-ts/core"
import { state } from "@kronos-ts/core"

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
  tags: { courseId: (p) => p.courseId },
})

const CourseCapacityChanged = ns.event("CourseCapacityChanged", {
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const StudentSubscribed = ns.event("StudentSubscribed", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
})

const StudentUnsubscribed = ns.event("StudentUnsubscribed", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
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

const Course = state({
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    (): CourseState => ({ created: false, name: "", capacity: 0, enrolled: [] }),
    [CourseCreated, (s, { payload }) => ({
      ...s, created: true, name: payload.name, capacity: payload.capacity,
    })],
    [CourseCapacityChanged, (s, { payload }) => ({
      ...s, capacity: payload.capacity,
    })],
    [StudentSubscribed, (s, { payload }) => ({
      ...s, enrolled: [...s.enrolled, payload.studentId],
    })],
    [StudentUnsubscribed, (s, { payload }) => ({
      ...s, enrolled: s.enrolled.filter((sid) => sid !== payload.studentId),
    })],
  ],
})

// ---------------------------------------------------------------------------
// Command handlers (private)
// ---------------------------------------------------------------------------

const createCourse = commandHandler(CreateCourse, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  ctx.append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const changeCourseCapacity = commandHandler(ChangeCourseCapacity, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (cmd.capacity === course.capacity) return
  if (cmd.capacity < course.enrolled.length) throw new Error("Cannot reduce capacity below enrolled count")
  ctx.append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Student already subscribed")
  ctx.append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

const unsubscribeStudent = commandHandler(UnsubscribeStudent, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (!course.enrolled.includes(cmd.studentId)) throw new Error("Student not subscribed")
  ctx.append(StudentUnsubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
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

const onCreated = eventHandler(CourseCreated, async ({ payload: e }, ctx: EmittingContext) => {
  courseViews.set(e.courseId, {
    courseId: e.courseId,
    name: e.name,
    capacity: e.capacity,
    enrolledCount: 0,
    students: [],
  })
  ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, courseViews.get(e.courseId))
})

const onCapChanged = eventHandler(CourseCapacityChanged, async ({ payload: e }, ctx: EmittingContext) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.capacity = e.capacity
    ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
  }
})

const onSubscribed = eventHandler(StudentSubscribed, async ({ payload: e }, ctx: EmittingContext) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.enrolledCount++
    view.students.push(e.studentId)
    ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
  }
})

const onUnsubscribed = eventHandler(StudentUnsubscribed, async ({ payload: e }, ctx: EmittingContext) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.enrolledCount--
    view.students = view.students.filter((id) => id !== e.studentId)
    ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
  }
})

// ---------------------------------------------------------------------------
// Query handlers (singular)
// ---------------------------------------------------------------------------

const getCourseView = queryHandler(GetCourseView, async ({ payload: q }) => {
  const view = courseViews.get(q.courseId)
  if (!view) throw new Error(`Course "${q.courseId}" not found`)
  return view
})

const getAllCourses = queryHandler(GetAllCourses, async () => {
  return [...courseViews.values()].map((v) => ({
    courseId: v.courseId,
    name: v.name,
    enrolledCount: v.enrolledCount,
  }))
})

// ---------------------------------------------------------------------------
// Public export — slice configurer (the only production-facing surface)
// ---------------------------------------------------------------------------

/**
 * Course domain slice — the canonical single-file slice convention.
 *
 * All domain primitives (messages, entity, command/event handlers, projection
 * state, query handlers) are module-private `const`. Only this Slice-shaped
 * record and {@link courses} are exposed to production callers; the re-exports at
 * the bottom of this file exist solely for integration-test inspection.
 *
 * The three lists carry BARE definitions: no event store, no buses, no processor.
 * A composition root attaches the site, because which log a slice lives in and
 * where its cursor is kept are deployment facts. `Course` is in no list at all —
 * the handlers that fold it close over it, and `ctx.load` needs no introduction.
 */
export const courseSlice: {
  commandHandlers: ReadonlyArray<CommandHandler<any, any>>
  queryHandlers: ReadonlyArray<QueryHandler<any, any>>
  eventHandlers: ReadonlyArray<EventHandler<any, any>>
} = {
  commandHandlers: [createCourse, changeCourseCapacity, subscribeStudent, unsubscribeStudent],
  queryHandlers: [getCourseView, getAllCourses],
  eventHandlers: [onCreated, onCapChanged, onSubscribed, onUnsubscribed],
}

/** The durable name the course projection's cursor is stored under. */
export const COURSE_PROJECTION = "course-projection"


/**
 * The course slice as a COMPOSITION ROOT: a function of the resources it runs on.
 *
 * This is what a process deploys and what `testFixture` runs — the same function,
 * called the same way. The projection is left PARTIAL: the slice closes out its
 * own semantics (its durable name, global stream order, no dead-letter queue for
 * a lane-free projection) and leaves its resources in the parameter list, so the
 * site calls it full-handed. Three parameters, not four, declines the queue by
 * assignability.
 *
 * ONE STORE PARAMETER. There is no snapshot store beside the log any more —
 * a site that caches folds hands in a log that CAN, and the entries point at
 * the one object.
 */
export function courses({ eventStore }: FixtureResources): FixtureLists {
  const projection: PartialProcessor = (log, tokenStore, unitOfWork) =>
    eventProcessor({ name: COURSE_PROJECTION, eventStore: log, tokenStore, unitOfWork })

  return {
    commandHandlers: courseSlice.commandHandlers.map((h) => ({ ...h, eventStore })),
    queryHandlers: courseSlice.queryHandlers.map((h) => ({ ...h, eventStore })),
    eventHandlers: courseSlice.eventHandlers.map((h) => ({ ...h, processor: projection })),
  }
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
// Production code outside the test suite imports ONLY `courseSlice`.
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
  Course,
}

export type { CourseView, CourseState }
