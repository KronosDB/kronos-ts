import {
  commandHandler,
  eventHandler,
  eventProcessor,
  queryHandler,
  state,
  withNamespace,
} from "@kronos-ts/core"
import type { EventStore, SnapshotStore } from "@kronos-ts/core"
import { z } from "zod"
import type { FixtureLists, PartialProcessor } from "../fixture.js"

// ---------------------------------------------------------------------------
// A university, written the way a slice is written: descriptors, one state, the
// three handler kinds, and a COMPOSITION ROOT that is a function of its
// resources. The fixture's scope parameter is that function's type, so the same
// value is what a process would deploy.
//
// It lives in its own module so the tests can import `event`/`command`/`query`
// from @kronos-ts/test — the ASSERTION vocabulary — without shadowing core's
// descriptor constructors, which are what a domain is declared with.
// ---------------------------------------------------------------------------

const ns = withNamespace("university")

export const CreateCourse = ns.command("CreateCourse", {
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
})

export const SubscribeStudent = ns.command("SubscribeStudent", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  result: z.object({ remainingSeats: z.number() }),
})

export const CloseCourse = ns.command("CloseCourse", {
  payload: z.object({ courseId: z.string() }),
})

export const CourseCreated = ns.event("CourseCreated", {
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

export const StudentSubscribed = ns.event("StudentSubscribed", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
})

export const CourseClosed = ns.event("CourseClosed", {
  payload: z.object({ courseId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

export const EnrolmentClosing = ns.event("EnrolmentClosing", {
  payload: z.object({ courseId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

export const GetCourseView = ns.query("GetCourseView", {
  payload: z.object({ courseId: z.string() }),
  result: z.object({
    courseId: z.string(),
    name: z.string(),
    enrolled: z.number(),
    closed: z.boolean(),
  }),
})

export class CourseFull extends Error {
  constructor(courseId: string) {
    super(`Course ${courseId} is full`)
    this.name = "CourseFull"
  }
}

interface CourseState {
  created: boolean
  name: string
  capacity: number
  enrolled: string[]
  closed: boolean
}

export const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (): CourseState => ({
    created: false,
    name: "",
    capacity: 0,
    enrolled: [],
    closed: false,
  }),
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    [
      CourseCreated,
      (s, { payload }) => ({ ...s, created: true, name: payload.name, capacity: payload.capacity }),
    ],
    [
      StudentSubscribed,
      (s, { payload }) => ({ ...s, enrolled: [...s.enrolled, payload.studentId] }),
    ],
    [CourseClosed, (s) => ({ ...s, closed: true })],
  ],
})

// ── decisions ──────────────────────────────────────────────────────────────

const createCourse = commandHandler(CreateCourse, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  ctx.append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.closed) throw new Error("Course is closed")
  if (course.enrolled.length >= course.capacity) throw new CourseFull(cmd.courseId)
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Student already subscribed")
  ctx.append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
  return { remainingSeats: course.capacity - course.enrolled.length - 1 }
})

const closeCourse = commandHandler(CloseCourse, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (course.closed) return
  ctx.append(CourseClosed, { courseId: cmd.courseId })
})

// ── automations ────────────────────────────────────────────────────────────

/** The last seat going closes the course. */
const closeWhenFull = eventHandler(StudentSubscribed, async ({ payload: e }, ctx) => {
  const course = await ctx.load(Course, { courseId: e.courseId })
  if (course.enrolled.length >= course.capacity && !course.closed) {
    await ctx.send(CloseCourse, { courseId: e.courseId })
  }
})

/** A new course gets a thirty-second enrolment window. */
const armEnrolmentWindow = eventHandler(CourseCreated, async ({ payload: e }, ctx) => {
  await ctx.scheduleAfter(EnrolmentClosing, { courseId: e.courseId }, 30_000)
})

/** The window closing closes the course. */
const closeOnDeadline = eventHandler(EnrolmentClosing, async ({ payload: e }, ctx) => {
  await ctx.send(CloseCourse, { courseId: e.courseId })
})

// ── the read model ─────────────────────────────────────────────────────────

interface CourseView {
  courseId: string
  name: string
  enrolled: number
  closed: boolean
}

const views = new Map<string, CourseView>()

/** Tests share one module, so they share one read model. Clear it between them. */
export function clearViews(): void {
  views.clear()
}

export function courseViews(): ReadonlyMap<string, CourseView> {
  return views
}

const onCreated = eventHandler(CourseCreated, ({ payload: e }) => {
  views.set(e.courseId, { courseId: e.courseId, name: e.name, enrolled: 0, closed: false })
})

const onSubscribed = eventHandler(StudentSubscribed, ({ payload: e }) => {
  const view = views.get(e.courseId)
  if (view) view.enrolled += 1
})

const onClosed = eventHandler(CourseClosed, ({ payload: e }) => {
  const view = views.get(e.courseId)
  if (view) view.closed = true
})

const getCourseView = queryHandler(GetCourseView, async ({ payload: q }) => {
  const view = views.get(q.courseId)
  if (!view) throw new Error(`Course "${q.courseId}" not found`)
  return view
})

// ── the composition root ───────────────────────────────────────────────────

/**
 * The projection's own semantics, closed out; its RESOURCES left as parameters.
 *
 * The slice knows its durable name and that it wants global stream order. It
 * does not know which log it reads or where its cursor lives — those are
 * deployment facts, so they stay in the parameter list and the site calls this
 * full-handed. Three parameters, not four: no dead-letter queue, because a lane-
 * free projection has no honest place to park.
 */
const projection: PartialProcessor = (eventStore, tokenStore, unitOfWork) =>
  eventProcessor({ name: "courses", eventStore, tokenStore, unitOfWork })

/**
 * The whole university, as a function of the resources it runs on.
 *
 * `testFixture(university)` and a production composition root call this the same
 * way — which is the point of taking the resources as parameters instead of
 * reaching for them.
 */
export function university(eventStore: EventStore, snapshotStore: SnapshotStore): FixtureLists {
  return {
    states: [{ ...Course, eventStore, snapshotStore }],
    commandHandlers: [createCourse, subscribeStudent, closeCourse].map((h) => ({
      ...h,
      eventStore,
      snapshotStore,
    })),
    queryHandlers: [getCourseView],
    eventHandlers: [
      closeWhenFull,
      armEnrolmentWindow,
      closeOnDeadline,
      onCreated,
      onSubscribed,
      onClosed,
    ].map((h) => ({ ...h, processor: projection })),
  }
}

/** Decisions only — no automations, so `then` is exactly what the command decided. */
export function decisions(eventStore: EventStore, snapshotStore: SnapshotStore): FixtureLists {
  return {
    states: [{ ...Course, eventStore, snapshotStore }],
    commandHandlers: [createCourse, subscribeStudent, closeCourse].map((h) => ({
      ...h,
      eventStore,
      snapshotStore,
    })),
  }
}

/** Decisions plus the seat automation, with no scheduler and no read model. */
export function withAutomation(eventStore: EventStore, snapshotStore: SnapshotStore): FixtureLists {
  return {
    ...decisions(eventStore, snapshotStore),
    eventHandlers: [{ ...closeWhenFull, processor: projection }],
  }
}

// ---------------------------------------------------------------------------
// A reminder that can be re-armed — the shape `scheduled` and `cancelled` are
// for. The token lives in STATE, which is how a process manager remembers what
// it armed, so re-arming can drop the old schedule before arranging the new one.
// ---------------------------------------------------------------------------

export const ArmReminder = ns.command("ArmReminder", {
  payload: z.object({ orderId: z.string(), afterMs: z.number() }),
})

export const ReminderArmed = ns.event("ReminderArmed", {
  payload: z.object({ orderId: z.string(), token: z.string() }),
  tags: { orderId: (p) => p.orderId },
})

export const ReminderDue = ns.event("ReminderDue", {
  payload: z.object({ orderId: z.string() }),
  tags: { orderId: (p) => p.orderId },
})

const Reminder = state({
  name: "Reminder",
  id: { orderId: z.string() },
  initial: () => ({ token: "" }),
  tags: (id) => ({ orderId: id.orderId }),
  evolve: [[ReminderArmed, (s, { payload }) => ({ ...s, token: payload.token })]],
})

const armReminder = commandHandler(ArmReminder, async ({ payload: cmd }, ctx) => {
  const reminder = await ctx.load(Reminder, { orderId: cmd.orderId })
  if (reminder.token !== "") await ctx.cancelSchedule({ id: reminder.token })
  const token = await ctx.scheduleAfter(ReminderDue, { orderId: cmd.orderId }, cmd.afterMs)
  ctx.append(ReminderArmed, { orderId: cmd.orderId, token: token.id })
})

/** Reminders on their own — one command, one state, one schedule. */
export function reminders(eventStore: EventStore, snapshotStore: SnapshotStore): FixtureLists {
  return {
    states: [{ ...Reminder, eventStore, snapshotStore }],
    commandHandlers: [{ ...armReminder, eventStore, snapshotStore }],
  }
}
