import {
  commandHandler,
  correlatingHandler,
  eventHandler,
  eventProcessor,
  queryHandler,
  state,
  withNamespace,
} from "@kronos-ts/core"
import type {
  CommandBus,
  CommandHandlerEntry,
  EventHandlerEntry,
  EventProcessor,
  EventStore,
  QueryBus,
  QueryHandlerEntry,
  ScheduleStoreCapability,
  SnapshotCapableEventStore,
  TokenStore,
  UnitOfWork,
} from "@kronos-ts/core"
import { z } from "zod"

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

type CourseState = {
  created: boolean
  name: string
  capacity: number
  enrolled: string[]
  closed: boolean
}

export const Course = state({
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    (): CourseState => ({
      created: false,
      name: "",
      capacity: 0,
      enrolled: [],
      closed: false,
      }),
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

type CourseView = {
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
 * The objects a composition root shares between its entries — what `main.ts`
 * builds once and every entry carries. A test builds them too (wrapping in
 * recorders what its fixture should observe) and calls the same function.
 */
export type Shared = {
  readonly eventStore: SnapshotCapableEventStore & ScheduleStoreCapability
  readonly commandBus: CommandBus
  readonly queryBus: QueryBus
  readonly tokenStore: TokenStore
  readonly unitOfWork: () => UnitOfWork
}

/**
 * The three lists `kronos` takes, each entry carrying the shared objects with
 * THEIR OWN types — so the app reports a recorded log as a recorded log.
 */
export type Lists<S extends Shared = Shared> = {
  readonly commandHandlers?: ReadonlyArray<
    Omit<CommandHandlerEntry<any, any, any>, "eventStore" | "commandBus" | "queryBus"> &
      Pick<S, "eventStore" | "commandBus" | "queryBus">
  >
  readonly queryHandlers?: ReadonlyArray<
    Omit<QueryHandlerEntry<any, any>, "eventStore" | "queryBus"> & Pick<S, "eventStore" | "queryBus">
  >
  readonly eventHandlers?: ReadonlyArray<
    Omit<EventHandlerEntry<any, any, any>, "commandBus" | "queryBus" | "processor"> &
      Pick<S, "commandBus" | "queryBus"> & { readonly processor: EventProcessor<any, S["eventStore"]> }
  >
}

const decide = [createCourse, subscribeStudent, closeCourse]
const react = [closeWhenFull, armEnrolmentWindow, closeOnDeadline, onCreated, onSubscribed, onClosed]

/** The house wiring: every handler correlated, every entry carrying its objects. */
function sited<S extends Shared>(
  { eventStore, commandBus, queryBus, tokenStore, unitOfWork }: S,
  parts: { commands?: typeof decide; queries?: typeof getCourseView[]; events?: typeof react },
): Lists<S> {
  // The projection's own semantics, closed out here: its durable name and global
  // stream order. One processor for every event handler of the slice.
  const processor = eventProcessor("courses", eventStore, tokenStore, unitOfWork)
  return {
    commandHandlers: (parts.commands ?? []).map(
      (h) => ({ ...h, handler: correlatingHandler(h.handler), eventStore, commandBus, queryBus }) as never,
    ),
    queryHandlers: (parts.queries ?? []).map(
      (h) => ({ ...h, handler: correlatingHandler(h.handler), eventStore, queryBus }) as never,
    ),
    eventHandlers: (parts.events ?? []).map(
      (h) => ({ ...h, handler: correlatingHandler(h.handler), commandBus, queryBus, processor }) as never,
    ),
  }
}

/**
 * The whole university, as a function of the objects it runs on: what a
 * production composition root and a test both hand to `kronos`.
 */
export function university<S extends Shared>(shared: S): Lists<S> {
  return sited(shared, { commands: decide, queries: [getCourseView], events: react })
}

/** Decisions only — no automations, so `then` is exactly what the command decided. */
export function decisions<S extends Shared>(shared: S): Lists<S> {
  return sited(shared, { commands: decide })
}

/** Decisions plus the seat automation, with no scheduler and no read model. */
export function withAutomation<S extends Shared>(shared: S): Lists<S> {
  return sited(shared, { commands: decide, events: [closeWhenFull] })
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
  id: { orderId: z.string() },
  tags: (id) => ({ orderId: id.orderId }),
  evolve: [() => ({ token: "" }), [ReminderArmed, (s, { payload }) => ({ ...s, token: payload.token })]],
})

const armReminder = commandHandler(ArmReminder, async ({ payload: cmd }, ctx) => {
  const reminder = await ctx.load(Reminder, { orderId: cmd.orderId })
  if (reminder.token !== "") await ctx.cancelSchedule({ id: reminder.token })
  const token = await ctx.scheduleAfter(ReminderDue, { orderId: cmd.orderId }, cmd.afterMs)
  ctx.append(ReminderArmed, { orderId: cmd.orderId, token: token.id })
})

/** Reminders on their own — one command, one state, one schedule. */
export function reminders<S extends Shared>({ eventStore, commandBus, queryBus }: S): Lists<S> {
  return {
    commandHandlers: [{ ...armReminder, handler: correlatingHandler(armReminder.handler), eventStore, commandBus, queryBus } as never],
  }
}
