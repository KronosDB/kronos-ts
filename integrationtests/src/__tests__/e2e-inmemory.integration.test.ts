/**
 * End-to-end integration test — full CQRS/ES flow with in-memory infrastructure.
 *
 * Validates the complete framework without external dependencies:
 * - Command dispatch → event sourcing → state management
 * - Event processor → projection updates
 * - Query dispatch → read model
 * - Snapshots with per-entity policy
 * - Correlation data propagation
 * - Business rule enforcement
 *
 * Wired against the functional composition root: `kronos({ states,
 * commandHandlers, queryHandlers, eventHandlers })`. There is no container, so
 * nothing has to be probed back out of one — the event store is an ordinary
 * value the test creates and hands to each entry's own `{ eventStore }`, then
 * asserts against directly.
 */
import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn } from "@kronos-ts/core"
import {
  command, event, query, send, commandHandler, eventHandler, queryHandler,
  eventProcessor, type EventProcessor,
  type CommandHandlerDefinition, type QueryHandlerDefinition, type EventHandlerDefinition,
  inMemoryTokenStore, type TokenStore,
} from "@kronos-ts/core"
import { state, type StateModule } from "@kronos-ts/core"
import {
  type SnapshotStore,
  inMemoryEventStore,
  inMemorySnapshotStore,
  afterEvents,
} from "@kronos-ts/core"
import {
  kronos,
  type App,
  type CommandHandlerEntry,
  type QueryHandlerEntry,
  type EventHandlerEntry,
  type HandlerSite,
  type Sited,
  type StateEntry,
  type StateOptions,
} from "@kronos-ts/core"
import {
  lineage,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  simpleCommandBus,
  simpleQueryBus,
  type UnitOfWork,
  type CommandBus,
  type QueryBus,
} from "@kronos-ts/core"

/**
 * The two things `kronos` needs that are not handlers. The UoW runner is
 * named once and handed to `simpleCommandBus` (which captures it at
 * construction) — writing it on an adjacent line is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork): { commandBus: CommandBus; queryBus: QueryBus } {
  return {
    commandBus: interceptingCommandBus(simpleCommandBus(uow), lineage),
    queryBus: interceptingQueryBus(simpleQueryBus(uow), lineage),
  }
}

/**
 * Everything `sitedOn` accepts, BEFORE a site is attached — deliberately loose
 * (`any, any`) generics on the command/query/event definitions, because a
 * caller here typically spreads an already-inferred heterogeneous array (e.g.
 * `...queryHandlers`) into this rest parameter, and the precise per-handler
 * payload/result types would otherwise fail the standard
 * `CommandHandlerEntry`/`QueryHandlerEntry`/`EventHandlerEntry` unions'
 * structural (contravariant) check.
 */
type SitedItem =
  | Sited<StateModule<any, any>>
  | readonly [Sited<StateModule<any, any>>, StateOptions]
  | Sited<CommandHandlerDefinition<any, any>>
  | Sited<QueryHandlerDefinition<any, any>>
  | EventHandlerDefinition<any, any>

/** What a host attaches uniformly here. There is no `stores` record any more —
 * this is the ARGUMENT LIST of a local helper, and every entry comes out
 * carrying BARE properties. `commandBus`/`queryBus` are required: `kronos`
 * takes them PER ENTRY now, not once for the whole app. */
type Site = HandlerSite & {
  commandBus: CommandBus
  queryBus: QueryBus
  tokenStore?: TokenStore
  unitOfWork?: () => UnitOfWork
  /**
   * Durable name for any bare event-handler entries in this call. All event
   * handlers passed to ONE `sitedOn` call share ONE processor built here —
   * for more than one processor in an app, call `sitedOn` once per processor
   * and combine the results with `mergeSited`.
   */
  processorName?: string
}

/**
 * Attach one site to a flat list of entries — the composition root's job,
 * replacing the old `module(name, stores, ...handlers)` — and sort them into
 * the four fields `kronos` now takes. Honours the `[state, options]` tuple
 * shape. Spread the result straight into `kronos({ ...sitedOn(site, ...) })`.
 *
 * `tokenStore` and `unitOfWork` are defaulted here because this is a test rig:
 * an event handler needs both to build its shared processor, and every caller
 * below wants the same in-memory cursor store and the same factory for the
 * whole app.
 */
function sitedOn(
  site: Site,
  ...items: ReadonlyArray<SitedItem>
): {
  states: StateEntry[]
  commandHandlers: CommandHandlerEntry[]
  queryHandlers: QueryHandlerEntry[]
  eventHandlers: EventHandlerEntry[]
} {
  const {
    tokenStore = inMemoryTokenStore(),
    unitOfWork: uow = unitOfWork,
    commandBus,
    queryBus,
    processorName,
    ...handlerSite
  } = site
  const states: StateEntry[] = []
  const commandHandlers: CommandHandlerEntry[] = []
  const queryHandlers: QueryHandlerEntry[] = []
  const eventHandlers: EventHandlerEntry[] = []
  let processor: EventProcessor | undefined

  for (const item of items) {
    if (Array.isArray(item)) {
      const [stateDef, options] = item
      states.push([{ ...stateDef, ...handlerSite }, options] as StateEntry)
      continue
    }
    const kind = (item as { kind?: string }).kind
    if (kind === "state-module") {
      states.push({ ...(item as object), ...handlerSite } as StateEntry)
    } else if (kind === "command-handler") {
      commandHandlers.push({ ...(item as object), ...handlerSite, commandBus, queryBus } as CommandHandlerEntry)
    } else if (kind === "query-handler") {
      queryHandlers.push({ ...(item as object), ...handlerSite, queryBus } as QueryHandlerEntry)
    } else if (kind === "event-handler") {
      if (!processor) {
        if (!processorName) {
          throw new Error("sitedOn: an event handler was given but no `processorName` on the site")
        }
        if (!handlerSite.eventStore) {
          throw new Error("sitedOn: an event handler needs an `eventStore` on the site")
        }
        processor = eventProcessor({
          name: processorName,
          eventStore: handlerSite.eventStore,
          tokenStore,
          unitOfWork: uow,
        })
      }
      eventHandlers.push({ ...(item as object), commandBus, queryBus, processor } as EventHandlerEntry)
    }
  }
  return { states, commandHandlers, queryHandlers, eventHandlers }
}

/** Combine several `sitedOn` groups (e.g. one per processor) into one. */
function mergeSited(
  ...groups: ReadonlyArray<ReturnType<typeof sitedOn>>
): {
  states: StateEntry[]
  commandHandlers: CommandHandlerEntry[]
  queryHandlers: QueryHandlerEntry[]
  eventHandlers: EventHandlerEntry[]
} {
  return {
    states: groups.flatMap((g) => g.states),
    commandHandlers: groups.flatMap((g) => g.commandHandlers),
    queryHandlers: groups.flatMap((g) => g.queryHandlers),
    eventHandlers: groups.flatMap((g) => g.eventHandlers),
  }
}


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
  tags: { courseId: (p) => p.courseId },
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const StudentSubscribed = event({
  name: qn("university", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

const CloseEnrollment = command({
  name: qn("university", "CloseEnrollment"),
  payload: z.object({ courseId: z.string() }),
})

const EnrollmentClosed = event({
  name: qn("university", "EnrollmentClosed"),
  payload: z.object({ courseId: z.string() }),
  tags: { courseId: (p) => p.courseId },
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
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    [CourseCreated, (s, { payload: e }) => ({
      ...s, created: true, name: e.name, capacity: e.capacity,
    })],
    [CourseCapacityChanged, (s, { payload: e }) => ({
      ...s, capacity: e.capacity,
    })],
    [StudentSubscribed, (s, { payload: e }) => ({
      ...s, enrolled: [...s.enrolled, e.studentId],
    })],
    [EnrollmentClosed, (s) => ({ ...s, closed: true })],
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
    const buses = inMemoryBuses()

    running = kronos({
      ...sitedOn(
        { eventStore: inMemoryEventStore(), ...buses, processorName: "course-projection" },
        Course,
        createCourse, changeCourseCapacity, subscribeStudent,
        ...queryHandlers,
        ...projectionHandlers,
      ),
    })

    // when
    await send(buses.commandBus, CreateCourse, {
      courseId: "cs-101",
      name: "Intro to CS",
      capacity: 30,
    })

    await waitFor(() => courseViews.has("cs-101"))

    // then
    const view = await query(buses.queryBus, GetCourseView, { courseId: "cs-101" })
    expect(view).toBeDefined()
    expect((view as CourseView).name).toBe("Intro to CS")
    expect((view as CourseView).capacity).toBe(30)
  })

  it("enforces business rules across commands", async () => {
    // given
    const { projectionHandlers, queryHandlers } = createProjection()
    const buses = inMemoryBuses()

    running = kronos({
      ...sitedOn(
        { eventStore: inMemoryEventStore(), ...buses, processorName: "course-projection" },
        Course,
        createCourse, subscribeStudent,
        ...queryHandlers,
        ...projectionHandlers,
      ),
    })

    // when
    await send(buses.commandBus, CreateCourse, {
      courseId: "small-101",
      name: "Small Course",
      capacity: 2,
    })

    await send(buses.commandBus, SubscribeStudent, { courseId: "small-101", studentId: "stu-1" })

    // then — duplicate enrollment (before capacity is full)
    await expect(
      send(buses.commandBus, SubscribeStudent, { courseId: "small-101", studentId: "stu-1" }),
    ).rejects.toThrow("Already enrolled")

    // fill the course
    await send(buses.commandBus, SubscribeStudent, { courseId: "small-101", studentId: "stu-2" })

    // then — course is full (capacity 2, 2 enrolled)
    await expect(
      send(buses.commandBus, SubscribeStudent, { courseId: "small-101", studentId: "stu-3" }),
    ).rejects.toThrow("Course is full")
  })

  // Per-STATE snapshot config (policy + its own store), declared as a
  // [state, options] tuple in the handler list.
  it("snapshots accelerate entity loading", async () => {
    // given
    const eventStore = inMemoryEventStore()
    const snapshotStore: SnapshotStore = inMemorySnapshotStore()
    const { projectionHandlers, queryHandlers } = createProjection()
    const buses = inMemoryBuses()

    running = kronos({
      ...sitedOn(
        { eventStore, snapshotStore, ...buses, processorName: "course-projection" },
        [Course, { snapshotPolicy: afterEvents(3), snapshotStore }],
        createCourse, changeCourseCapacity,
        ...queryHandlers,
        ...projectionHandlers,
      ),
    })

    // when — create + 4 capacity changes (5 events total)
    // Snapshot triggers after 3+ events are replayed during a load.
    await send(buses.commandBus, CreateCourse, { courseId: "snap-101", name: "Snap Course", capacity: 10 })
    await send(buses.commandBus, ChangeCourseCapacity, { courseId: "snap-101", capacity: 20 })
    await send(buses.commandBus, ChangeCourseCapacity, { courseId: "snap-101", capacity: 30 })
    await send(buses.commandBus, ChangeCourseCapacity, { courseId: "snap-101", capacity: 40 })
    // This load replays 4 events → triggers snapshot with capacity=40
    await send(buses.commandBus, ChangeCourseCapacity, { courseId: "snap-101", capacity: 50 })

    // Wait for async snapshot storage
    await new Promise(r => setTimeout(r, 50))

    // then — snapshot exists with state from when it was triggered
    const snapshot = await snapshotStore.load("Course", { courseId: "snap-101" })
    expect(snapshot).toBeDefined()
    expect((snapshot!.payload as CourseState).capacity).toBeGreaterThanOrEqual(30)
  })

  it("multiple processors operate independently", async () => {
    // given
    const { projectionHandlers, queryHandlers, courseViews } = createProjection()
    const auditLog: string[] = []
    const eventStore = inMemoryEventStore()
    const buses = inMemoryBuses()

    const auditOnCourseCreated = eventHandler(CourseCreated, async ({ payload: e }, ctx) => { auditLog.push(`created:${e.courseId}`) })
    const auditOnStudentSubscribed = eventHandler(StudentSubscribed, async ({ payload: e }, ctx) => { auditLog.push(`enrolled:${e.studentId}`) })

    running = kronos({
      ...mergeSited(
        sitedOn(
          { eventStore, ...buses, processorName: "course-projection" },
          Course,
          createCourse, subscribeStudent,
          ...queryHandlers,
          ...projectionHandlers,
        ),
        sitedOn(
          { eventStore, ...buses, processorName: "audit-log" },
          auditOnCourseCreated, auditOnStudentSubscribed,
        ),
      ),
    })

    // when
    await send(buses.commandBus, CreateCourse, { courseId: "multi-1", name: "Multi", capacity: 10 })
    await send(buses.commandBus, SubscribeStudent, { courseId: "multi-1", studentId: "stu-1" })

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
    const buses = inMemoryBuses()

    running = kronos({
      ...sitedOn({ eventStore, ...buses }, Course, createCourse),
    })

    // when — dispatch a command with custom metadata
    const metadata = { tenantId: "t-1", userId: "u-42" }
    await send(buses.commandBus, CreateCourse, {
      courseId: "corr-1",
      name: "Correlation Test",
      capacity: 10,
    }, metadata)

    // then — events inherit the command's metadata
    const { events } = await eventStore.source({
      query: { tags: { courseId: "corr-1" } },
    })

    expect(events.length).toBe(1)
    expect(events[0]!.metadata.tenantId).toBe("t-1")
    expect(events[0]!.metadata.userId).toBe("u-42")
  })

  it("query returns all courses", async () => {
    // given
    const { projectionHandlers, queryHandlers, courseViews } = createProjection()
    const buses = inMemoryBuses()

    running = kronos({
      ...sitedOn(
        { eventStore: inMemoryEventStore(), ...buses, processorName: "course-projection" },
        Course,
        createCourse,
        ...queryHandlers,
        ...projectionHandlers,
      ),
    })

    // when
    await send(buses.commandBus, CreateCourse, { courseId: "all-1", name: "Course A", capacity: 10 })
    await send(buses.commandBus, CreateCourse, { courseId: "all-2", name: "Course B", capacity: 20 })

    await waitFor(() => courseViews.size >= 2)

    // then
    const allCourses = await query(buses.queryBus, GetAllCourses, {}) as CourseView[]
    expect(allCourses.length).toBeGreaterThanOrEqual(2)
    expect(allCourses.some(c => c.courseId === "all-1")).toBe(true)
    expect(allCourses.some(c => c.courseId === "all-2")).toBe(true)
  })

  it("stateful automation — an event handler sends a command in its own UoW", async () => {
    // given — a "close enrolment when full" automation on its own processor
    const eventStore = inMemoryEventStore()
    const buses = inMemoryBuses()

    running = kronos({
      ...sitedOn(
        { eventStore, ...buses, processorName: "enrollment-automation" },
        Course,
        createCourse, subscribeStudent, closeEnrollment,
        closeEnrollmentWhenFull,
      ),
    })

    // when — a one-seat course is filled
    await send(buses.commandBus, CreateCourse, { courseId: "auto-1", name: "One Seat", capacity: 1 })
    await send(buses.commandBus, SubscribeStudent, { courseId: "auto-1", studentId: "stu-1" })

    // then — the automation sources the now-full course and dispatches
    // CloseEnrollment, whose handler appends EnrollmentClosed in its own UoW
    const courseQuery = { tags: { courseId: "auto-1" } }
    await waitFor(async () => {
      const { events } = await eventStore.source({ query: courseQuery })
      return events.some((ev) => ev.name.name === "EnrollmentClosed")
    })

    const { events } = await eventStore.source({ query: courseQuery })
    expect(events.map((ev) => ev.name.name)).toEqual([
      "CourseCreated",
      "StudentSubscribed",
      "EnrollmentClosed",
    ])
  })
})
