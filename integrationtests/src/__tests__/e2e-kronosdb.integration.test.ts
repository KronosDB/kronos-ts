/**
 * Full-stack E2E integration test for KronosDB.
 *
 * Spins up ghcr.io/kronosdb/kronosdb:0.8.0 via testcontainers — no local
 * server needed. The image's entrypoint runs kronosdb-server, which listens
 * for gRPC on 50051 and admin on 9240.
 *
 * Tests the full CQRS/ES pipeline:
 *   command → event store → tracking processor → projection → query
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { z } from "zod"
import { qn, send, query } from "@kronos-ts/core"
import {
  jsonSerializer, command, event, commandHandler, eventHandler, queryHandler,
  eventProcessor, type EventProcessor,
  type CommandHandler, type QueryHandler, type EventHandler,
  inMemoryTokenStore, type TokenStore,
} from "@kronos-ts/core"
import { state } from "@kronos-ts/core"
import {
  type EventStore,
} from "@kronos-ts/core"
import {
  kronos,
  type App,
  type CommandHandlerEntry,
  type QueryHandlerEntry,
  type EventHandlerEntry,
  type HandlerSite,
  type Sited,
} from "@kronos-ts/core"
import {
  correlation,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  localCommandBus,
  localQueryBus,
  type UnitOfWork,
  type CommandBus,
  type QueryBus,
} from "@kronos-ts/core"
import {
  kronosDbConnection,
  kronosDbCommandBus,
  kronosDbQueryBus,
  kronosDbEventStore,
  kronosDbSnapshottingEventStore,
  type KronosDbConnectionHandle,
} from "@kronos-ts/kronosdb"

/**
 * The two things `kronos` needs that are not handlers. The UoW runner is
 * named once and handed to `localCommandBus` (which captures it at
 * construction) — writing it on an adjacent line is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork): { commandBus: CommandBus; queryBus: QueryBus } {
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(uow), correlation),
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
  | Sited<CommandHandler<any, any>>
  | Sited<QueryHandler<any, any>>
  | EventHandler<any, any>

/** What a host attaches uniformly here. There is no `stores` record any more —
 * this is the ARGUMENT LIST of a local helper, and every entry comes out
 * carrying BARE properties. `commandBus`/`queryBus` are required: `kronos`
 * takes them PER ENTRY now, not once for the whole app. */
type Site = HandlerSite & {
  commandBus: CommandBus
  queryBus: QueryBus
  tokenStore?: TokenStore
  unitOfWork?: () => UnitOfWork
  /** Durable name for any bare event-handler entries in this call. */
  processorName?: string
}

/**
 * Attach one site to a flat list of entries — the composition root's job,
 * replacing the old `module(name, stores, ...handlers)` — and sort them into
 * the three fields `kronos` now takes. States are in no list: a handler closes
 * over the one it folds. Spread the result straight into
 * `kronos({ ...sitedOn(site, ...) })`.
 */
function sitedOn(
  site: Site,
  ...items: ReadonlyArray<SitedItem>
): {
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
  const commandHandlers: CommandHandlerEntry[] = []
  const queryHandlers: QueryHandlerEntry[] = []
  const eventHandlers: EventHandlerEntry[] = []
  let processor: EventProcessor | undefined

  for (const item of items) {
    const kind = (item as { kind?: string }).kind
    if (kind === "command-handler") {
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
  return { commandHandlers, queryHandlers, eventHandlers }
}


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
  tags: { courseId: (p) => p.courseId },
})

const StudentSubscribed = event({
  name: qn("kronosdb-e2e", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

const CloseEnrollment = command({
  name: qn("kronosdb-e2e", "CloseEnrollment"),
  payload: z.object({ courseId: z.string() }),
  routingKey: "courseId",
})

const EnrollmentClosed = event({
  name: qn("kronosdb-e2e", "EnrollmentClosed"),
  payload: z.object({ courseId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[]; closed: boolean }

const Course = state({
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    () => ({ created: false, name: "", capacity: 0, enrolled: [], closed: false }) as CourseState,
    [CourseCreated, (s, { payload: e }) => ({ ...s, created: true, name: e.name, capacity: e.capacity })],
    [StudentSubscribed, (s, { payload: e }) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })],
    [EnrollmentClosed, (s) => ({ ...s, closed: true })],
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
  let backend: KronosDbConnectionHandle
  let backendEventStore: EventStore
  let kronosHost: string
  let kronosPort: number
  let buses: { commandBus: CommandBus; queryBus: QueryBus }

  beforeAll(async () => {
    courseViews.clear()

    // Spin up KronosDB. The server logs "KronosDB starting" once it binds
    // its gRPC listener; testcontainers also waits for port 50051 to accept
    // connections before returning.
    container = await new GenericContainer("ghcr.io/kronosdb/kronosdb:0.8.0")
      .withExposedPorts(50051, 9240)
      .withWaitStrategy(Wait.forHttp("/ready", 9240).forStatusCode(200))
      .start()

    kronosHost = container.getHost()
    kronosPort = container.getMappedPort(50051)

    // The app's serializer + UoW runner must be the SAME instances the
    // distributed buses use, so the runner is named once and handed to both.
    const uow = unitOfWork
    const base = inMemoryBuses(uow)

    backend = await kronosDbConnection({
      componentName: "kronosdb-e2e-test",
      host: kronosHost,
      port: kronosPort,
      context: "default",
      serializer: jsonSerializer(),
    })

    // KronosDB serves BOTH halves natively now: `AppendSnapshot` for the write
    // and `SnapshottedSource` for the fused read — one call that leads with the
    // cached fold and continues with the events after it. The native path
    // landed inside the wrapper, and this line never changed.
    backendEventStore = kronosDbSnapshottingEventStore(
      kronosDbEventStore(backend, "default"),
      backend,
      "default",
    )

    // The KronosDB buses wrap the in-memory ones rather than replacing them:
    // the local segment is a real bus now, so a command the server routes back
    // here runs through the SAME `localCommandBus(uow)` a local dispatch would
    // have used, and inherits its unit-of-work policy.
    buses = {
      commandBus: kronosDbCommandBus(base.commandBus, backend),
      queryBus: kronosDbQueryBus(base.queryBus, backend),
    }

    app = kronos({
      ...sitedOn(
        {
          eventStore: backendEventStore,
          ...buses,
          processorName: "kronosdb-course-projection",
        },
        Course,
        createCourse, subscribeStudent,
        getCourse,
        onCourseCreated, onStudentSubscribed,
      ),
    })

    // Wait until KronosDB has acked this client's handler — the handler
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
    return backendEventStore
  }

  it("command persists events to KronosDB event store", async () => {
    const courseId = id("cs-101")

    await send(buses.commandBus, CreateCourse, {
      courseId,
      name: "Full Stack Course",
      capacity: 30,
    })

    const { events } = await eventStore().source({
      query: { tags: { courseId: courseId } },
    })
    expect(events.length).toBe(1)
    expect((events[0]!.payload as any).name).toBe("Full Stack Course")
  }, 30_000)

  it("command handler sources state from KronosDB", async () => {
    const courseId = id("cs-101")

    // Second command on same aggregate — state must be sourced from first event
    await send(buses.commandBus, SubscribeStudent, {
      courseId,
      studentId: "stu-1",
    })

    const { events } = await eventStore().source({
      query: { tags: { courseId: courseId } },
    })
    expect(events.length).toBe(2)
  }, 30_000)

  it("business rules enforced via event-sourced state", async () => {
    const courseId = id("cs-101")

    // Duplicate creation should fail
    await expect(
      send(buses.commandBus, CreateCourse, { courseId, name: "Duplicate", capacity: 5 }),
    ).rejects.toThrow()
  }, 30_000)

  it("capacity enforcement across multiple commands", async () => {
    const courseId = id("cs-cap")

    await send(buses.commandBus, CreateCourse, {
      courseId,
      name: "Small Class",
      capacity: 1,
    })

    await send(buses.commandBus, SubscribeStudent, {
      courseId,
      studentId: "stu-1",
    })

    // Course is full
    await expect(
      send(buses.commandBus, SubscribeStudent, { courseId, studentId: "stu-2" }),
    ).rejects.toThrow()

    const { events } = await eventStore().source({
      query: { tags: { courseId: courseId } },
    })
    expect(events.length).toBe(2) // CourseCreated + StudentSubscribed
  }, 30_000)

  it("multiple aggregates sourced independently", async () => {
    const courseA = id("cs-a")
    const courseB = id("cs-b")

    await send(buses.commandBus, CreateCourse, { courseId: courseA, name: "Course A", capacity: 10 })
    await send(buses.commandBus, CreateCourse, { courseId: courseB, name: "Course B", capacity: 20 })

    const eventsA = await eventStore().source({ query: { tags: { courseId: courseA } } })
    const eventsB = await eventStore().source({ query: { tags: { courseId: courseB } } })

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

    const result = await query(buses.queryBus, GetCourse, { courseId: courseA })
    expect((result as CourseView).name).toBe("Course A")
  }, 30_000)

  it("stateful automation — an event handler sends a command in its own UoW", async () => {
    // A dedicated app isolates the automation processor so it cannot perturb
    // the event counts asserted by the tests above. It connects to the same
    // KronosDB instance.
    const autoUnitOfWork = unitOfWork
    const autoBase = inMemoryBuses(autoUnitOfWork)
    const autoBackend = await kronosDbConnection({
      componentName: "kronosdb-automation-test",
      host: kronosHost,
      port: kronosPort,
      context: "default",
      serializer: jsonSerializer(),
    })
    const autoBuses = {
      commandBus: kronosDbCommandBus(autoBase.commandBus, autoBackend),
      queryBus: kronosDbQueryBus(autoBase.queryBus, autoBackend),
    }
    const autoEventStore: EventStore = kronosDbSnapshottingEventStore(
      kronosDbEventStore(autoBackend, "default"),
      autoBackend,
      "default",
    )
    const autoApp = kronos({
      ...sitedOn(
        {
          eventStore: autoEventStore,
          ...autoBuses,
          processorName: "kronosdb-enrollment-automation",
        },
        Course,
        createCourse, subscribeStudent, closeEnrollment,
        closeEnrollmentWhenFull,
      ),
    })
    await autoBackend.start()
    await new Promise(r => setTimeout(r, 2000))

    try {
      const courseId = id("auto-cap")
      await send(autoBuses.commandBus, CreateCourse, { courseId, name: "One Seat", capacity: 1 })
      await send(autoBuses.commandBus, SubscribeStudent, { courseId, studentId: "stu-1" })

      // The automation sources the now-full course and dispatches
      // CloseEnrollment; its handler appends EnrollmentClosed in its own UoW.
      const courseQuery = { tags: { courseId: courseId } }
      await waitFor(async () => {
        const { events } = await autoEventStore.source({ query: courseQuery })
        return events.some((ev) => ev.name.name === "EnrollmentClosed")
      }, 30000)

      const { events } = await autoEventStore.source({ query: courseQuery })
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
