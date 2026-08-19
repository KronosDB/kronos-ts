/**
 * Full-stack E2E integration test:
 * Axon Server (event store + command/query distribution)
 * + In-memory projections via an event processor
 * + HTTP layer via plain Express (no framework extension)
 * + Full CQRS flow validation
 *
 * Demonstrates the decoupled HTTP wiring: Kronos has no knowledge of the web
 * framework. The app is built first, then HTTP routes are registered against
 * the app's buses, and the server begins listening. Routes are defined
 * alongside their domain slice (registerCourseHttp) so a slice bundles both
 * its Kronos handlers and its HTTP surface.
 *
 * Uses testcontainers for Axon Server.
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import express, { type Express } from "express"
import type { Server } from "node:http"
import { z } from "zod"
import { qn, send, query } from "@kronos-ts/core"
import {
  jsonSerializer,
  command,
  event,
  commandHandler,
  eventHandler,
  queryHandler,
  eventProcessor,
  type EventProcessor,
  type CommandHandlerDefinition,
  type QueryHandlerDefinition,
  type EventHandlerDefinition,
  inMemoryTokenStore,
  type TokenStore,
} from "@kronos-ts/core"
import { state, type StateModule } from "@kronos-ts/core"
import { type EventStore, type SnapshotStore, afterEvents } from "@kronos-ts/core"
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
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork): {
  commandBus: CommandBus
  queryBus: QueryBus
} {
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
  /** Durable name for any bare event-handler entries in this call. */
  processorName?: string
}

/**
 * Attach one site to a flat list of entries — the composition root's job,
 * replacing the old `module(name, stores, ...handlers)` — and sort them into
 * the four fields `kronos` now takes. Honours the `[state, options]` tuple
 * shape. Spread the result straight into `kronos({ ...sitedOn(site, ...) })`.
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
      commandHandlers.push({
        ...(item as object),
        ...handlerSite,
        commandBus,
        queryBus,
      } as CommandHandlerEntry)
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
      eventHandlers.push({
        ...(item as object),
        commandBus,
        queryBus,
        processor,
      } as EventHandlerEntry)
    }
  }
  return { states, commandHandlers, queryHandlers, eventHandlers }
}

import {
  axonServerConnection,
  axonServerCommandBus,
  axonServerQueryBus,
  axonServerEventStore,
  axonServerSnapshotStore,
  axonServerControlPlane,
  type AxonServerConnectionHandle,
} from "@kronos-ts/axon-server"

// ============================================================================
// Domain
// ============================================================================

const CreateCourse = command({
  name: qn("e2e", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  routingKey: "courseId",
})

const SubscribeStudent = command({
  name: qn("e2e", "SubscribeStudent"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  routingKey: "courseId",
})

const GetCourse = query({
  name: qn("e2e", "GetCourse"),
  payload: z.object({ courseId: z.string() }),
})

const CourseCreated = event({
  name: qn("e2e", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const StudentSubscribed = event({
  name: qn("e2e", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

const CloseEnrollment = command({
  name: qn("e2e", "CloseEnrollment"),
  payload: z.object({ courseId: z.string() }),
  routingKey: "courseId",
})

const EnrollmentClosed = event({
  name: qn("e2e", "EnrollmentClosed"),
  payload: z.object({ courseId: z.string() }),
  tags: { courseId: (p) => p.courseId },
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
  initial: (_id) =>
    ({ created: false, name: "", capacity: 0, enrolled: [], closed: false }) as CourseState,
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    [
      CourseCreated,
      (s, { payload: e }) => ({ ...s, created: true, name: e.name, capacity: e.capacity }),
    ],
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
  courseViews.set(e.courseId, {
    courseId: e.courseId,
    name: e.name,
    capacity: e.capacity,
    enrolledCount: 0,
  })
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

// -- HTTP surface for the Course slice --
// Co-located with the slice's domain. Plain Express: no framework extension,
// no deferred decorator. Receives the app's buses and closes over them.
// Composes the same way Kronos slices do — call it for each slice on the
// shared Express instance before listen().
function registerCourseHttp(
  http: Express,
  buses: { commandBus: CommandBus; queryBus: QueryBus },
): void {
  http.post("/courses", async (req, res) => {
    try {
      await send(buses.commandBus, CreateCourse, req.body)
      res.status(201).end()
    } catch (err) {
      res.status(400).json({ error: (err as Error).message })
    }
  })
  http.get("/courses/:courseId", async (req, res) => {
    try {
      const view = await query(buses.queryBus, GetCourse, { courseId: req.params.courseId })
      res.status(200).json(view)
    } catch {
      res.status(404).end()
    }
  })
}

// ============================================================================
// Axon Server helpers
// ============================================================================

async function initClusterWithDcb(host: string, httpPort: number): Promise<void> {
  await fetch(`http://${host}:${httpPort}/v2/cluster/init?dcb=true`, { method: "POST" })
  const start = Date.now()
  while (Date.now() - start < 15000) {
    try {
      const res = await fetch(`http://${host}:${httpPort}/v1/public/context`)
      const contexts = (await res.json()) as Array<{ context: string }>
      if (contexts.some((c) => c.context === "default")) return
    } catch {
      /* not ready */
    }
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error("Timed out waiting for default context")
}

// ============================================================================
// Tests
// ============================================================================

// Wired against the Axon Server CONNECTION and the four functions over it:
//   const axon = await axonServerConnection({ ..., serializer })
//   const eventStore = axonServerEventStore(axon, "default")
//   const commandBus = interceptingCommandBus(
//     axonServerCommandBus(axon, simpleCommandBus(unitOfWork)), lineage)
//   kronos({ states, commandHandlers, queryHandlers, eventHandlers })
//   await axon.start()                                       // data-path readiness
//   await axonServerControlPlane(axon, app.processors.values())  // opt-in remote admin
// The connection is the shared RESOURCE — one gRPC channel, the platform stream
// on it, start()/close(). The serializer lives on it because it is a property
// of this client's wire; the unit-of-work policy lives on the LOCAL bus each
// transport bus is given, so a command Axon Server routes back to us runs under
// exactly the policy this composition root chose.
//
// The HTTP layer is plain Express, wired AFTER kronos(): no framework
// extension. Kronos composes, then registerCourseHttp() binds routes to the
// app's buses, then the server listens. This is the decoupled replacement for
// the removed withExpress/withFastify/withHono extensions.
describe("E2E: Axon Server full stack", () => {
  let container: StartedTestContainer
  let app: App
  let axon: AxonServerConnectionHandle
  let control: Awaited<ReturnType<typeof axonServerControlPlane>>
  let httpServer: Server
  let baseUrl: string
  let axonHost: string
  let axonGrpcPort: number
  let buses: { commandBus: CommandBus; queryBus: QueryBus }
  let axonEventStore: EventStore
  let axonSnapshotStore: SnapshotStore

  beforeAll(async () => {
    courseViews.clear()

    container = await new GenericContainer("axoniq/axonserver:2025.2.5")
      .withExposedPorts(8024, 8124)
      .withEnvironment({ AXONIQ_AXONSERVER_DEVMODE_ENABLED: "true" })
      .withWaitStrategy(Wait.forHttp("/actuator/health", 8024).forStatusCode(200))
      .start()

    const host = container.getHost()
    const grpcPort = container.getMappedPort(8124)
    const httpPort = container.getMappedPort(8024)
    axonHost = host
    axonGrpcPort = grpcPort

    await initClusterWithDcb(host, httpPort)
    // Extra delay for DCB event store stream endpoint initialization
    await new Promise((r) => setTimeout(r, 3000))

    // The UoW runner is the LOCAL bus's, and the local bus is what the Axon
    // buses route inbound work into — so a command Axon Server sends back to
    // this process runs under exactly this policy.
    const uow = unitOfWork
    const local = inMemoryBuses(uow)

    axon = await axonServerConnection({
      componentName: "e2e-full-stack",
      host,
      port: grpcPort,
      context: "default",
      serializer: jsonSerializer(),
    })

    axonEventStore = axonServerEventStore(axon, "default")
    axonSnapshotStore = axonServerSnapshotStore(axon, "default")

    // Interception OUTSIDE the transport. `local` already carries `lineage` of
    // its own, so a server-routed command meets it twice — which is a no-op
    // past the first application, since both fields are `??` seeds.
    buses = {
      commandBus: interceptingCommandBus(axonServerCommandBus(axon, local.commandBus), lineage),
      queryBus: interceptingQueryBus(axonServerQueryBus(axon, local.queryBus), lineage),
    }

    app = kronos({
      // Per-state repository options, declared in the handler list. The
      // tuple is the state plus how ITS repository is built — the stores
      // are Axon Server's, in the "default" context.
      ...sitedOn(
        {
          eventStore: axonEventStore,
          snapshotStore: axonSnapshotStore,
          ...buses,
          processorName: "course-projection",
        },
        [Course, { snapshotPolicy: afterEvents(1) }],
        createCourse,
        subscribeStudent,
        getCourse,
        onCourseCreated,
        onStudentSubscribed,
      ),
    })

    // Platform stream + subscription-ack wait, AFTER handlers are subscribed.
    await axon.start()
    control = await axonServerControlPlane(axon, app.processors.values())

    // HTTP layer: plain Express, wired after kronos() against the app's buses.
    const expressApp: Express = express()
    expressApp.use(express.json())
    registerCourseHttp(expressApp, buses)
    // Random ephemeral port to avoid collisions with other tests in the file.
    const httpServerPort = 30_000 + Math.floor(Math.random() * 5000)
    httpServer = await new Promise<Server>((resolve) => {
      const server = expressApp.listen(httpServerPort, () => resolve(server))
    })
    baseUrl = `http://127.0.0.1:${httpServerPort}`

    await new Promise((r) => setTimeout(r, 2000))
  }, 120_000)

  afterAll(async () => {
    await control?.close()
    await new Promise<void>((resolve) => {
      if (!httpServer) return resolve()
      httpServer.close(() => resolve())
    })
    await app?.stop()
    await axon?.close()
    await container?.stop()
  })

  function eventStore(): EventStore {
    return axonEventStore
  }

  async function waitFor(
    check: () => boolean | Promise<boolean>,
    timeoutMs = 30000,
  ): Promise<void> {
    const start = Date.now()
    while (Date.now() - start < timeoutMs) {
      if (await check()) return
      await new Promise((r) => setTimeout(r, 100))
    }
    throw new Error("Timed out waiting for condition")
  }

  it("command → event → Axon Server → source back → query", async () => {
    // when — dispatch command through Axon Server
    await send(buses.commandBus, CreateCourse, {
      courseId: "e2e-101",
      name: "Full Stack Course",
      capacity: 30,
    })

    // then — events persisted in Axon Server
    const { events } = await eventStore().source({
      query: { tags: { courseId: "e2e-101" } },
    })
    expect(events.length).toBe(1)
    expect((events[0]!.payload as any).name).toBe("Full Stack Course")

    // and — second command sources state from first event
    await send(buses.commandBus, SubscribeStudent, {
      courseId: "e2e-101",
      studentId: "stu-1",
    })

    // and — re-source shows both events
    const { events: allEvents } = await eventStore().source({
      query: { tags: { courseId: "e2e-101" } },
    })
    expect(allEvents.length).toBe(2)
  }, 60_000)

  it("events persist in Axon Server", async () => {
    const { events } = await eventStore().source({
      query: { tags: { courseId: "e2e-101" } },
    })
    // 2 events: CourseCreated + StudentSubscribed (from previous test)
    expect(events.length).toBe(2)
    expect((events[0]!.payload as any).name).toBe("Full Stack Course")
  }, 60_000)

  it("business rules enforced through event-sourced state", async () => {
    await expect(
      send(buses.commandBus, CreateCourse, { courseId: "e2e-101", name: "Duplicate", capacity: 5 }),
    ).rejects.toThrow()
  }, 60_000)

  it("the declared snapshot policy writes to Axon's snapshot store", async () => {
    // The Course repository was declared as `[Course, { snapshotPolicy:
    // afterEvents(1) }]` — no hand-handler through app.stateManagers. The
    // store it writes to is the one the site named, which is Axon Server's.
    //
    // afterEvents(1) fires on a load that observes MORE THAN ONE event: the
    // duplicate-create above sourced e2e-101's two events, so it triggered
    // there. Snapshot writes are fire-and-forget, hence the poll.
    const snapshotStore = axonSnapshotStore
    await waitFor(
      async () => (await snapshotStore.load("Course", { courseId: "e2e-101" })) !== undefined,
    )

    const snapshot = await snapshotStore.load("Course", { courseId: "e2e-101" })
    expect(snapshot).toBeDefined()
    expect(snapshot!.position).toBeGreaterThan(0n)
    // The snapshot is the folded state, not the raw events.
    expect((snapshot!.payload as CourseState).name).toBe("Full Stack Course")
    expect((snapshot!.payload as CourseState).enrolled).toEqual(["stu-1"])
  }, 60_000)

  it("enrollment with capacity enforcement via event-sourced state", async () => {
    await send(buses.commandBus, CreateCourse, {
      courseId: "e2e-cap",
      name: "Small Class",
      capacity: 1,
    })

    await send(buses.commandBus, SubscribeStudent, {
      courseId: "e2e-cap",
      studentId: "stu-1",
    })

    // Course is full — state sourced from Axon Server events
    await expect(
      send(buses.commandBus, SubscribeStudent, { courseId: "e2e-cap", studentId: "stu-2" }),
    ).rejects.toThrow()

    // Verify events in store
    const { events } = await eventStore().source({
      query: { tags: { courseId: "e2e-cap" } },
    })
    expect(events.length).toBe(2) // CourseCreated + StudentSubscribed
  }, 60_000)

  it("multiple aggregates sourced independently", async () => {
    await send(buses.commandBus, CreateCourse, {
      courseId: "e2e-a",
      name: "Course A",
      capacity: 10,
    })
    await send(buses.commandBus, CreateCourse, {
      courseId: "e2e-b",
      name: "Course B",
      capacity: 20,
    })

    const eventsA = await eventStore().source({ query: { tags: { courseId: "e2e-a" } } })
    const eventsB = await eventStore().source({ query: { tags: { courseId: "e2e-b" } } })

    expect(eventsA.events.length).toBe(1)
    expect(eventsB.events.length).toBe(1)
    expect((eventsA.events[0]!.payload as any).name).toBe("Course A")
    expect((eventsB.events[0]!.payload as any).name).toBe("Course B")
  }, 60_000)

  it("tracking processor streams events from Axon Server to projection", async () => {
    // given — course created by earlier test (e2e-a)
    // when — wait for tracking processor to catch up
    await waitFor(() => courseViews.has("e2e-a"), 30000)

    // then — projection populated by tracking processor
    const view = courseViews.get("e2e-a")!
    expect(view.name).toBe("Course A")
    expect(view.capacity).toBe(10)

    // and — query gateway returns projected view
    const result = await query(buses.queryBus, GetCourse, { courseId: "e2e-a" })
    expect((result as CourseView).name).toBe("Course A")
  }, 60_000)

  it("HTTP POST → command → event → Axon Server, HTTP GET → projection", async () => {
    // when — create a course over HTTP (plain Express → send)
    const created = await fetch(`${baseUrl}/courses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ courseId: "e2e-http", name: "HTTP Course", capacity: 12 }),
    })
    expect(created.status).toBe(201)

    // then — event persisted in Axon Server
    const { events } = await eventStore().source({
      query: { tags: { courseId: "e2e-http" } },
    })
    expect(events.length).toBe(1)

    // and — projection reachable over HTTP GET once the tracking processor catches up
    await waitFor(() => courseViews.has("e2e-http"), 30000)
    const fetched = await fetch(`${baseUrl}/courses/e2e-http`)
    expect(fetched.status).toBe(200)
    expect(((await fetched.json()) as CourseView).name).toBe("HTTP Course")
  }, 60_000)

  it("HTTP surfaces a rejected command as 400", async () => {
    // duplicate create — handler throws, route maps it to 400
    const res = await fetch(`${baseUrl}/courses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ courseId: "e2e-http", name: "Duplicate", capacity: 1 }),
    })
    expect(res.status).toBe(400)
  }, 60_000)

  // NOTE: this test runs LAST on purpose. It stands up a second app that
  // registers the SAME command names on the SAME Axon context, and closing it
  // leaves Axon Server briefly routing to a socket that is gone — any test
  // after it can get a command dispatched into the closed client.
  it("stateful automation — an event handler sends a command in its own UoW", async () => {
    // A dedicated app isolates the automation processor from the shared app's
    // assertions; it connects to the same Axon Server instance.
    const autoUnitOfWork = unitOfWork
    const autoLocal = inMemoryBuses(autoUnitOfWork)
    const autoAxon = await axonServerConnection({
      componentName: "e2e-automation",
      host: axonHost,
      port: axonGrpcPort,
      context: "default",
      serializer: jsonSerializer(),
    })
    const autoBuses = {
      commandBus: interceptingCommandBus(
        axonServerCommandBus(autoAxon, autoLocal.commandBus),
        lineage,
      ),
      queryBus: interceptingQueryBus(axonServerQueryBus(autoAxon, autoLocal.queryBus), lineage),
    }
    const autoEventStore: EventStore = axonServerEventStore(autoAxon, "default")
    const autoApp = kronos({
      ...sitedOn(
        {
          eventStore: autoEventStore,
          snapshotStore: axonServerSnapshotStore(autoAxon, "default"),
          ...autoBuses,
          processorName: "axonserver-enrollment-automation",
        },
        Course,
        createCourse,
        subscribeStudent,
        closeEnrollment,
        closeEnrollmentWhenFull,
      ),
    })
    await autoAxon.start()
    const autoControl = await axonServerControlPlane(autoAxon, autoApp.processors.values())
    await new Promise((r) => setTimeout(r, 2000))

    try {
      const courseId = "e2e-auto-cap"
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
      await autoControl.close()
      await autoAxon.close()
      // This app registered the SAME command names as the shared app on the
      // same context. Axon Server drops a closed client's handlers
      // asynchronously, so give it a beat — otherwise the next command can be
      // routed to the socket that just went away.
      await new Promise((r) => setTimeout(r, 1000))
    }
  }, 90_000)
})
