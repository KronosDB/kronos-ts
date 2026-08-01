/**
 * Full-stack E2E integration test:
 * Axon Server (event store + command/query distribution)
 * + In-memory projections via a tracking processor
 * + HTTP layer via plain Express (no framework extension)
 * + Full CQRS flow validation
 *
 * Demonstrates the decoupled HTTP wiring: Kronos has no knowledge of the web
 * framework. The app is started first, then HTTP routes are registered against
 * the resulting RunningApp gateways and the server begins listening. Routes are
 * defined alongside their domain slice (registerCourseHttp) so a slice bundles
 * both its Kronos handlers and its HTTP surface.
 *
 * Uses testcontainers for Axon Server.
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import express, { type Express } from "express"
import type { Server } from "node:http"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  query,
  commandHandler,
  eventHandler,
  queryHandler,
  EventCriteria,
  trackingProcessor,
  emitUpdate,
  send,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { type EventStore } from "@kronos-ts/eventsourcing"
import { kronos, type App, type RunningApp } from "@kronos-ts/app"
import { axonServer } from "@kronos-ts/axon-server"

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
  tags: (p) => [tag("courseId", p.courseId)],
})

const StudentSubscribed = event({
  name: qn("e2e", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const CloseEnrollment = command({
  name: qn("e2e", "CloseEnrollment"),
  payload: z.object({ courseId: z.string() }),
  routingKey: "courseId",
})

const EnrollmentClosed = event({
  name: qn("e2e", "EnrollmentClosed"),
  payload: z.object({ courseId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[]; closed: boolean }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0, enrolled: [], closed: false }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: (on) => [
    on(CourseCreated, (s, { payload: e }) => ({ ...s, created: true, name: e.name, capacity: e.capacity })),
    on(StudentSubscribed, (s, { payload: e }) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })),
    on(EnrollmentClosed, (s) => ({ ...s, closed: true })),
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
// CloseEnrollment command via send(). The command runs in its own fresh
// UnitOfWork per the AF5-aligned model.

const closeEnrollment = commandHandler(CloseEnrollment, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created || course.closed) return
  ctx.append(EnrollmentClosed, { courseId: cmd.courseId })
})

const closeEnrollmentWhenFull = eventHandler(StudentSubscribed, async ({ payload: e }, ctx) => {
  const course = await ctx.load(Course, { courseId: e.courseId })
  if (course.created && !course.closed && course.enrolled.length >= course.capacity) {
    await send(CloseEnrollment, { courseId: e.courseId })
  }
})

// -- Projection --
type CourseView = { courseId: string; name: string; capacity: number; enrolledCount: number }
const courseViews = new Map<string, CourseView>()

const onCourseCreated = eventHandler(CourseCreated, async ({ payload: e }, ctx) => {
  courseViews.set(e.courseId, { courseId: e.courseId, name: e.name, capacity: e.capacity, enrolledCount: 0 })
  emitUpdate(GetCourse, (q) => q.courseId === e.courseId, courseViews.get(e.courseId))
})

const onStudentSubscribed = eventHandler(StudentSubscribed, async ({ payload: e }, ctx) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.enrolledCount++
    emitUpdate(GetCourse, (q) => q.courseId === e.courseId, view)
  }
})

const getCourse = queryHandler(GetCourse, async ({ payload: q }) => {
  const view = courseViews.get(q.courseId)
  if (!view) throw new Error("Course not found")
  return view
})

// -- HTTP surface for the Course slice --
// Co-located with the slice's domain. Plain Express: no framework extension,
// no deferred decorator. Receives the already-started RunningApp and closes
// over its gateways. Composes the same way Kronos slices do — call it for each
// slice on the shared Express instance before listen().
function registerCourseHttp(http: Express, k: RunningApp): void {
  http.post("/courses", async (req, res) => {
    try {
      await k.commandGateway.send(CreateCourse, req.body)
      res.status(201).end()
    } catch (err) {
      res.status(400).json({ error: (err as Error).message })
    }
  })
  http.get("/courses/:courseId", async (req, res) => {
    try {
      const view = await k.queryGateway.query(GetCourse, { courseId: req.params.courseId })
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
      if (contexts.some(c => c.context === "default")) return
    } catch { /* not ready */ }
    await new Promise(r => setTimeout(r, 500))
  }
  throw new Error("Timed out waiting for default context")
}

// ============================================================================
// Tests
// ============================================================================

// Wired against the native Axon Server extension (D-95):
//   .states(...).commands(...).queries(...).processors(...)
//   .use(axonServer({...}))  // native (app: App) => void
// axonServer() populates eventStore / snapshotStore / commandBus / queryBus
// typed slots via app.set(...) and runs connect/processors/stop hooks under the
// @kronos-ts/common resilience helper.
//
// The HTTP layer is plain Express, wired AFTER start(): no framework extension.
// Kronos starts, then registerCourseHttp() binds routes to the RunningApp
// gateways, then the server listens. This is the decoupled replacement for the
// removed withExpress/withFastify/withHono extensions.
describe("E2E: Axon Server full stack", () => {
  let container: StartedTestContainer
  let app: RunningApp
  let capturedEventStore: EventStore | undefined
  let httpServer: Server
  let baseUrl: string
  let axonHost: string
  let axonGrpcPort: number

  beforeAll(async () => {
    courseViews.clear()
    capturedEventStore = undefined

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
    await new Promise(r => setTimeout(r, 3000))

    // Capture eventStore via probe decorator (native RunningApp has no eventStore field).
    const captureExtension = (kronosApp: App) => {
      kronosApp.decorate("eventStore", (inner) => {
        capturedEventStore = inner
        return inner
      })
    }

    app = await kronos({ quiet: true })
      .states(Course)
      .commands(createCourse, subscribeStudent)
      .queries(getCourse)
      .processors(
        trackingProcessor("course-projection")
          .eventHandlers(onCourseCreated, onStudentSubscribed)
          .build(),
      )
      .use(captureExtension)
      .use(axonServer({                                         // native (app: App) => void
        componentName: "e2e-full-stack",
        host,
        port: grpcPort,
        context: "default",
      }))
      .start()

    // HTTP layer: plain Express, wired after start() against the RunningApp.
    const expressApp: Express = express()
    expressApp.use(express.json())
    registerCourseHttp(expressApp, app)
    // Random ephemeral port to avoid collisions with other tests in the file.
    const httpServerPort = 30_000 + Math.floor(Math.random() * 5000)
    httpServer = await new Promise<Server>((resolve) => {
      const server = expressApp.listen(httpServerPort, () => resolve(server))
    })
    baseUrl = `http://127.0.0.1:${httpServerPort}`

    await new Promise(r => setTimeout(r, 2000))
  }, 120_000)

  afterAll(async () => {
    await new Promise<void>((resolve) => {
      if (!httpServer) return resolve()
      httpServer.close(() => resolve())
    })
    await app?.stop()
    await container?.stop()
  })

  function eventStore(): EventStore {
    if (!capturedEventStore) throw new Error("eventStore capture failed — start() did not run probe decorator")
    return capturedEventStore
  }

  async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 30000): Promise<void> {
    const start = Date.now()
    while (Date.now() - start < timeoutMs) {
      if (await check()) return
      await new Promise(r => setTimeout(r, 100))
    }
    throw new Error("Timed out waiting for condition")
  }

  it("command → event → Axon Server → source back → query", async () => {
    // when — dispatch command through Axon Server
    await app.commandGateway.send(CreateCourse, {
      courseId: "e2e-101",
      name: "Full Stack Course",
      capacity: 30,
    })

    // then — events persisted in Axon Server
    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", "e2e-101")),
    })
    expect(events.length).toBe(1)
    expect((events[0]!.payload as any).name).toBe("Full Stack Course")

    // and — second command sources state from first event
    await app.commandGateway.send(SubscribeStudent, {
      courseId: "e2e-101",
      studentId: "stu-1",
    })

    // and — re-source shows both events
    const { events: allEvents } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", "e2e-101")),
    })
    expect(allEvents.length).toBe(2)
  }, 60_000)

  it("events persist in Axon Server", async () => {
    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", "e2e-101")),
    })
    // 2 events: CourseCreated + StudentSubscribed (from previous test)
    expect(events.length).toBe(2)
    expect((events[0]!.payload as any).name).toBe("Full Stack Course")
  }, 60_000)

  it("business rules enforced through event-sourced state", async () => {
    await expect(
      app.commandGateway.send(CreateCourse, { courseId: "e2e-101", name: "Duplicate", capacity: 5 }),
    ).rejects.toThrow()
  }, 60_000)

  it("enrollment with capacity enforcement via event-sourced state", async () => {
    await app.commandGateway.send(CreateCourse, {
      courseId: "e2e-cap",
      name: "Small Class",
      capacity: 1,
    })

    await app.commandGateway.send(SubscribeStudent, {
      courseId: "e2e-cap",
      studentId: "stu-1",
    })

    // Course is full — state sourced from Axon Server events
    await expect(
      app.commandGateway.send(SubscribeStudent, { courseId: "e2e-cap", studentId: "stu-2" }),
    ).rejects.toThrow()

    // Verify events in store
    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", "e2e-cap")),
    })
    expect(events.length).toBe(2) // CourseCreated + StudentSubscribed
  }, 60_000)

  it("multiple aggregates sourced independently", async () => {
    await app.commandGateway.send(CreateCourse, { courseId: "e2e-a", name: "Course A", capacity: 10 })
    await app.commandGateway.send(CreateCourse, { courseId: "e2e-b", name: "Course B", capacity: 20 })

    const eventsA = await eventStore().source({ criteria: EventCriteria.havingTags(tag("courseId", "e2e-a")) })
    const eventsB = await eventStore().source({ criteria: EventCriteria.havingTags(tag("courseId", "e2e-b")) })

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
    const result = await app.queryGateway.query(GetCourse, { courseId: "e2e-a" })
    expect((result as CourseView).name).toBe("Course A")
  }, 60_000)

  it("stateful automation — an event handler sends a command in its own UoW", async () => {
    // A dedicated app isolates the automation processor from the shared app's
    // assertions; it connects to the same Axon Server instance.
    let autoEventStore: EventStore | undefined
    const autoApp = await kronos({ quiet: true })
      .states(Course)
      .commands(createCourse, subscribeStudent, closeEnrollment)
      .processors(
        trackingProcessor("axonserver-enrollment-automation")
          .eventHandlers(closeEnrollmentWhenFull)
          .build(),
      )
      .use((a: App) => {
        a.decorate("eventStore", (inner) => {
          autoEventStore = inner
          return inner
        })
      })
      .use(axonServer({
        componentName: "e2e-automation",
        host: axonHost,
        port: axonGrpcPort,
        context: "default",
      }))
      .start()
    await new Promise(r => setTimeout(r, 2000))

    try {
      const courseId = "e2e-auto-cap"
      await autoApp.commandGateway.send(CreateCourse, { courseId, name: "One Seat", capacity: 1 })
      await autoApp.commandGateway.send(SubscribeStudent, { courseId, studentId: "stu-1" })

      // The automation sources the now-full course and dispatches
      // CloseEnrollment; its handler appends EnrollmentClosed in its own UoW.
      const criteria = EventCriteria.havingTags(tag("courseId", courseId))
      await waitFor(async () => {
        const { events } = await autoEventStore!.source({ criteria })
        return events.some((ev) => ev.name.name === "EnrollmentClosed")
      }, 30000)

      const { events } = await autoEventStore!.source({ criteria })
      expect(events.map((ev) => ev.name.name)).toEqual([
        "CourseCreated",
        "StudentSubscribed",
        "EnrollmentClosed",
      ])
    } finally {
      await autoApp.stop()
    }
  }, 90_000)

  it("HTTP POST → command → event → Axon Server, HTTP GET → projection", async () => {
    // when — create a course over HTTP (plain Express → commandGateway)
    const created = await fetch(`${baseUrl}/courses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ courseId: "e2e-http", name: "HTTP Course", capacity: 12 }),
    })
    expect(created.status).toBe(201)

    // then — event persisted in Axon Server
    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", "e2e-http")),
    })
    expect(events.length).toBe(1)

    // and — projection reachable over HTTP GET once the tracking processor catches up
    await waitFor(() => courseViews.has("e2e-http"), 30000)
    const fetched = await fetch(`${baseUrl}/courses/e2e-http`)
    expect(fetched.status).toBe(200)
    expect((await fetched.json() as CourseView).name).toBe("HTTP Course")
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
})
