/**
 * Full-stack E2E integration test:
 * Axon Server (event store + command/query distribution)
 * + In-memory projections via subscribing processor
 * + Full CQRS flow validation
 *
 * Uses testcontainers for Axon Server.
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import express from "express"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  query,
  on,
  commandHandler,
  eventHandler,
  queryHandlers,
  EventCriteria,
  trackingProcessor,
  emitUpdate,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { type EventStore, load, append } from "@kronos-ts/eventsourcing"
import { kronos, type App, type RunningApp } from "@kronos-ts/core"
import { withExpress } from "@kronos-ts/extensions/express"
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

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[] }

const CourseEntity = eventSourcedEntity({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0, enrolled: [] }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: [
    on(CourseCreated, (s: CourseState, e) => ({ ...s, created: true, name: e.name, capacity: e.capacity })),
    on(StudentSubscribed, (s: CourseState, e) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })),
  ],
})

const createCourse = commandHandler(CreateCourse, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
  append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// -- Projection --
type CourseView = { courseId: string; name: string; capacity: number; enrolledCount: number }
const courseViews = new Map<string, CourseView>()

const onCourseCreated = eventHandler(CourseCreated, async (e, _metadata) => {
  courseViews.set(e.courseId, { courseId: e.courseId, name: e.name, capacity: e.capacity, enrolledCount: 0 })
  emitUpdate(GetCourse, (q) => q.courseId === e.courseId, courseViews.get(e.courseId))
})

const onStudentSubscribed = eventHandler(StudentSubscribed, async (e, _metadata) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.enrolledCount++
    emitUpdate(GetCourse, (q) => q.courseId === e.courseId, view)
  }
})

const courseQueries = queryHandlers({
  name: "course-queries",
  handlers: [
    on(GetCourse, async (q) => {
      const view = courseViews.get(q.courseId)
      if (!view) throw new Error("Course not found")
      return view
    }),
  ],
})

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

// Plan 09-04 — wired against the native Axon Server extension (D-95):
//   .entities(...).commands(...).queries(...).processors(...)
//   .use(withExpress(...))   // Plan 01 native HTTP Extension
//   .use(axonServer({...}))  // Plan 09-04 native (app: App) => void
// Express extension validates Plan 01's HTTP-extension shape end-to-end
// alongside the native Axon Server extension. axonServer() populates
// eventStore / snapshotStore / commandBus / queryBus typed slots via app.set(...)
// and runs connect/processors/stop hooks under the @kronos-ts/common
// resilience helper.
describe("E2E: Axon Server full stack", () => {
  let container: StartedTestContainer
  let app: RunningApp
  let capturedEventStore: EventStore | undefined
  let httpServerPort: number

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

    await initClusterWithDcb(host, httpPort)
    // Extra delay for DCB event store stream endpoint initialization
    await new Promise(r => setTimeout(r, 3000))

    // Express + withExpress (Plan 01 native shape)
    const expressApp = express()
    // Random ephemeral port to avoid collisions with other tests in the file
    httpServerPort = 30_000 + Math.floor(Math.random() * 5000)

    // Capture eventStore via probe decorator (native RunningApp has no eventStore field).
    const captureExtension = (kronosApp: App) => {
      kronosApp.decorate("eventStore", (inner) => {
        capturedEventStore = inner
        return inner
      })
    }

    app = await kronos({ quiet: true })
      .entities(CourseEntity)
      .commands(createCourse, subscribeStudent)
      .queries(courseQueries)
      .processors(
        trackingProcessor("course-projection")
          .eventHandlers(onCourseCreated, onStudentSubscribed)
          .build(),
      )
      .use(captureExtension)
      .use(withExpress(expressApp, { port: httpServerPort })) // Plan 01 HTTP wiring
      .use(axonServer({                                         // Plan 09-04 native shape
        componentName: "e2e-full-stack",
        host,
        port: grpcPort,
        context: "default",
      }))
      .start()

    await new Promise(r => setTimeout(r, 2000))
  }, 120_000)

  afterAll(async () => {
    await app?.stop()
    await container?.stop()
  })

  function eventStore(): EventStore {
    if (!capturedEventStore) throw new Error("eventStore capture failed — start() did not run probe decorator")
    return capturedEventStore
  }

  async function waitFor(check: () => boolean, timeoutMs = 30000): Promise<void> {
    const start = Date.now()
    while (Date.now() - start < timeoutMs) {
      if (check()) return
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
})
