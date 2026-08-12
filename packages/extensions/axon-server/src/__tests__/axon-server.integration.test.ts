/**
 * Axon Server integration test — exercises the `axonServer(...)` backend
 * factory against a real Axon Server container.
 *
 * Written on the functional composition shape: `axonServer()` connects, its
 * components are spread into `createApp`, and `start()` waits for the server's
 * routing tables. Requires docker (testcontainers).
 *
 * Coverage parity with the original deferred suite:
 *   1. dispatches a command through Axon Server and sources state
 *   2. enforces business rules from event-sourced state
 *   3. sources events by tag criteria
 *   4. enforces capacity limits (multi-event entity load)
 *   5. prevents duplicate enrollment
 *   6. snapshot store roundtrip via createAxonServerSnapshotStore
 *
 * Reconnect / failover scenarios are covered by the integrationtests-suite
 * e2e (e2e-axonserver-http.integration.test.ts) where the full HTTP stack
 * is exercised — kept out of this per-package test to avoid duplicating the
 * testcontainers boot cost.
 */
import { describe, it, expect, beforeAll, afterAll } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  commandHandler,
  EventCriteria,
  jsonSerializer,
  runInNewUoW,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import {
  type EventStore,
  type Snapshot
} from "@kronos-ts/eventsourcing"
import { createApp, inMemoryComponents, module, type App } from "@kronos-ts/app"
import { axonServer, type AxonServerBackend } from "../axon-server.js"
import { axonServerControlPlane, type ManagedEventProcessor } from "../control-plane.js"
import {
  connectToAxonServer,
  type AxonServerConnection,
} from "../connection.js"
import { createAxonServerSnapshotStore } from "../axon-server-snapshot-store.js"

// ============================================================================
// Domain — Course / Student enrollment (mirror of the integrationtests e2e)
// ============================================================================

const CreateCourse = command({
  name: qn("axon-it", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  routingKey: "courseId",
})

const EnrollStudent = command({
  name: qn("axon-it", "EnrollStudent"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  routingKey: "courseId",
})

const CourseCreated = event({
  name: qn("axon-it", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const StudentEnrolled = event({
  name: qn("axon-it", "StudentEnrolled"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[] }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0, enrolled: [] }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: (on) => [
    on(CourseCreated, (s, { payload: e }) => ({ ...s, created: true, name: e.name, capacity: e.capacity })),
    on(StudentEnrolled, (s, { payload: e }) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })),
  ],
})

const handleCreateCourse = commandHandler(CreateCourse, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  ctx.append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const handleEnrollStudent = commandHandler(EnrollStudent, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
  ctx.append(StudentEnrolled, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// ============================================================================
// Axon Server bring-up helpers
// ============================================================================

async function initClusterWithDcb(host: string, httpPort: number): Promise<void> {
  await fetch(`http://${host}:${httpPort}/v2/cluster/init?dcb=true`, { method: "POST" })
  const start = Date.now()
  while (Date.now() - start < 15_000) {
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

describe("Axon Server integration — axonServer() backend", () => {
  let container: StartedTestContainer
  let app: App
  let axon: AxonServerBackend
  let host: string
  let grpcPort: number

  beforeAll(async () => {
    container = await new GenericContainer("axoniq/axonserver:2025.2.5")
      .withExposedPorts(8024, 8124)
      .withEnvironment({ AXONIQ_AXONSERVER_DEVMODE_ENABLED: "true" })
      .withWaitStrategy(Wait.forHttp("/actuator/health", 8024).forStatusCode(200))
      .start()

    host = container.getHost()
    grpcPort = container.getMappedPort(8124)
    const httpPort = container.getMappedPort(8024)

    await initClusterWithDcb(host, httpPort)
    // Extra delay for DCB event store stream endpoint initialization.
    await new Promise((r) => setTimeout(r, 3000))

    // The backend connects eagerly; its components are the app's. No probe
    // decorator is needed to see the event store — it is a property.
    const serializer = jsonSerializer()
    axon = await axonServer({
      componentName: "axon-it-suite",
      host,
      port: grpcPort,
      context: "default",
      serializer,
      unitOfWorkFactory: runInNewUoW,
    })

    app = createApp({
      components: {
        ...inMemoryComponents({ serializer, unitOfWorkFactory: runInNewUoW }),
        ...axon.components,
      },
      modules: [module("axon-it", Course, handleCreateCourse, handleEnrollStudent)],
    })

    // Handlers are subscribed by now — wait until the server can route to them.
    // NOTE: this is the DATA-PATH readiness barrier only. No platform (control)
    // stream is started anywhere in this suite — every command/query/event test
    // below runs with `axon.platform.connected === false`, which is what proves
    // remote administration is genuinely orthogonal to the data path.
    await axon.start()
  }, 120_000)

  afterAll(async () => {
    await app?.stop()
    await axon?.close()
    await container?.stop()
  })

  function eventStore(): EventStore {
    return axon.components.eventStore
  }

  it("dispatches a command through Axon Server and sources state", async () => {
    await app.commandGateway.send(CreateCourse, {
      courseId: "course-1",
      name: "Distributed Systems",
      capacity: 30,
    })

    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", "course-1")),
    })
    expect(events.length).toBe(1)
    expect((events[0]!.payload as any).name).toBe("Distributed Systems")
  }, 60_000)

  it("enforces business rules from event-sourced state (duplicate course rejected)", async () => {
    await expect(
      app.commandGateway.send(CreateCourse, {
        courseId: "course-1",
        name: "Duplicate",
        capacity: 5,
      }),
    ).rejects.toThrow()
  }, 60_000)

  it("sources events by tag criteria", async () => {
    await app.commandGateway.send(CreateCourse, {
      courseId: "course-tag-A",
      name: "Course A",
      capacity: 10,
    })
    await app.commandGateway.send(CreateCourse, {
      courseId: "course-tag-B",
      name: "Course B",
      capacity: 20,
    })

    const eventsA = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", "course-tag-A")),
    })
    const eventsB = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", "course-tag-B")),
    })

    expect(eventsA.events.length).toBe(1)
    expect(eventsB.events.length).toBe(1)
    expect((eventsA.events[0]!.payload as any).name).toBe("Course A")
    expect((eventsB.events[0]!.payload as any).name).toBe("Course B")
  }, 60_000)

  it("enforces capacity limits via multi-event entity load", async () => {
    await app.commandGateway.send(CreateCourse, {
      courseId: "course-cap",
      name: "Tiny Class",
      capacity: 1,
    })
    await app.commandGateway.send(EnrollStudent, {
      courseId: "course-cap",
      studentId: "student-A",
    })

    // Course is full — sources both events, sees enrolled.length >= capacity.
    await expect(
      app.commandGateway.send(EnrollStudent, {
        courseId: "course-cap",
        studentId: "student-B",
      }),
    ).rejects.toThrow()

    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", "course-cap")),
    })
    expect(events.length).toBe(2) // CourseCreated + StudentEnrolled
  }, 60_000)

  it("prevents duplicate student enrollment", async () => {
    await app.commandGateway.send(CreateCourse, {
      courseId: "course-dup",
      name: "Course With Limits",
      capacity: 5,
    })
    await app.commandGateway.send(EnrollStudent, {
      courseId: "course-dup",
      studentId: "student-X",
    })

    await expect(
      app.commandGateway.send(EnrollStudent, {
        courseId: "course-dup",
        studentId: "student-X",
      }),
    ).rejects.toThrow()
  }, 60_000)

  it("start() leaves the platform stream down; the control plane brings it up", async () => {
    // The whole point of the extraction: a service that is not remotely
    // administered never opens the control stream, yet everything above worked.
    expect(axon.platform.connected).toBe(false)

    const proc: ManagedEventProcessor = { name: "course-projection", running: true, position: 3n }
    const control = await axonServerControlPlane(axon.platform, [proc])
    try {
      expect(axon.platform.connected).toBe(true)
      expect(await axon.platform.subscriptionsAcked()).toBe(true)
      expect(control.processors.get("course-projection")).toBe(proc)
    } finally {
      await control.close()
    }
    expect(axon.platform.connected).toBe(false)
  }, 60_000)

  it("snapshot store roundtrip via createAxonServerSnapshotStore", async () => {
    // Use a dedicated direct connection so the snapshot test does not depend
    // on the app wiring at all — this exercises the same snapshot-store factory
    // contract that `axonServer()` calls into for `components.snapshotStore`.
    const directConnection: AxonServerConnection = connectToAxonServer({
      componentName: "axon-it-snapshot",
      host,
      port: grpcPort,
      context: "default",
    })
    try {
      const snapshotStore = createAxonServerSnapshotStore(directConnection, jsonSerializer())
      const snapshot: Snapshot = {
        position: 42n,
        payload: { name: "Snapshotted Course", capacity: 17, enrolled: ["alice", "bob"] },
        timestamp: Date.now(),
        metadata: {},
      }

      await snapshotStore.store("Course", { courseId: "course-snap" }, snapshot)

      const loaded = await snapshotStore.load("Course", { courseId: "course-snap" })
      expect(loaded).toBeDefined()
      expect(loaded!.position).toBe(42n)
      expect((loaded!.payload as any).name).toBe("Snapshotted Course")
      expect((loaded!.payload as any).capacity).toBe(17)
      expect((loaded!.payload as any).enrolled).toEqual(["alice", "bob"])
    } finally {
      directConnection.close()
    }
  }, 60_000)
})
