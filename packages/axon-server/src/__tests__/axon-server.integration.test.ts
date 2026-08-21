/**
 * Axon Server integration test — exercises the `axonServerConnection(...)`
 * family against a real Axon Server container.
 *
 * Written on the functional composition shape: the connection is the shared
 * resource, the stores and buses are plain functions over it, and `start()`
 * waits for the server's routing tables. Requires docker (testcontainers).
 *
 * Coverage parity with the original deferred suite:
 *   1. dispatches a command through Axon Server and sources state
 *   2. enforces business rules from event-sourced state
 *   3. sources events by tag query
 *   4. enforces capacity limits (multi-event entity load)
 *   5. prevents duplicate enrollment
 *   6. snapshot roundtrip via axonServerSnapshottingEventStore
 *
 * Reconnect / failover scenarios are covered by the integrationtests-suite
 * e2e (e2e-axonserver-http.integration.test.ts) where the full HTTP stack
 * is exercised — kept out of this per-package test to avoid duplicating the
 * testcontainers boot cost.
 */
import { describe, it, expect, beforeAll, afterAll } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import { z } from "zod"
import { qn } from "@kronos-ts/core"
import { command, event, commandHandler, jsonSerializer, send, unitOfWork } from "@kronos-ts/core"
import { state } from "@kronos-ts/core"
import {
  interceptingCommandBus,
  interceptingQueryBus,
  correlation,
  localCommandBus,
  localQueryBus,
  type CommandBus,
  type EventStore,
  type QueryBus,
  type Snapshot,
  type SnapshotCapableEventStore,
  snapshotIdentifier,
} from "@kronos-ts/core"
import { kronos, type App } from "@kronos-ts/core"
import { axonServerCommandBus, axonServerQueryBus } from "../axon-server.js"
import { axonServerControlPlane, type ManagedEventProcessor } from "../control-plane.js"
import { axonServerConnection, type AxonServerConnectionHandle } from "../connection.js"
import { axonServerEventStore } from "../axon-server-event-store.js"
import { axonServerSnapshottingEventStore } from "../axon-server-snapshotting-event-store.js"

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
  tags: { courseId: (p) => p.courseId },
})

const StudentEnrolled = event({
  name: qn("axon-it", "StudentEnrolled"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[] }

const Course = state({
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    () => ({ created: false, name: "", capacity: 0, enrolled: [] }) as CourseState,
    [
      CourseCreated,
      (s, { payload: e }) => ({ ...s, created: true, name: e.name, capacity: e.capacity }),
    ],
    [StudentEnrolled, (s, { payload: e }) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })],
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

describe("Axon Server integration — axonServerConnection() family", () => {
  let container: StartedTestContainer
  let app: App
  let axon: AxonServerConnectionHandle
  let eventStore: SnapshotCapableEventStore
  let commandBus: CommandBus
  let queryBus: QueryBus
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

    // The connection is the shared resource and it connects eagerly; the four
    // components are plain functions over it. The serializer is named ONCE, on
    // the connection — it is a property of this client's wire, which is what
    // leaves the stores and buses their two-argument shapes.
    axon = await axonServerConnection({
      componentName: "axon-it-suite",
      host,
      port: grpcPort,
      context: "default",
      serializer: jsonSerializer(),
      // Timeout sized for CI runners, where a JVM Axon Server can pause past
      // the 7.5s production default; the ping cadence stays default because
      // start() latches on the first heartbeat response. The timeout MECHANISM
      // has its own tight-threshold unit test — this suite tests the data path.
      platformService: { heartbeatTimeoutMs: 60_000 },
    })

    // Contexts are a per-call header, so the log and its cache ride the ONE
    // channel — and they are ONE object, because the cache is a tier on the log.
    eventStore = axonServerSnapshottingEventStore(
      axonServerEventStore(axon, "default"),
      axon,
      "default",
    )

    // The REST context listing races the DCB gRPC endpoint actually serving
    // the context — on a slow runner the gap is seconds, and a fixed sleep is
    // a coin-flip. Probe the real endpoint until it answers: readiness is the
    // thing itself working, not a proxy for it.
    const probeStart = Date.now()
    for (;;) {
      try {
        await eventStore.latestToken()
        break
      } catch (err) {
        if (!/Unknown Context/i.test(String(err)) || Date.now() - probeStart > 60_000) throw err
        await new Promise((r) => setTimeout(r, 500))
      }
    }
    // The local segment is a REAL bus: a command Axon Server routes back to us
    // runs under the unit-of-work policy chosen HERE. Interception stays
    // outside, so it covers the message on its way to the wire.
    commandBus = interceptingCommandBus(
      axonServerCommandBus(localCommandBus(unitOfWork), axon),
      correlation,
    )
    queryBus = interceptingQueryBus(axonServerQueryBus(localQueryBus(unitOfWork), axon), correlation)
    app = kronos({
      commandHandlers: [
        { ...handleCreateCourse, eventStore, commandBus, queryBus },
        { ...handleEnrollStudent, eventStore, commandBus, queryBus },
      ],
    })

    // Handlers are subscribed by now — wait until the server can route to them.
    // NOTE: this is the DATA PATH only. `start()` arms heartbeat-driven
    // reconnect detection on the platform stream and waits for bus routability;
    // it arms NO remote administration. No `axonServerControlPlane` is built
    // until the dedicated test below, so every command/query/event test in
    // between runs with instruction routing and processor status reporting
    // switched off — which is what proves remote administration is genuinely
    // orthogonal to the data path.
    await axon.start()
  }, 120_000)

  afterAll(async () => {
    await app?.stop()
    await axon?.close()
    await container?.stop()
  })

  it("dispatches a command through Axon Server and sources state", async () => {
    await send(commandBus, CreateCourse, {
      courseId: "course-1",
      name: "Distributed Systems",
      capacity: 30,
    })

    const { events } = await eventStore.source({
      query: { tags: { courseId: "course-1" } },
    })
    expect(events.length).toBe(1)
    expect((events[0]!.payload as any).name).toBe("Distributed Systems")
  }, 60_000)

  it("enforces business rules from event-sourced state (duplicate course rejected)", async () => {
    await expect(
      send(commandBus, CreateCourse, {
        courseId: "course-1",
        name: "Duplicate",
        capacity: 5,
      }),
    ).rejects.toThrow()
  }, 60_000)

  it("sources events by tag query", async () => {
    await send(commandBus, CreateCourse, {
      courseId: "course-tag-A",
      name: "Course A",
      capacity: 10,
    })
    await send(commandBus, CreateCourse, {
      courseId: "course-tag-B",
      name: "Course B",
      capacity: 20,
    })

    const eventsA = await eventStore.source({
      query: { tags: { courseId: "course-tag-A" } },
    })
    const eventsB = await eventStore.source({
      query: { tags: { courseId: "course-tag-B" } },
    })

    expect(eventsA.events.length).toBe(1)
    expect(eventsB.events.length).toBe(1)
    expect((eventsA.events[0]!.payload as any).name).toBe("Course A")
    expect((eventsB.events[0]!.payload as any).name).toBe("Course B")
  }, 60_000)

  it("enforces capacity limits via multi-event entity load", async () => {
    await send(commandBus, CreateCourse, {
      courseId: "course-cap",
      name: "Tiny Class",
      capacity: 1,
    })
    await send(commandBus, EnrollStudent, {
      courseId: "course-cap",
      studentId: "student-A",
    })

    // Course is full — sources both events, sees enrolled.length >= capacity.
    await expect(
      send(commandBus, EnrollStudent, {
        courseId: "course-cap",
        studentId: "student-B",
      }),
    ).rejects.toThrow()

    const { events } = await eventStore.source({
      query: { tags: { courseId: "course-cap" } },
    })
    expect(events.length).toBe(2) // CourseCreated + StudentEnrolled
  }, 60_000)

  it("prevents duplicate student enrollment", async () => {
    await send(commandBus, CreateCourse, {
      courseId: "course-dup",
      name: "Course With Limits",
      capacity: 5,
    })
    await send(commandBus, EnrollStudent, {
      courseId: "course-dup",
      studentId: "student-X",
    })

    await expect(
      send(commandBus, EnrollStudent, {
        courseId: "course-dup",
        studentId: "student-X",
      }),
    ).rejects.toThrow()
  }, 60_000)

  it("start() arms the data path's platform stream; the control plane layers admin on top", async () => {
    // The data path owns reconnect detection, so `axon.start()` has the platform
    // stream up already — a service nobody administers still notices a dead
    // channel. What it does NOT have is instruction routing or status
    // reporting; those arrive with the control plane, on this same stream.
    expect(axon.platform.connected).toBe(true)

    const proc: ManagedEventProcessor = { name: "course-projection", running: true, position: 3n }
    const control = await axonServerControlPlane(axon, [proc])
    try {
      // start() over an already-armed stream is a no-op for the stream itself.
      expect(axon.platform.connected).toBe(true)
      expect(await axon.platform.subscriptionsAcked()).toBe(true)
      expect(control.processors.get("course-projection")).toBe(proc)
    } finally {
      await control.close()
    }
    // close() tears down the SHARED stream — see the note on PlatformConnection.stop().
    expect(axon.platform.connected).toBe(false)
  }, 60_000)

  it("snapshot roundtrip via axonServerSnapshottingEventStore", async () => {
    // Use a dedicated connection so the snapshot test does not depend on the
    // app wiring at all — this exercises exactly the wrapper the suite's own
    // `axonServerSnapshottingEventStore(…)` builds.
    const direct = await axonServerConnection({
      componentName: "axon-it-snapshot",
      host,
      port: grpcPort,
      context: "default",
      serializer: jsonSerializer(),
      platformService: { heartbeatTimeoutMs: 60_000 },
    })
    try {
      const capable = axonServerSnapshottingEventStore(
        axonServerEventStore(direct, "default"),
        direct,
        "default",
      )
      /** What is filed under `key` — read by ASKING a read for it. */
      const readCached = async (k: string) =>
        (await capable.source({
          query: { tags: { courseId: "no-such-course" } },
          snapshot: { key: k },
        })).snapshot
      // ONE opaque key. `state()` would compose this same string; here the test
      // writes it directly, which is exactly what the raw layer does.
      const key = `course-v1:${snapshotIdentifier({ courseId: "course-snap" })}`
      const snapshot: Snapshot = {
        position: 42n,
        state: { name: "Snapshotted Course", capacity: 17, enrolled: ["alice", "bob"] },
      }

      await capable.storeSnapshot(key, snapshot)

      const loaded = await readCached(key)
      expect(loaded).toBeDefined()
      expect(loaded!.position).toBe(42n)
      expect((loaded!.state as any).name).toBe("Snapshotted Course")
      expect((loaded!.state as any).capacity).toBe(17)
      expect((loaded!.state as any).enrolled).toEqual(["alice", "bob"])

      // A miss is `undefined`, not a throw — the mechanism relies on it.
      expect(await readCached("course-v1:never-written")).toBeUndefined()
    } finally {
      await direct.close()
    }
  }, 60_000)
})
