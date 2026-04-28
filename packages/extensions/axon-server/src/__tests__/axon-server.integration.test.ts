import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import { z } from "zod"
import { qn, tag, ComponentKeys } from "@kronos-ts/common"
import {
  command,
  event,
  on,
  commandHandler,
  EventCriteria,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { EventSourcingConfigurer, load, append } from "@kronos-ts/eventsourcing"
import { axonServerConfigurationEnhancer } from "../axon-server-configuration-enhancer.js"
import { createAxonServerSnapshotStore } from "../axon-server-snapshot-store.js"
import { connectToAxonServer } from "../connection.js"

// ============================================================================
// Domain
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
  tags: (p) => [tag("courseId", p.courseId)],
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const StudentSubscribed = event({
  name: qn("university", "StudentSubscribed"),
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
    on(CourseCapacityChanged, (s: CourseState, e) => ({ ...s, capacity: e.capacity })),
    on(StudentSubscribed, (s: CourseState, e) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })),
  ],
})

const createCourse = commandHandler(CreateCourse, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const changeCourseCapacity = commandHandler(ChangeCourseCapacity, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (cmd.capacity === course.capacity) return
  append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async (cmd, _metadata) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
  append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// ============================================================================
// Axon Server setup helpers
// ============================================================================

async function initClusterWithDcb(host: string, httpPort: number): Promise<void> {
  const url = `http://${host}:${httpPort}/v2/cluster/init?dcb=true`
  const response = await fetch(url, { method: "POST" })
  if (response.status !== 202 && response.status !== 200) {
    const body = await response.text()
    throw new Error(`Failed to init cluster with DCB: ${response.status} ${body}`)
  }
  await waitForContexts(host, httpPort, ["_admin", "default"])
}

async function waitForContexts(host: string, httpPort: number, expected: string[], timeoutMs = 15000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`http://${host}:${httpPort}/v1/public/context`)
      const contexts = (await res.json()) as Array<{ context: string }>
      const names = contexts.map((c) => c.context)
      if (expected.every((e) => names.includes(e))) return
    } catch { /* Server not ready */ }
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error(`Timed out waiting for contexts: ${expected.join(", ")}`)
}

async function waitForHandlers(ms = 2000): Promise<void> {
  await new Promise((r) => setTimeout(r, ms))
}

// ============================================================================
// Tests — single app for all command/event tests
// ============================================================================

describe("Axon Server integration", () => {
  let container: StartedTestContainer
  let grpcPort: number
  let httpPort: number
  let app: Awaited<ReturnType<typeof EventSourcingConfigurer.prototype.build>>

  beforeAll(async () => {
    container = await new GenericContainer("axoniq/axonserver:2025.2.5")
      .withExposedPorts(8024, 8124)
      .withEnvironment({
        AXONIQ_AXONSERVER_DEVMODE_ENABLED: "true",
      })
      .withWaitStrategy(Wait.forHttp("/actuator/health", 8024).forStatusCode(200))
      .start()

    httpPort = container.getMappedPort(8024)
    grpcPort = container.getMappedPort(8124)
    await initClusterWithDcb(container.getHost(), httpPort)

    // Single app for all tests — avoids connection lifecycle issues
    app = EventSourcingConfigurer.create()
      .registerEntity(CourseEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createCourse)
        m.registerCommandHandler(() => changeCourseCapacity)
        m.registerCommandHandler(() => subscribeStudent)
      })
      .registerEnhancer(axonServerConfigurationEnhancer({
        componentName: "university-test",
        host: container.getHost(),
        port: grpcPort,
        context: "default",
      }))
      .build()

    await app.start()
    await waitForHandlers()
  }, 120_000)

  afterAll(async () => {
    await app?.stop()
    await container?.stop()
  })

  // -- Command dispatch + event sourcing --

  it("dispatches a command through Axon Server and sources state", async () => {
    await app.commandGateway.send(CreateCourse, {
      courseId: "cs-101",
      name: "Intro to CS",
      capacity: 30,
    })

    await app.commandGateway.send(ChangeCourseCapacity, {
      courseId: "cs-101",
      capacity: 50,
    })

    const { events } = await app.eventStore.source({
      criteria: EventCriteria.havingTags(tag("courseId", "cs-101")),
    })

    expect(events.length).toBeGreaterThanOrEqual(2)
    const firstPayload = events[0]!.payload as any
    expect(firstPayload.courseId).toBe("cs-101")
    expect(firstPayload.name).toBe("Intro to CS")
  }, 30_000)

  // -- Business rules --

  it("enforces business rules from event-sourced state", async () => {
    await app.commandGateway.send(CreateCourse, {
      courseId: "cs-201",
      name: "Data Structures",
      capacity: 25,
    })

    expect(
      app.commandGateway.send(CreateCourse, {
        courseId: "cs-201",
        name: "Duplicate",
        capacity: 10,
      }),
    ).rejects.toThrow()
  }, 30_000)

  // -- Tag-based sourcing --

  it("sources events by tag criteria", async () => {
    await app.commandGateway.send(CreateCourse, { courseId: "tag-101", name: "Tagged", capacity: 10 })
    await app.commandGateway.send(CreateCourse, { courseId: "tag-102", name: "Other", capacity: 20 })

    const { events } = await app.eventStore.source({
      criteria: EventCriteria.havingTags(tag("courseId", "tag-101")),
    })

    expect(events.length).toBe(1)
    expect((events[0]!.payload as any).courseId).toBe("tag-101")
  }, 30_000)

  // -- Capacity enforcement --

  it("enforces capacity limits across multiple students", async () => {
    await app.commandGateway.send(CreateCourse, {
      courseId: "cap-101",
      name: "Small Course",
      capacity: 2,
    })

    await app.commandGateway.send(SubscribeStudent, { courseId: "cap-101", studentId: "stu-1" })
    await app.commandGateway.send(SubscribeStudent, { courseId: "cap-101", studentId: "stu-2" })

    expect(
      app.commandGateway.send(SubscribeStudent, { courseId: "cap-101", studentId: "stu-3" }),
    ).rejects.toThrow()
  }, 30_000)

  // -- Duplicate enrollment --

  it("prevents duplicate student enrollment", async () => {
    await app.commandGateway.send(CreateCourse, {
      courseId: "dup-101",
      name: "No Duplicates",
      capacity: 10,
    })

    await app.commandGateway.send(SubscribeStudent, { courseId: "dup-101", studentId: "stu-1" })

    expect(
      app.commandGateway.send(SubscribeStudent, { courseId: "dup-101", studentId: "stu-1" }),
    ).rejects.toThrow()
  }, 30_000)

  // -- Snapshot store --

  it("stores and loads snapshots via Axon Server", async () => {
    const connection = connectToAxonServer({
      componentName: "test-snapshots",
      host: container.getHost(),
      port: grpcPort,
      context: "default",
    })

    try {
      const serializer = app.configuration.getComponent("serializer")
      const snapshotStore = createAxonServerSnapshotStore(connection, serializer as any)

      await snapshotStore.store("Course", "snap-101", {
        position: 42n,
        payload: { created: true, name: "Snapshotted", capacity: 100 },
        timestamp: Date.now(),
        metadata: {},
      })

      const loaded = await snapshotStore.load("Course", "snap-101")

      expect(loaded).toBeDefined()
      expect(loaded!.position).toBe(42n)
      expect((loaded!.payload as any).name).toBe("Snapshotted")

      await snapshotStore.deleteSnapshots("Course", "snap-101")
      const deleted = await snapshotStore.load("Course", "snap-101")
      expect(deleted).toBeUndefined()
    } finally {
      connection.close()
    }
  }, 30_000)
})
