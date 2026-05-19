/**
 * End-to-end integration test for @kronos-ts/postgres.
 *
 * Spins up postgres:16-alpine via testcontainers, wires the postgres()
 * extension into a kronos() app, and exercises the full CQRS/ES pipeline:
 *
 *   command  → DCB-checked append (Postgres)
 *   sourcing → state reconstruction (Postgres)
 *   tracking → gap-free streaming via xid8 + pg_snapshot_xmin (Postgres)
 *   query    → projection read model
 *
 * Also verifies the DCB conflict path end-to-end: two concurrent appends with
 * the same criteria + marker race, exactly one commits, the other throws
 * AppendConditionError (SQLSTATE KR001).
 *
 * Image: postgres:16-alpine. PG14+ is the floor (D-12.13) because the engine
 * relies on `xid8` and `pg_snapshot_xmin(pg_current_snapshot())` for gap-free
 * tailing (D-12.14).
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { z } from "zod"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { qn, tag, generateIdentifier, emptyMetadata } from "@kronos-ts/common"
import type { EventMessage } from "@kronos-ts/messaging"
import {
  command,
  event,
  query,
  on,
  commandHandler,
  eventHandler,
  queryHandler,
  EventCriteria,
  trackingProcessor,
  emitUpdate,
  send,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { type EventStore, load, append, afterEvents } from "@kronos-ts/eventsourcing"
import { kronos, type App, type RunningApp } from "@kronos-ts/app"
import { postgres, AppendConditionError } from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"

// ============================================================================
// Domain — university courses, same shape as e2e-inmemory / e2e-kronosdb
// ============================================================================

const CreateCourse = command({
  name: qn("postgres-e2e", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  routingKey: "courseId",
})

const SubscribeStudent = command({
  name: qn("postgres-e2e", "SubscribeStudent"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  routingKey: "courseId",
})

const GetCourse = query({
  name: qn("postgres-e2e", "GetCourse"),
  payload: z.object({ courseId: z.string() }),
})

const CourseCreated = event({
  name: qn("postgres-e2e", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const StudentSubscribed = event({
  name: qn("postgres-e2e", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const CloseEnrollment = command({
  name: qn("postgres-e2e", "CloseEnrollment"),
  payload: z.object({ courseId: z.string() }),
  routingKey: "courseId",
})

const EnrollmentClosed = event({
  name: qn("postgres-e2e", "EnrollmentClosed"),
  payload: z.object({ courseId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[]; closed: boolean }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: () => ({ created: false, name: "", capacity: 0, enrolled: [], closed: false }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: [
    on(CourseCreated, (s, e) => ({ ...s, created: true, name: e.name, capacity: e.capacity })),
    on(StudentSubscribed, (s, e) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })),
    on(EnrollmentClosed, (s) => ({ ...s, closed: true })),
  ],
})

const createCourse = commandHandler(CreateCourse, async (cmd) => {
  const course = await load(Course, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async (cmd) => {
  const course = await load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
  append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// -- Stateful automation: an event handler that reacts to StudentSubscribed,
// sources the affected Course, and — if it is now full — issues a
// CloseEnrollment command via send(). The command runs in its own fresh
// UnitOfWork per the AF5-aligned model.

const closeEnrollment = commandHandler(CloseEnrollment, async (cmd) => {
  const course = await load(Course, { courseId: cmd.courseId })
  if (!course.created || course.closed) return
  append(EnrollmentClosed, { courseId: cmd.courseId })
})

const closeEnrollmentWhenFull = eventHandler(StudentSubscribed, async (e) => {
  const course = await load(Course, { courseId: e.courseId })
  if (course.created && !course.closed && course.enrolled.length >= course.capacity) {
    await send(CloseEnrollment, { courseId: e.courseId })
  }
})

// -- Projection (read model fed by the tracking processor) --

type CourseView = { courseId: string; name: string; capacity: number; enrolledCount: number }
const courseViews = new Map<string, CourseView>()

const onCourseCreated = eventHandler(CourseCreated, async (e) => {
  const view: CourseView = { courseId: e.courseId, name: e.name, capacity: e.capacity, enrolledCount: 0 }
  courseViews.set(e.courseId, view)
  emitUpdate(GetCourse, (q) => q.courseId === e.courseId, view)
})

const onStudentSubscribed = eventHandler(StudentSubscribed, async (e) => {
  const view = courseViews.get(e.courseId)
  if (!view) return
  view.enrolledCount++
  emitUpdate(GetCourse, (q) => q.courseId === e.courseId, view)
})

const getCourse = queryHandler(GetCourse, async (q) => {
  const view = courseViews.get(q.courseId)
  if (!view) throw new Error("Course not found")
  return view
})

// ============================================================================
// Helpers
// ============================================================================

async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 10000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await check()) return
    await new Promise((r) => setTimeout(r, 50))
  }
  throw new Error("Timed out waiting for condition")
}

// Unique IDs per test run (testcontainers gives us a fresh DB anyway, but this
// keeps tests independent within the run).
const runId = Math.random().toString(36).slice(2, 8)
const id = (name: string) => `${name}-${runId}`

// ============================================================================
// Tests
// ============================================================================

describe("E2E: @kronos-ts/postgres full stack", () => {
  let container: StartedTestContainer
  let connectionString: string
  let app: RunningApp
  let capturedEventStore: EventStore | undefined

  beforeAll(async () => {
    courseViews.clear()
    capturedEventStore = undefined

    // 1. Spin up Postgres 16. Wait for the second "ready to accept connections"
    //    line — the first is initdb, the second is the listening server.
    container = await new GenericContainer("postgres:16-alpine")
      .withEnvironment({
        POSTGRES_PASSWORD: "kronos_e2e",
        POSTGRES_DB: "kronos_e2e",
        POSTGRES_USER: "kronos_e2e",
      })
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
      .start()

    connectionString = `postgresql://kronos_e2e:kronos_e2e@${container.getHost()}:${container.getMappedPort(5432)}/kronos_e2e`

    // 2. Capture probe — RunningApp doesn't expose the resolved eventStore.
    const captureExtension = (a: App) => {
      a.decorate("eventStore", (inner) => {
        capturedEventStore = inner
        return inner
      })
    }

    // 3. Wire postgres() — bootstraps schema on connect, populates eventStore +
    //    snapshotStore slots.
    app = await kronos({ quiet: true })
      .states([Course, { snapshotPolicy: afterEvents(1) }])
      .commands(createCourse, subscribeStudent)
      .queries(getCourse)
      .processors(
        trackingProcessor("postgres-course-projection")
          .eventHandlers(onCourseCreated, onStudentSubscribed)
          .build(),
      )
      .use(captureExtension)
      .use(postgres({ adapter: pgAdapter({ connectionString }) }))
      .start()
  }, 60_000)

  afterAll(async () => {
    await app?.stop()
    await container?.stop()
  })

  function eventStore(): EventStore {
    if (!capturedEventStore) throw new Error("eventStore capture failed")
    return capturedEventStore
  }

  it("command persists events through @kronos-ts/postgres", async () => {
    const courseId = id("cs-101")

    await app.commandGateway.send(CreateCourse, {
      courseId,
      name: "Intro to Postgres",
      capacity: 30,
    })

    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", courseId)),
    })

    expect(events.length).toBe(1)
    expect((events[0]!.payload as { name: string }).name).toBe("Intro to Postgres")
  })

  it("command handler sources state from Postgres", async () => {
    const courseId = id("cs-101")

    await app.commandGateway.send(SubscribeStudent, { courseId, studentId: "stu-1" })

    const { events } = await eventStore().source({
      criteria: EventCriteria.havingTags(tag("courseId", courseId)),
    })
    expect(events.length).toBe(2)
    expect(events[1]!.name.name).toBe("StudentSubscribed")
  })

  it("DCB business rule: duplicate course creation rejected", async () => {
    const courseId = id("cs-101")
    await expect(
      app.commandGateway.send(CreateCourse, { courseId, name: "Dup", capacity: 1 }),
    ).rejects.toThrow()
  })

  it("DCB business rule: capacity enforced across commands", async () => {
    const courseId = id("cs-cap")

    await app.commandGateway.send(CreateCourse, { courseId, name: "Tiny", capacity: 1 })
    await app.commandGateway.send(SubscribeStudent, { courseId, studentId: "stu-1" })

    await expect(
      app.commandGateway.send(SubscribeStudent, { courseId, studentId: "stu-2" }),
    ).rejects.toThrow("Course is full")
  })

  it("tracking processor streams events into the projection (gap-free)", async () => {
    const courseId = id("cs-101")
    // Already created in earlier tests; processor should have caught up.
    await waitFor(() => courseViews.has(courseId))

    const view = courseViews.get(courseId)!
    expect(view.name).toBe("Intro to Postgres")
    expect(view.capacity).toBe(30)
    // SubscribeStudent fired in test 2 — projection should reflect it.
    expect(view.enrolledCount).toBe(1)
  })

  it("query gateway returns the projected view", async () => {
    const courseId = id("cs-101")
    await waitFor(() => courseViews.has(courseId))

    const view = (await app.queryGateway.query(GetCourse, { courseId })) as CourseView
    expect(view.name).toBe("Intro to Postgres")
    expect(view.enrolledCount).toBe(1)
  })

  it("same-tag concurrent appends — exactly one commits, the other throws AppendConditionError", async () => {
    const courseId = id("conflict")
    const criteria = EventCriteria.havingTags(tag("courseId", courseId))

    // Source once to capture a shared starting marker.
    const { marker } = await eventStore().source({ criteria })

    // Two appends racing on the same tag with the same precondition marker.
    // Advisory locks serialise them; the loser hits the DCB conflict check
    // and the postgres adapter raises AppendConditionError (SQLSTATE KR001).
    const ev = (studentId: string): EventMessage => ({
      identifier: generateIdentifier(),
      name: qn("postgres-e2e", "StudentSubscribed"),
      payload: { courseId, studentId },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      version: "1.0",
      tags: [{ key: "courseId", value: courseId }],
    })

    const results = await Promise.allSettled([
      eventStore().append([ev("racer-a")], { criteria, marker }),
      eventStore().append([ev("racer-b")], { criteria, marker }),
    ])

    const winners = results.filter((r) => r.status === "fulfilled")
    const losers = results.filter((r) => r.status === "rejected")
    expect(winners.length).toBe(1)
    expect(losers.length).toBe(1)
    expect((losers[0] as PromiseRejectedResult).reason).toBeInstanceOf(AppendConditionError)
  })

  it("snapshot store wired from postgres() slot — entity policy alone is sufficient", async () => {
    // The entities() tuple only specified `snapshotPolicy: afterEvents(2)` — no
    // explicit snapshotStore. The framework must fall back to the slot value
    // populated by postgres(), so snapshots land in kronos_snapshots.
    const courseId = id("cs-snap")

    // afterEvents(1) triggers when a load() observes > 1 event. The second
    // SubscribeStudent's load sees 2 events, so snapshotting fires (async,
    // fire-and-forget — we poll for the row).
    await app.commandGateway.send(CreateCourse, { courseId, name: "Snap", capacity: 10 })
    await app.commandGateway.send(SubscribeStudent, { courseId, studentId: "snap-1" })
    await app.commandGateway.send(SubscribeStudent, { courseId, studentId: "snap-2" })

    const { Client } = await import("pg")
    const client = new Client({ connectionString })
    await client.connect()
    try {
      let match: { state_id: string; position: string } | undefined
      await waitFor(async () => {
        const res = await client.query<{ state_id: string; position: string }>(
          "SELECT state_id, position FROM kronos_snapshots WHERE state_name = $1",
          ["Course"],
        )
        match = res.rows.find((r) => r.state_id.includes(courseId))
        return match !== undefined
      })
      expect(match).toBeDefined()
      expect(BigInt(match!.position)).toBeGreaterThan(0n)
    } finally {
      await client.end()
    }
  })

  it("schema was bootstrapped by the postgres() extension", async () => {
    // The extension's connect-stage lifecycle hook ran bootstrapSchema(),
    // which is the only reason the previous tests' inserts worked. As a
    // direct sanity check, look the tables up via the captured adapter.
    const { Client } = await import("pg")
    const client = new Client({ connectionString })
    await client.connect()
    try {
      const res = await client.query<{ table_name: string }>(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
      )
      const tables = res.rows.map((r) => r.table_name)
      expect(tables).toContain("kronos_events")
      expect(tables).toContain("kronos_snapshots")
    } finally {
      await client.end()
    }
  })

  it("stateful automation — an event handler sends a command in its own UoW", async () => {
    // A dedicated app isolates the automation processor from the shared app's
    // assertions; it connects to the same Postgres database.
    let autoEventStore: EventStore | undefined
    const autoApp = await kronos({ quiet: true })
      .states(Course)
      .commands(createCourse, subscribeStudent, closeEnrollment)
      .processors(
        trackingProcessor("postgres-enrollment-automation")
          .eventHandlers(closeEnrollmentWhenFull)
          .build(),
      )
      .use((a: App) => {
        a.decorate("eventStore", (inner) => {
          autoEventStore = inner
          return inner
        })
      })
      .use(postgres({ adapter: pgAdapter({ connectionString }) }))
      .start()

    try {
      const courseId = id("auto-cap")
      await autoApp.commandGateway.send(CreateCourse, { courseId, name: "One Seat", capacity: 1 })
      await autoApp.commandGateway.send(SubscribeStudent, { courseId, studentId: "stu-1" })

      // The automation sources the now-full course and dispatches
      // CloseEnrollment; its handler appends EnrollmentClosed in its own UoW.
      const criteria = EventCriteria.havingTags(tag("courseId", courseId))
      await waitFor(async () => {
        const { events } = await autoEventStore!.source({ criteria })
        return events.some((ev) => ev.name.name === "EnrollmentClosed")
      })

      const { events } = await autoEventStore!.source({ criteria })
      expect(events.map((ev) => ev.name.name)).toEqual([
        "CourseCreated",
        "StudentSubscribed",
        "EnrollmentClosed",
      ])
    } finally {
      await autoApp.stop()
    }
  })
})
