/**
 * End-to-end integration test for @kronos-ts/postgres.
 *
 * Spins up postgres:16-alpine via testcontainers, builds each store as a plain
 * function of one `postgresPool`, and exercises the full CQRS/ES pipeline:
 *
 *   command  → DCB-checked append (Postgres)
 *   sourcing → state reconstruction (Postgres)
 *   tracking → gap-free streaming via xid8 + pg_snapshot_xmin (Postgres)
 *   query    → projection read model
 *
 * Also verifies the DCB conflict path end-to-end: two concurrent appends with
 * the same query + marker race, exactly one commits, the other throws
 * AppendConditionError (SQLSTATE KR001).
 *
 * Image: postgres:16-alpine. PG14+ is the floor (D-12.13) because the engine
 * relies on `xid8` and `pg_snapshot_xmin(pg_current_snapshot())` for gap-free
 * tailing (D-12.14).
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { z } from "zod"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { qn, generateIdentifier, emptyMetadata, send, query } from "@kronos-ts/core"
import type { EventMessage } from "@kronos-ts/core"
import {
  command, event, commandHandler, eventHandler, queryHandler, jsonSerializer,
  eventProcessor, type EventProcessor,
  type CommandHandlerDefinition, type QueryHandlerDefinition, type EventHandlerDefinition,
  inMemoryTokenStore, type TokenStore,
} from "@kronos-ts/core"
import { state, type StateModule } from "@kronos-ts/core"
import {
  type EventStore,
  afterEvents,
  descriptorBasedTagResolver,
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
import {
  postgresPool,
  postgresEventStore,
  postgresSnapshotStore,
  postgresUnitOfWork,
  AppendConditionError,
  type PostgresResource,
} from "@kronos-ts/postgres"

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


/**
 * Everything this app needs from postgres, named at one call site. There is no
 * bundle to take apart: the pool is the only thing with a lifetime, and each
 * store is a function of it. They share the pool, so they share transactions.
 */
function postgresStack(pool: PostgresResource) {
  const eventStore = postgresEventStore(pool, {
    serializer: jsonSerializer(),
    tagResolver: descriptorBasedTagResolver(),
  })
  return {
    eventStore,
    snapshotStore: postgresSnapshotStore(pool, { serializer: jsonSerializer() }),
    unitOfWork: postgresUnitOfWork(pool, unitOfWork),
  }
}

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
  tags: { courseId: (p) => p.courseId },
})

const StudentSubscribed = event({
  name: qn("postgres-e2e", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

const CloseEnrollment = command({
  name: qn("postgres-e2e", "CloseEnrollment"),
  payload: z.object({ courseId: z.string() }),
  routingKey: "courseId",
})

const EnrollmentClosed = event({
  name: qn("postgres-e2e", "EnrollmentClosed"),
  payload: z.object({ courseId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[]; closed: boolean }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: () => ({ created: false, name: "", capacity: 0, enrolled: [], closed: false }) as CourseState,
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
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

// -- Projection (read model fed by the tracking processor) --

type CourseView = { courseId: string; name: string; capacity: number; enrolledCount: number }
const courseViews = new Map<string, CourseView>()

const onCourseCreated = eventHandler(CourseCreated, async ({ payload: e }, ctx) => {
  const view: CourseView = { courseId: e.courseId, name: e.name, capacity: e.capacity, enrolledCount: 0 }
  courseViews.set(e.courseId, view)
  ctx.emitUpdate(GetCourse, (q) => q.courseId === e.courseId, view)
})

const onStudentSubscribed = eventHandler(StudentSubscribed, async ({ payload: e }, ctx) => {
  const view = courseViews.get(e.courseId)
  if (!view) return
  view.enrolledCount++
  ctx.emitUpdate(GetCourse, (q) => q.courseId === e.courseId, view)
})

const getCourse = queryHandler(GetCourse, async ({ payload: q }) => {
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
  let app: App
  let pool: PostgresResource
  let stack: ReturnType<typeof postgresStack>
  let buses: { commandBus: CommandBus; queryBus: QueryBus }

  beforeAll(async () => {
    courseViews.clear()

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

    // 2. Open the pool — start() connects and bootstraps the schema. Nothing
    //    has to be probed back out of a container: `stack.eventStore` IS the
    //    event store the app runs on, because the same value is handed to
    //    `kronos` below.
    pool = postgresPool(connectionString)
    await pool.start()
    stack = postgresStack(pool)

    // 3. Compose. The in-memory command bus must be built around postgres's
    //    LAZY transactional UoW factory, so it is passed as an override rather
    //    than only spread on top — otherwise handlers would run in a plain
    //    unitOfWork and never see a transaction.
    buses = inMemoryBuses(stack.unitOfWork)
    app = kronos({
      // Per-state snapshot policy, declared in the handler list — see
      // the "snapshot store" test below. The tuple is the state plus the
      // options its repository is built from; the stores come from
      // postgres's components.
      ...sitedOn(
        {
          eventStore: stack.eventStore,
          snapshotStore: stack.snapshotStore,
          ...buses,
          processorName: "postgres-course-projection",
        },
        [Course, { snapshotPolicy: afterEvents(1) }],
        createCourse, subscribeStudent,
        getCourse,
        onCourseCreated, onStudentSubscribed,
      ),
    })
  }, 60_000)

  afterAll(async () => {
    await app?.stop()
    await pool?.close()
    await container?.stop()
  })

  function eventStore(): EventStore {
    return stack.eventStore
  }

  it("command persists events through @kronos-ts/postgres", async () => {
    const courseId = id("cs-101")

    await send(buses.commandBus, CreateCourse, {
      courseId,
      name: "Intro to Postgres",
      capacity: 30,
    })

    const { events } = await eventStore().source({
      query: { tags: { courseId: courseId } },
    })

    expect(events.length).toBe(1)
    expect((events[0]!.payload as { name: string }).name).toBe("Intro to Postgres")
  })

  it("command handler sources state from Postgres", async () => {
    const courseId = id("cs-101")

    await send(buses.commandBus, SubscribeStudent, { courseId, studentId: "stu-1" })

    const { events } = await eventStore().source({
      query: { tags: { courseId: courseId } },
    })
    expect(events.length).toBe(2)
    expect(events[1]!.name.name).toBe("StudentSubscribed")
  })

  it("DCB business rule: duplicate course creation rejected", async () => {
    const courseId = id("cs-101")
    await expect(
      send(buses.commandBus, CreateCourse, { courseId, name: "Dup", capacity: 1 }),
    ).rejects.toThrow()
  })

  it("DCB business rule: capacity enforced across commands", async () => {
    const courseId = id("cs-cap")

    await send(buses.commandBus, CreateCourse, { courseId, name: "Tiny", capacity: 1 })
    await send(buses.commandBus, SubscribeStudent, { courseId, studentId: "stu-1" })

    await expect(
      send(buses.commandBus, SubscribeStudent, { courseId, studentId: "stu-2" }),
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

    const view = (await query(buses.queryBus, GetCourse, { courseId })) as CourseView
    expect(view.name).toBe("Intro to Postgres")
    expect(view.enrolledCount).toBe(1)
  })

  it("same-tag concurrent appends — exactly one commits, the other throws AppendConditionError", async () => {
    const courseId = id("conflict")
    const courseQuery = { tags: { courseId: courseId } }

    // Source once to capture a shared starting marker.
    const { marker } = await eventStore().source({ query: courseQuery })

    // Two appends racing on the same tag with the same precondition marker.
    // Advisory locks serialise them; the loser hits the DCB conflict check
    // and the postgres adapter raises AppendConditionError (SQLSTATE KR001).
    const ev = (studentId: string): EventMessage => ({
      kind: "event",
      identifier: generateIdentifier(),
      name: qn("postgres-e2e", "StudentSubscribed"),
      payload: { courseId, studentId },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      version: "1.0",
      tags: [{ key: "courseId", value: courseId }],
    })

    const results = await Promise.allSettled([
      eventStore().append([ev("racer-a")], { query: courseQuery, marker }),
      eventStore().append([ev("racer-b")], { query: courseQuery, marker }),
    ])

    const winners = results.filter((r) => r.status === "fulfilled")
    const losers = results.filter((r) => r.status === "rejected")
    expect(winners.length).toBe(1)
    expect(losers.length).toBe(1)
    expect((losers[0] as PromiseRejectedResult).reason).toBeInstanceOf(AppendConditionError)
  })

  it("snapshots land in kronos_snapshots — the postgres snapshot store is what the state runs on", async () => {
    // The Course repository declared in beforeAll — `[Course, {
    // snapshotPolicy: afterEvents(1) }]` — is built on the module's snapshot
    // store, which is postgres's (spread into the app's components), so
    // snapshots must land in kronos_snapshots.
    const courseId = id("cs-snap")

    // afterEvents(1) triggers when a ctx.load() observes > 1 event. The second
    // SubscribeStudent's load sees 2 events, so snapshotting fires (async,
    // fire-and-forget — we poll for the row).
    await send(buses.commandBus, CreateCourse, { courseId, name: "Snap", capacity: 10 })
    await send(buses.commandBus, SubscribeStudent, { courseId, studentId: "snap-1" })
    await send(buses.commandBus, SubscribeStudent, { courseId, studentId: "snap-2" })

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

  it("schema was bootstrapped by pool.start()", async () => {
    // `await pool.start()` ran bootstrapSchema(), which is the only reason the
    // previous tests' inserts worked. As a direct sanity check, look the tables
    // up over a fresh client.
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
      // The pool bootstraps the whole family's schema, including the processor
      // stores this app happens not to use.
      expect(tables).toContain("kronos_token_entries")
      expect(tables).toContain("kronos_dead_letters")
    } finally {
      await client.end()
    }
  })

  it("stateful automation — an event handler sends a command in its own UoW", async () => {
    // A dedicated app isolates the automation processor from the shared app's
    // assertions; it connects to the same Postgres database.
    const autoPool = postgresPool(connectionString)
    await autoPool.start()
    const autoStack = postgresStack(autoPool)
    const autoEventStore: EventStore = autoStack.eventStore
    const autoBuses = inMemoryBuses(autoStack.unitOfWork)
    const autoApp = kronos({
      ...sitedOn(
        {
          eventStore: autoStack.eventStore,
          snapshotStore: autoStack.snapshotStore,
          ...autoBuses,
          processorName: "postgres-enrollment-automation",
        },
        Course,
        createCourse, subscribeStudent, closeEnrollment,
        closeEnrollmentWhenFull,
      ),
    })

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
      })

      const { events } = await autoEventStore.source({ query: courseQuery })
      expect(events.map((ev) => ev.name.name)).toEqual([
        "CourseCreated",
        "StudentSubscribed",
        "EnrollmentClosed",
      ])
    } finally {
      await autoApp.stop()
      await autoPool.close()
    }
  })
})
