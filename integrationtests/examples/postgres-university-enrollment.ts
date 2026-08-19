/**
 * Example: full CQRS slice on @kronos-ts/postgres.
 *
 *   write side  : kronos() + postgresPool on bunSqlAdapter (Bun.sql driver)
 *                 → events land in kronos_events, snapshots in kronos_snapshots
 *
 *   read side   : drizzle-orm/bun-sql against the SAME database
 *                 → a tracking processor projects events into course_views
 *
 *   query       : drizzle reads course_views for the final dump
 *
 * The framework's bunSqlAdapter and drizzle each open their own Bun.SQL
 * client against the same connection string. That's the canonical Kronos
 * composition: the framework owns its event tables; your projections live
 * wherever you want and are read with whatever query layer you prefer.
 *
 * Run: bun run integrationtests/examples/postgres-university-enrollment.ts
 * (requires docker for testcontainers, Bun >= 1.2 for Bun.SQL).
 */
import { z } from "zod"
import { GenericContainer, Wait } from "testcontainers"
import { qn, send } from "@kronos-ts/core"
import {
  command, event, commandHandler, eventHandler, jsonSerializer,
  eventProcessor, inMemoryTokenStore,
} from "@kronos-ts/core"
import { state } from "@kronos-ts/core"
import {
  afterEvents,
  descriptorBasedTagResolver,
} from "@kronos-ts/core"
import { kronos } from "@kronos-ts/core"
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
} from "@kronos-ts/postgres"
import { bunSqlAdapter } from "@kronos-ts/postgres/adapters/bun-sql"
import { drizzle } from "drizzle-orm/bun-sql"
import { pgTable, text, integer, timestamp } from "drizzle-orm/pg-core"
import { eq, sql } from "drizzle-orm"

/**
 * The two things `kronos` needs that are not handlers. The UoW runner is named
 * once and handed to BOTH `simpleCommandBus` (which captures it at construction)
 * and the event processor below — writing them on adjacent lines is what makes
 * that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork): { commandBus: CommandBus; queryBus: QueryBus } {
  return {
    commandBus: interceptingCommandBus(simpleCommandBus(uow), lineage),
    queryBus: interceptingQueryBus(simpleQueryBus(uow), lineage),
  }
}


// ============================================================================
// Read-side schema (drizzle owns this table)
// ============================================================================

const courseViews = pgTable("course_views", {
  courseId: text("course_id").primaryKey(),
  title: text("title").notNull(),
  capacity: integer("capacity").notNull(),
  enrolledCount: integer("enrolled_count").notNull().default(0),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
})

// ============================================================================
// Domain (same as before)
// ============================================================================

const CourseOpened = event({
  name: qn("university", "CourseOpened"),
  payload: z.object({ courseId: z.string(), title: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const StudentRegistered = event({
  name: qn("university", "StudentRegistered"),
  payload: z.object({ studentId: z.string(), name: z.string(), maxCourses: z.number() }),
  tags: { studentId: (p) => p.studentId },
})

const StudentEnrolled = event({
  name: qn("university", "StudentEnrolled"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
})

type CourseState = { opened: boolean; capacity: number; enrolled: string[] }
const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (): CourseState => ({ opened: false, capacity: 0, enrolled: [] }),
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    [CourseOpened, (s, { payload: e }) => ({ ...s, opened: true, capacity: e.capacity })],
    [StudentEnrolled, (s, { payload: e }) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })],
  ],
})

type StudentState = { registered: boolean; maxCourses: number; courses: string[] }
const Student = state({
  name: "Student",
  id: { studentId: z.string() },
  initial: (): StudentState => ({ registered: false, maxCourses: 0, courses: [] }),
  tags: (id) => ({ studentId: id.studentId }),
  evolve: [
    [StudentRegistered, (s, { payload: e }) => ({ ...s, registered: true, maxCourses: e.maxCourses })],
    [StudentEnrolled, (s, { payload: e }) => ({ ...s, courses: [...s.courses, e.courseId] })],
  ],
})

// Commands -------------------------------------------------------------------

const OpenCourse = command({
  name: qn("university", "OpenCourse"),
  payload: z.object({ courseId: z.string(), title: z.string(), capacity: z.number() }),
  routingKey: "courseId",
})
const RegisterStudent = command({
  name: qn("university", "RegisterStudent"),
  payload: z.object({ studentId: z.string(), name: z.string(), maxCourses: z.number() }),
  routingKey: "studentId",
})
const EnrollStudent = command({
  name: qn("university", "EnrollStudent"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  routingKey: "courseId",
})

const openCourse = commandHandler(OpenCourse, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (course.opened) throw new Error(`Course ${cmd.courseId} already opened`)
  ctx.append(CourseOpened, { courseId: cmd.courseId, title: cmd.title, capacity: cmd.capacity })
})

const registerStudent = commandHandler(RegisterStudent, async ({ payload: cmd }, ctx) => {
  const student = await ctx.load(Student, { studentId: cmd.studentId })
  if (student.registered) throw new Error(`Student ${cmd.studentId} already registered`)
  ctx.append(StudentRegistered, { studentId: cmd.studentId, name: cmd.name, maxCourses: cmd.maxCourses })
})

const enrollStudent = commandHandler(EnrollStudent, async ({ payload: cmd }, ctx) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  const student = await ctx.load(Student, { studentId: cmd.studentId })
  if (!course.opened) throw new Error("Course not open")
  if (!student.registered) throw new Error("Student not registered")
  if (course.enrolled.length >= course.capacity) throw new Error("Course full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already enrolled")
  if (student.courses.length >= student.maxCourses) throw new Error("Student over course load")
  ctx.append(StudentEnrolled, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// ============================================================================
// Demo runner
// ============================================================================

type DrizzleDb = ReturnType<typeof drizzle<Record<string, never>>>

function buildProjector(db: DrizzleDb) {
  const onCourseOpened = eventHandler(CourseOpened, async ({ payload: e }, ctx) => {
    await db
      .insert(courseViews)
      .values({ courseId: e.courseId, title: e.title, capacity: e.capacity, enrolledCount: 0 })
      .onConflictDoUpdate({
        target: courseViews.courseId,
        set: { title: e.title, capacity: e.capacity, updatedAt: new Date() },
      })
  })
  const onStudentEnrolled = eventHandler(StudentEnrolled, async ({ payload: e }, ctx) => {
    await db
      .update(courseViews)
      .set({
        enrolledCount: sql`${courseViews.enrolledCount} + 1`,
        updatedAt: new Date(),
      })
      .where(eq(courseViews.courseId, e.courseId))
  })
  return [onCourseOpened, onStudentEnrolled]
}

async function waitFor(check: () => Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await check()) return
    await new Promise((r) => setTimeout(r, 50))
  }
  throw new Error("Timed out waiting for condition")
}

async function main(): Promise<void> {
  console.log("== booting postgres:16-alpine ==")
  const container = await new GenericContainer("postgres:16-alpine")
    .withEnvironment({ POSTGRES_PASSWORD: "demo", POSTGRES_DB: "demo", POSTGRES_USER: "demo" })
    .withExposedPorts(5432)
    .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
    .start()

  const connectionString =
    `postgresql://demo:demo@${container.getHost()}:${container.getMappedPort(5432)}/demo`

  // Read side: drizzle on top of Bun.SQL, same DB as the framework.
  const db = drizzle(connectionString)

  try {
    // Apply the projection's DDL. In a real app this would be a drizzle-kit
    // migration; inline here to keep the demo single-file.
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS course_views (
        course_id      TEXT PRIMARY KEY,
        title          TEXT NOT NULL,
        capacity       INTEGER NOT NULL,
        enrolled_count INTEGER NOT NULL DEFAULT 0,
        updated_at     TIMESTAMP NOT NULL DEFAULT now()
      )
    `)

    // Write side: one pool, then each store named as a plain function of it.
    // There is no bundle — this app wants an event store, a snapshot store and
    // a transactional unit of work, so it says exactly that.
    //
    // The lazy transactional UoW factory is passed INTO the bus builder so the
    // command bus is built around it; handing it over afterwards would leave
    // the bus running handlers outside any transaction.
    const pg = postgresPool(bunSqlAdapter({ connectionString }))
    await pg.start()
    const eventStore = postgresEventStore(pg, {
      serializer: jsonSerializer(),
      tagResolver: descriptorBasedTagResolver(),
    })
    const snapshotStore = postgresSnapshotStore(pg, { serializer: jsonSerializer() })
    const uow = postgresUnitOfWork(pg, unitOfWork)
    const buses = inMemoryBuses(uow)
    const projection = eventProcessor({
      name: "course-projection",
      eventStore,
      tokenStore: inMemoryTokenStore(),
      unitOfWork: uow,
    })
    const app = kronos({
      // Per-state snapshot config, declared where the state is registered.
      states: [
        [{ ...Course, eventStore, snapshotStore }, { snapshotPolicy: afterEvents(1) }],
        [{ ...Student, eventStore, snapshotStore }, { snapshotPolicy: afterEvents(1) }],
      ],
      commandHandlers: [
        { ...openCourse, eventStore, ...buses },
        { ...registerStudent, eventStore, ...buses },
        { ...enrollStudent, eventStore, ...buses },
      ],
      eventHandlers: buildProjector(db).map((h) => ({ ...h, ...buses, processor: projection })),
    })

    try {
      // ---- Scenario 1: open CS-101 (cap 2), register, enroll both --------
      console.log("\n== open CS-101 (cap 2), register alice + bob, enroll both ==")
      await send(buses.commandBus, OpenCourse, {
        courseId: "CS-101", title: "Intro to DCB", capacity: 2,
      })
      await send(buses.commandBus, RegisterStudent, {
        studentId: "alice", name: "Alice", maxCourses: 2,
      })
      await send(buses.commandBus, RegisterStudent, {
        studentId: "bob", name: "Bob", maxCourses: 2,
      })
      await send(buses.commandBus, EnrollStudent, {
        courseId: "CS-101", studentId: "alice",
      })
      await send(buses.commandBus, EnrollStudent, {
        courseId: "CS-101", studentId: "bob",
      })

      // ---- Scenario 2: DCB conflict on CS-102 last seat -----------------
      console.log("\n== open CS-102 (cap 1), register carol + dave, race two enrolls ==")
      await send(buses.commandBus, OpenCourse, {
        courseId: "CS-102", title: "Tiny Seminar", capacity: 1,
      })
      await send(buses.commandBus, RegisterStudent, {
        studentId: "carol", name: "Carol", maxCourses: 1,
      })
      await send(buses.commandBus, RegisterStudent, {
        studentId: "dave", name: "Dave", maxCourses: 1,
      })
      const results = await Promise.allSettled([
        send(buses.commandBus, EnrollStudent, { courseId: "CS-102", studentId: "carol" }),
        send(buses.commandBus, EnrollStudent, { courseId: "CS-102", studentId: "dave" }),
      ])
      const winners = results.filter((r) => r.status === "fulfilled").length
      const losers = results.filter((r) => r.status === "rejected")
      console.log(`   -> ${winners} commit, ${losers.length} reject`)
      for (const l of losers) {
        const e = (l as PromiseRejectedResult).reason as Error
        console.log(`      reject: ${e.constructor.name}: ${e.message}`)
      }

      // ---- Wait for projection to catch up ------------------------------
      console.log("\n== waiting for projection to catch up ==")
      await waitFor(async () => {
        const cs101 = await db.select().from(courseViews).where(eq(courseViews.courseId, "CS-101"))
        return cs101[0]?.enrolledCount === 2
      })

      // ---- Dump everything via drizzle ----------------------------------
      console.log("\n== course_views (drizzle-owned projection) ==")
      const views = await db.select().from(courseViews).orderBy(courseViews.courseId)
      console.log("  course_id | title              | capacity | enrolled")
      console.log("  ----------+--------------------+----------+---------")
      for (const v of views) {
        console.log(
          `  ${v.courseId.padEnd(9)} | ${v.title.padEnd(18)} | ${String(v.capacity).padStart(8)} | ${String(v.enrolledCount).padStart(7)}`,
        )
      }

      console.log("\n== kronos_events (framework-owned) ==")
      const eventRows = await db.execute<{
        seq: string; xid: string; type: string; tags: string[]; payload: unknown
      }>(sql`
        SELECT sequence_position::text AS seq,
               transaction_id::text    AS xid,
               type, tags, payload
          FROM kronos_events
         ORDER BY sequence_position
      `)
      console.log("  seq | type                          | tags (deduped)")
      console.log("  ----+-------------------------------+---------------------------------------")
      for (const r of eventRows) {
        const dedupTags = [...new Set(r.tags)]
        console.log(`  ${r.seq.padStart(3)} | ${r.type.padEnd(29)} | ${JSON.stringify(dedupTags)}`)
      }

      console.log("\n== kronos_snapshots (framework-owned) ==")
      const snapRows = await db.execute<{
        state_name: string; state_id: string; position: string; bytes: number
      }>(sql`
        SELECT state_name, state_id, position::text AS position,
               octet_length(payload) AS bytes
          FROM kronos_snapshots
         ORDER BY state_name, state_id
      `)
      if (snapRows.length === 0) {
        console.log("  (none)")
      } else {
        console.log("  state_name | state_id                  | pos | payload bytes")
        console.log("  ------------+----------------------------+-----+---------------")
        for (const r of snapRows) {
          console.log(
            `  ${r.state_name.padEnd(11)} | ${r.state_id.padEnd(26)} | ${r.position.padStart(3)} | ${r.bytes}`,
          )
        }
      }
    } finally {
      await app.stop()
      await pg.close()
    }
  } finally {
    // Close drizzle's Bun.SQL client before tearing down the container.
    await db.$client.end()
    await container.stop()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
