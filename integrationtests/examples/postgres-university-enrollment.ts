/**
 * Example: a full CQRS slice on @kronos-ts/postgres, composed the way the
 * surface intends — every part named on its own line, nothing bundled, and no
 * builder standing between a handler and the reader.
 *
 *   write side  : kronos() + postgresPool on bunSqlAdapter (Bun.sql driver)
 *                 → events land in kronos_events, snapshots in kronos_snapshots
 *
 *   read side   : a tracking processor projects into course_views, writing
 *                 through drizzle bound to THE UNIT OF WORK'S transaction
 *
 *   query       : drizzle reads course_views for the final dump
 *
 * ONE FAMILY OWNS THE TRANSACTION, AND THE ORM RIDES IT. The postgres family
 * owns each task's transaction; `uowDb(ctx)` binds drizzle to the very
 * connection the event store appends on (`tx.unwrap()`), so a projection write
 * and the processor's token update commit or roll back together. Handing
 * drizzle its own pool would give it its own transaction — silently
 * non-atomic. See "Transactions: one owner, lenses for the rest" in
 * docs/how-it-works.md.
 *
 * WHAT THIS FILE DELIBERATELY DOES NOT DO: no `buildProjector(db)`, no bus
 * bundle, no `carrying()` wrapper. Handlers are plain top-level values that
 * name what they use; the composition root names each store, bus, task factory
 * and processor once and writes the arrows itself.
 *
 * Run: bun run integrationtests/examples/postgres-university-enrollment.ts
 * (requires docker for testcontainers, Bun >= 1.2 for Bun.SQL).
 */
import { z } from "zod"
import { GenericContainer, Wait } from "testcontainers"
import { qn, send } from "@kronos-ts/core"
import {
  command, event, commandHandler, eventHandler, jsonSerializer,
  eventProcessor,
} from "@kronos-ts/core"
import { state } from "@kronos-ts/core"
import {
  afterEvents,
  descriptorBasedTagResolver,
} from "@kronos-ts/core"
import { kronos } from "@kronos-ts/core"
import {
  correlating,
  correlatingHandler,
  correlation,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  localCommandBus,
  localQueryBus,
  type UnitOfWork,
  type CommandBus,
  type CommandHandlerContext,
  type SubscriptionCapableQueryBus,
} from "@kronos-ts/core"
import {
  postgresPool,
  postgresEventStore,
  postgresSnapshottingEventStore,
  postgresTokenStore,
  postgresTransaction,
  postgresUnitOfWork,
} from "@kronos-ts/postgres"
import { bunSqlAdapter } from "@kronos-ts/postgres/adapters/bun-sql"
import { drizzle } from "drizzle-orm/bun-sql"
import { pgTable, text, integer, timestamp } from "drizzle-orm/pg-core"
import { eq, sql } from "drizzle-orm"
import type { Message, Metadata } from "@kronos-ts/core"

// The id-pair cargo, written out as any host writes it: the chain is inherited
// or seeded; the cause is the parent, unconditionally.
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

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
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    (): CourseState => ({ opened: false, capacity: 0, enrolled: [] }),
    [CourseOpened, (s, { payload: e }) => ({ ...s, opened: true, capacity: e.capacity })],
    [StudentEnrolled, (s, { payload: e }) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })],
  ],
  snapshot: { key: "course-v1", when: afterEvents(1) },
})

type StudentState = { registered: boolean; maxCourses: number; courses: string[] }
const Student = state({
  id: { studentId: z.string() },
  tags: (id) => ({ studentId: id.studentId }),
  evolve: [
    (): StudentState => ({ registered: false, maxCourses: 0, courses: [] }),
    [StudentRegistered, (s, { payload: e }) => ({ ...s, registered: true, maxCourses: e.maxCourses })],
    [StudentEnrolled, (s, { payload: e }) => ({ ...s, courses: [...s.courses, e.courseId] })],
  ],
  snapshot: { key: "student-v1", when: afterEvents(1) },
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

// THE CONTEXT THIS APP'S COMMAND HANDLERS GET, NAMED ONCE.
//
// Every capability the app depends on is spelled here and nowhere else, so a
// handler writes one word instead of an intersection that grows every time the
// deployment learns a new trick. Add scheduling later and this line changes;
// the handlers do not:
//
//   type UniversityCommandContext =
//     CommandHandlerContext & ScheduleCapability & SubscriptionCapability & DrizzleCapability
//
// Named for the APP. Today this app uses nothing beyond the base context:
// `Course` / `Student` declare a snapshot policy, but snapshotting is a STORE
// tier — the wrapped log below serves it through `ctx.load`, and a handler has
// nothing to name. Wire a bare `postgresEventStore` instead and the first load
// throws, naming the wrapper.
type UniversityCommandContext = CommandHandlerContext

const openCourse = commandHandler(OpenCourse, async ({ payload: cmd }, ctx: UniversityCommandContext) => {
  const course = await ctx.load(Course, { courseId: cmd.courseId })
  if (course.opened) throw new Error(`Course ${cmd.courseId} already opened`)
  ctx.append(CourseOpened, { courseId: cmd.courseId, title: cmd.title, capacity: cmd.capacity })
})

const registerStudent = commandHandler(RegisterStudent, async ({ payload: cmd }, ctx: UniversityCommandContext) => {
  const student = await ctx.load(Student, { studentId: cmd.studentId })
  if (student.registered) throw new Error(`Student ${cmd.studentId} already registered`)
  ctx.append(StudentRegistered, { studentId: cmd.studentId, name: cmd.name, maxCourses: cmd.maxCourses })
})

const enrollStudent = commandHandler(EnrollStudent, async ({ payload: cmd }, ctx: UniversityCommandContext) => {
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

/**
 * Drizzle bound to THE TASK'S transaction — the same connection the event
 * store appends on, so what a projection writes commits with the token update
 * that records it. `postgresTransaction` opens the lazy transaction if this is
 * the first writer in the task; `unwrap()` hands over the live driver handle,
 * and the caller owns the cast because the handle type is adapter-specific.
 */
async function uowDb(ctx: { readonly unitOfWork: UnitOfWork }) {
  const tx = await postgresTransaction(ctx.unitOfWork)
  return drizzle(tx.unwrap<string>())
}

// ── projections ─────────────────────────────────────────────────────────────
// Plain top-level values. They close over NOTHING: the handle comes from the
// handling, which is what lets them be written here, beside the decisions,
// instead of inside a builder that has to be handed a database first.

const onCourseOpened = eventHandler(CourseOpened, async ({ payload: e }, ctx) => {
  const db = await uowDb(ctx)
  await db
    .insert(courseViews)
    .values({ courseId: e.courseId, title: e.title, capacity: e.capacity, enrolledCount: 0 })
    .onConflictDoUpdate({
      target: courseViews.courseId,
      set: { title: e.title, capacity: e.capacity, updatedAt: new Date() },
    })
})

const onStudentEnrolled = eventHandler(StudentEnrolled, async ({ payload: e }, ctx) => {
  const db = await uowDb(ctx)
  await db
    .update(courseViews)
    .set({ enrolledCount: sql`${courseViews.enrolledCount} + 1`, updatedAt: new Date() })
    .where(eq(courseViews.courseId, e.courseId))
})

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
    const pg = postgresPool(bunSqlAdapter({ connectionString }))
    await pg.start()

    // THE LOG, wrapped so it can serve the folds `Course` and `Student` declare
    // a policy for. ONE object, ONE serializer — and without the wrap the
    // `ctx.load` calls in the decisions above would not compile.
    const eventStore = postgresSnapshottingEventStore(
      postgresEventStore(pg, { tagResolver: descriptorBasedTagResolver() }),
      pg,
      { serializer: jsonSerializer() },
    )

    // THE TASK, named once. `correlating` makes it carry a map; the postgres
    // decorator gives it a transaction. Everything below is checked against
    // this one type — a bus built from a bare `unitOfWork` would not fit the
    // wrapped handlers, and a foreign family's token store would not fit the
    // processor.
    const uow = postgresUnitOfWork(() => correlating(unitOfWork()), pg)

    // THE BUSES, one line each. Interception sits OUTSIDE, so a command born
    // anywhere is stamped before anything routes it.
    const commandBus = interceptingCommandBus(localCommandBus(uow), correlation)
    const queryBus = interceptingQueryBus(localQueryBus(uow), correlation)

    // THE DELIVERY. `postgresTokenStore` is the SAME family as `uow`, so the
    // token update writes through the transaction the projection wrote in —
    // mixing families here is a compile error naming the factory to call.
    const projection = eventProcessor({
      name: "course-projection",
      eventStore,
      tokenStore: postgresTokenStore(pg),
      unitOfWork: uow,
    })

    // THE ARROWS, written out by the host — one entry per handler, each
    // pointing at the shared objects above. A `.map` over these would have to
    // erase the handler types to a union, which is what the `any`-typed
    // helper this file used to carry was hiding; five explicit lines cost
    // nothing and every wiring decision is visible on the line it belongs to.
    //
    // Snapshot POLICY rides on `Course` / `Student` themselves; the CAPABILITY
    // is a site fact riding on the log attached here. `correlatingHandler`
    // demands a correlating task on its OUTPUT — which is why no handler above
    // had to mention one.
    const app = kronos({
      commandHandlers: [
        { ...openCourse, handler: correlatingHandler(openCourse.handler, correlationFrom), eventStore, commandBus, queryBus },
        { ...registerStudent, handler: correlatingHandler(registerStudent.handler, correlationFrom), eventStore, commandBus, queryBus },
        { ...enrollStudent, handler: correlatingHandler(enrollStudent.handler, correlationFrom), eventStore, commandBus, queryBus },
      ],
      eventHandlers: [
        { ...onCourseOpened, handler: correlatingHandler(onCourseOpened.handler, correlationFrom), commandBus, queryBus, processor: projection },
        { ...onStudentEnrolled, handler: correlatingHandler(onStudentEnrolled.handler, correlationFrom), commandBus, queryBus, processor: projection },
      ],
    })

    try {
      // ---- Scenario 1: open CS-101 (cap 2), register, enroll both --------
      console.log("\n== open CS-101 (cap 2), register alice + bob, enroll both ==")
      await send(commandBus, OpenCourse, {
        courseId: "CS-101", title: "Intro to DCB", capacity: 2,
      })
      await send(commandBus, RegisterStudent, {
        studentId: "alice", name: "Alice", maxCourses: 2,
      })
      await send(commandBus, RegisterStudent, {
        studentId: "bob", name: "Bob", maxCourses: 2,
      })
      await send(commandBus, EnrollStudent, {
        courseId: "CS-101", studentId: "alice",
      })
      await send(commandBus, EnrollStudent, {
        courseId: "CS-101", studentId: "bob",
      })

      // ---- Scenario 2: DCB conflict on CS-102 last seat -----------------
      console.log("\n== open CS-102 (cap 1), register carol + dave, race two enrolls ==")
      await send(commandBus, OpenCourse, {
        courseId: "CS-102", title: "Tiny Seminar", capacity: 1,
      })
      await send(commandBus, RegisterStudent, {
        studentId: "carol", name: "Carol", maxCourses: 1,
      })
      await send(commandBus, RegisterStudent, {
        studentId: "dave", name: "Dave", maxCourses: 1,
      })
      const results = await Promise.allSettled([
        send(commandBus, EnrollStudent, { courseId: "CS-102", studentId: "carol" }),
        send(commandBus, EnrollStudent, { courseId: "CS-102", studentId: "dave" }),
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
      // Wait for EVERY enrollment the log accepted, not just the first course:
      // the dump below prints what the projection holds, so it has to wait for
      // all of it or it prints a race.
      await waitFor(async () => {
        const rows = await db.select().from(courseViews)
        const byId = new Map(rows.map((r) => [r.courseId, r.enrolledCount]))
        return byId.get("CS-101") === 2 && byId.get("CS-102") === 1
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
      // ONE key column, holding exactly the string the state declared plus its
      // flattened id — `course-v1:{"courseId":"CS101"}`. Nothing parses it,
      // which is precisely why it is readable here.
      const snapRows = await db.execute<{
        key: string; position: string; bytes: number
      }>(sql`
        SELECT key, position::text AS position,
               octet_length(payload) AS bytes
          FROM kronos_snapshots
         ORDER BY key
      `)
      if (snapRows.length === 0) {
        console.log("  (none)")
      } else {
        console.log("  key                                    | pos | payload bytes")
        console.log("  ---------------------------------------+-----+---------------")
        for (const r of snapRows) {
          console.log(
            `  ${r.key.padEnd(38)} | ${r.position.padStart(3)} | ${r.bytes}`,
          )
        }
      }
    } finally {
      await app.stop()
      await pg.close()
    }
  } finally {
    // Close drizzle's Bun.SQL client before tearing down the container.
    await (db.$client as unknown as { end(): Promise<void> }).end()
    await container.stop()
  }
}

// TOP-LEVEL AWAIT, not `main().catch(…)`. A floating promise does not hold
// Bun's event loop open across testcontainers' docker calls, so the older
// spelling exited 0 having printed one line and done nothing.
try {
  await main()
} catch (err) {
  console.error(err)
  process.exit(1)
}
