import { describe, expect, it, afterEach, beforeEach } from "bun:test"
import { inMemoryEventStore, send, query } from "@kronos-ts/core"
import {
  kronos,
  type App,
  type CommandHandlerEntry,
  type QueryHandlerEntry,
  type EventHandlerEntry,
  type HandlerSite,
  type StateEntry,
  eventProcessor,
} from "@kronos-ts/core"
import {
  lineage,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  simpleCommandBus,
  simpleQueryBus, inMemoryTokenStore, type TokenStore,
  type UnitOfWork, type CommandBus, type QueryBus,
} from "@kronos-ts/core"
import {
  command,
  error,
  event,
  given,
  noEvents,
  query as queryValue,
  result,
  scenario,
  testFixture,
  type TestFixture,
} from "@kronos-ts/test"
import {
  courseSlice,
  courses,
  COURSE_PROJECTION,
  getCourseViews,
  clearCourseViews,
  CreateCourse, ChangeCourseCapacity, SubscribeStudent, UnsubscribeStudent,
  CourseCreated, CourseCapacityChanged, StudentSubscribed, StudentUnsubscribed,
  GetCourseView, GetAllCourses,
} from "../domain/courses/courses.js"

/**
 * The two things `kronos` needs that are not modules. The UoW runner is named
 * once and handed to `simpleCommandBus` (which captures it at construction) —
 * writing it on an adjacent line is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork): { commandBus: CommandBus; queryBus: QueryBus } {
  return {
    commandBus: interceptingCommandBus(simpleCommandBus(uow), lineage),
    queryBus: interceptingQueryBus(simpleQueryBus(uow), lineage),
  }
}


// ============================================================================
// Helper
// ============================================================================

async function waitFor(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error("Timed out")
}

/**
 * Attach a site to the course slice's four lists — spread the result straight
 * into `kronos({ ...sitedCourses({ eventStore, ...buses }) })`.
 *
 * `site` is the ARGUMENT LIST of a local helper, not a stored record: every
 * entry comes out carrying BARE properties. `tokenStore` is defaulted because
 * an event processor needs one; `commandBus`/`queryBus` are required — `kronos`
 * takes them per entry now, not once for the whole app. Every event-handler
 * entry belongs to the ONE durable `COURSE_PROJECTION` processor, built here
 * from the site.
 */
function sitedCourses(site: HandlerSite & {
  commandBus: CommandBus
  queryBus: QueryBus
  tokenStore?: TokenStore
  unitOfWork?: () => UnitOfWork
}) {
  const { tokenStore = inMemoryTokenStore(), unitOfWork: uow = unitOfWork, commandBus, queryBus, ...handlerSite } = site
  if (!handlerSite.eventStore) throw new Error("sitedCourses: needs an `eventStore` on the site")
  const processor = eventProcessor({
    name: COURSE_PROJECTION,
    eventStore: handlerSite.eventStore,
    tokenStore,
    unitOfWork: uow,
  })
  return {
    states: courseSlice.states.map((s) => ({ ...s, ...handlerSite }) as StateEntry),
    commandHandlers: courseSlice.commandHandlers.map(
      (h) => ({ ...h, ...handlerSite, commandBus, queryBus }) as CommandHandlerEntry,
    ),
    queryHandlers: courseSlice.queryHandlers.map(
      (h) => ({ ...h, ...handlerSite, queryBus }) as QueryHandlerEntry,
    ),
    eventHandlers: courseSlice.eventHandlers.map(
      (h) => ({ ...h, commandBus, queryBus, processor }) as EventHandlerEntry,
    ),
  }
}

// ============================================================================
// Given-When-Then — a test is a VALUE
// ============================================================================

describe("University — the given/when/then triple", () => {
  let fixture: TestFixture

  // The slice's projection handlers run inside the fixture and write into the
  // module's view map, so each scenario starts from a clean read model.
  beforeEach(() => {
    clearCourseViews()
    fixture = testFixture(courses)
  })

  describe("Course creation", () => {
    it("creates a course with valid data", async () => {
      await fixture.run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro to CS", capacity: 30 }))
          .then(event(CourseCreated, { courseId: "cs-101", name: "Intro to CS", capacity: 30 })),
      )
    })

    it("rejects duplicate course creation", async () => {
      await fixture.run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(error("Course already exists"), noEvents()),
      )
    })
  })

  describe("Course capacity changes", () => {
    it("changes capacity on existing course", async () => {
      await fixture.run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .when(command(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 }))
          .then(event(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 })),
      )
    })

    it("rejects capacity change on nonexistent course", async () => {
      await fixture.run(
        scenario()
          .when(command(ChangeCourseCapacity, { courseId: "cs-999", capacity: 50 }))
          .then(error("Course does not exist"), noEvents()),
      )
    })

    it("skips no-op capacity change", async () => {
      await fixture.run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .when(command(ChangeCourseCapacity, { courseId: "cs-101", capacity: 30 }))
          .then(noEvents()),
      )
    })

    it("rejects capacity below enrolled count", async () => {
      await fixture.run(
        given(
          event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 }),
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-002" }),
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-003" }),
        )
          .when(command(ChangeCourseCapacity, { courseId: "cs-101", capacity: 2 }))
          .then(error("Cannot reduce capacity below enrolled count"), noEvents()),
      )
    })
  })

  describe("Student subscription", () => {
    it("subscribes a student to a course", async () => {
      await fixture.run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" })),
      )
    })

    it("rejects subscription to nonexistent course", async () => {
      await fixture.run(
        scenario()
          .when(command(SubscribeStudent, { courseId: "cs-999", studentId: "stu-001" }))
          .then(error("Course does not exist")),
      )
    })

    it("rejects subscription when course is full", async () => {
      await fixture.run(
        given(
          event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }),
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
        )
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" }))
          .then(error("Course is full"), noEvents()),
      )
    })

    it("rejects duplicate subscription", async () => {
      await fixture.run(
        given(
          event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }),
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
        )
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .then(error("Student already subscribed"), noEvents()),
      )
    })
  })

  describe("Student unsubscription", () => {
    it("unsubscribes a student from a course", async () => {
      await fixture.run(
        given(
          event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }),
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
        )
          .when(command(UnsubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .then(event(StudentUnsubscribed, { courseId: "cs-101", studentId: "stu-001" })),
      )
    })

    it("rejects unsubscription of non-subscribed student", async () => {
      await fixture.run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .when(command(UnsubscribeStudent, { courseId: "cs-101", studentId: "stu-999" }))
          .then(error("Student not subscribed"), noEvents()),
      )
    })
  })

  describe("One timeline, several acts", () => {
    it("creates a course then subscribes multiple students", async () => {
      await fixture.run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 3 }))
          .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 3 })),
      )
      for (const studentId of ["stu-001", "stu-002", "stu-003"]) {
        await fixture.run(
          scenario()
            .when(command(SubscribeStudent, { courseId: "cs-101", studentId }))
            .then(event(StudentSubscribed, { courseId: "cs-101", studentId })),
        )
      }

      // Course should now be full
      await fixture.run(
        scenario()
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-004" }))
          .then(error("Course is full")),
      )
    })

    it("subscribe then unsubscribe frees up a spot", async () => {
      await fixture.run(
        given(
          event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }),
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
        )
          .when(command(UnsubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .then(event(StudentUnsubscribed, { courseId: "cs-101", studentId: "stu-001" })),
      )

      // Spot is now free
      await fixture.run(
        scenario()
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" }))
          .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-002" })),
      )
    })

    it("a prior act's command flows through the full bus and becomes history", async () => {
      await fixture.run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 })),
      )
      await fixture.run(
        scenario()
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" })),
      )
    })

    it("the slice's projection runs inside the fixture, and answers a query act", async () => {
      await fixture.run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 })),
      )
      await fixture.run(
        scenario()
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" })),
      )

      // `run` does not return until the processors are quiet, so the read model
      // is already caught up — no polling, no sleep. Asked through the read side,
      // which is what a state-view slice is FOR.
      await fixture.run(
        scenario()
          .when(queryValue(GetCourseView, { courseId: "cs-101" }))
          .then(
            result({
              courseId: "cs-101",
              name: "Intro",
              capacity: 30,
              enrolledCount: 1,
              students: ["stu-001"],
            }),
            noEvents(),
          ),
      )
      expect(getCourseViews().get("cs-101")?.enrolledCount).toBe(1)
    })
  })
})

// ============================================================================
// Full Application Flow Tests (no fixture — real event processors)
// ============================================================================

describe("University — Full Application Flow", () => {
  let app: App | undefined

  beforeEach(() => { clearCourseViews() })
  afterEach(async () => {
    if (app) { await app.stop(); app = undefined }
  })

  it("command → event → processor → projection → query", async () => {
    const buses = inMemoryBuses()
    app = kronos({
      ...sitedCourses({ eventStore: inMemoryEventStore(), ...buses }),
    })

    // when
    await send(buses.commandBus, CreateCourse, {
      courseId: "cs-101", name: "Intro to CS", capacity: 30,
    })

    // then — wait for the event processor to deliver to the projection
    await waitFor(() => getCourseViews().has("cs-101"))

    const course = await query(buses.queryBus, GetCourseView, { courseId: "cs-101" })
    expect((course as any).name).toBe("Intro to CS")
    expect((course as any).capacity).toBe(30)
    expect((course as any).enrolledCount).toBe(0)
  })

  it("multiple commands update the projection correctly", async () => {
    const buses = inMemoryBuses()
    app = kronos({
      ...sitedCourses({ eventStore: inMemoryEventStore(), ...buses }),
    })

    await send(buses.commandBus, CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })
    await send(buses.commandBus, SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
    await send(buses.commandBus, SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" })

    await waitFor(() => {
      const view = getCourseViews().get("cs-101")
      return view !== undefined && view.enrolledCount === 2
    })

    const course = await query(buses.queryBus, GetCourseView, { courseId: "cs-101" })
    expect((course as any).enrolledCount).toBe(2)
    expect((course as any).students).toContain("stu-001")
    expect((course as any).students).toContain("stu-002")
  })

  it("business rules enforced after state sourced from events", async () => {
    const buses = inMemoryBuses()
    app = kronos({
      ...sitedCourses({ eventStore: inMemoryEventStore(), ...buses }),
    })

    await send(buses.commandBus, CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 2 })
    await send(buses.commandBus, SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
    await send(buses.commandBus, SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" })

    // Course is now full — third subscription should fail
    expect(
      send(buses.commandBus, SubscribeStudent, { courseId: "cs-101", studentId: "stu-003" }),
    ).rejects.toThrow("Course is full")
  })

  // Coverage: token-store position persistence — an injected tokenStore (an
  // ordinary component override, no container slot) receives processor position
  // writes for the COURSE_PROJECTION processor.
  it("token store records processor position via a component override", async () => {
    const probe = inMemoryTokenStore()
    const buses = inMemoryBuses()
    app = kronos({
      ...sitedCourses({ eventStore: inMemoryEventStore(), tokenStore: probe, ...buses }),
    })

    await send(buses.commandBus, CreateCourse, {
      courseId: "tk-101", name: "TokenStore", capacity: 10,
    })
    await waitFor(() => getCourseViews().has("tk-101"))

    // Slot-default tokenStore was overridden by the probe; the
    // course-projection processor must have written its position there.
    const segments = await probe.fetchSegments(COURSE_PROJECTION)
    expect(segments.length).toBeGreaterThan(0)
    const token = await probe.get(COURSE_PROJECTION, segments[0]!)
    expect(token).toBeDefined()
  })

  it("query returns all courses", async () => {
    const buses = inMemoryBuses()
    app = kronos({
      ...sitedCourses({ eventStore: inMemoryEventStore(), ...buses }),
    })

    await send(buses.commandBus, CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })
    await send(buses.commandBus, CreateCourse, { courseId: "cs-201", name: "Advanced", capacity: 20 })

    await waitFor(() => getCourseViews().size >= 2)

    const all = await query(buses.queryBus, GetAllCourses, {}) as any[]
    expect(all.length).toBe(2)
    expect(all.find((c: any) => c.courseId === "cs-101")).toBeDefined()
    expect(all.find((c: any) => c.courseId === "cs-201")).toBeDefined()
  })
})
