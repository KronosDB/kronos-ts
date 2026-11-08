import assert from "node:assert/strict"
import { describe, expect, it, afterEach, beforeEach } from "bun:test"
import { inMemoryEventStore, send, query } from "@kronos-ts/core"
import {
  kronos,
  type App,
  type CommandHandlerEntry,
  type QueryHandlerEntry,
  type EventHandlerEntry,
  type HandlerSite,
  eventProcessor,
} from "@kronos-ts/core"
import {
  correlation,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  localCommandBus,
  localQueryBus, inMemoryTokenStore, type TokenStore,
  type UnitOfWork, type CommandBus, type QueryBus,
} from "@kronos-ts/core"
import {
  command,
  error,
  event,
  events,
  fixture,
  noEvents,
  query as queryValue,
  recordingCommandBus,
  recordingEventStore,
  result,
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
 * once and handed to `localCommandBus` (which captures it at construction) —
 * writing it on an adjacent line is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork): { commandBus: CommandBus; queryBus: QueryBus } {
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(unitOfWork), correlation),
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
 * Attach a site to the course slice's three lists — spread the result straight
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
  const processor = eventProcessor(COURSE_PROJECTION, handlerSite.eventStore, tokenStore, uow)
  return {
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
// Given-When-Then, in the shapes the model is written in
// ============================================================================

/**
 * A fresh app per scenario, composed the way a process composes it — with the
 * log and the command bus the fixture observes wrapped in recorders.
 */
function courseApp() {
  const eventStore = recordingEventStore(inMemoryEventStore())
  const commandBus = recordingCommandBus(interceptingCommandBus(localCommandBus(unitOfWork), correlation))
  const queryBus = interceptingQueryBus(localQueryBus(unitOfWork), correlation)
  const app = kronos(courses({ eventStore, commandBus, queryBus, tokenStore: inMemoryTokenStore(), unitOfWork }))
  return fixture({ app, eventStore, commandBus, queryBus })
}

describe("University — given, when, then", () => {
  // The slice's projection handlers run inside each scenario and write into the
  // module's view map, so each scenario starts from a clean read model.
  beforeEach(() => {
    clearCourseViews()
  })

  const course = (capacity: number, name = "Intro") => ({ courseId: "cs-101", name, capacity })
  const seat = (studentId: string) => ({ courseId: "cs-101", studentId })

  describe("Course creation", () => {
    it("creates a course with valid data", async () => {
      await courseApp()
        .when(command(CreateCourse, course(30, "Intro to CS")))
        .then(events(event(CourseCreated, course(30, "Intro to CS"))))
    })

    it("rejects duplicate course creation", async () => {
      await courseApp()
        .given(event(CourseCreated, course(30)))
        .when(command(CreateCourse, course(30)))
        .then(error("Course already exists"), noEvents())
    })
  })

  describe("Course capacity changes", () => {
    it("changes capacity on existing course", async () => {
      await courseApp()
        .given(event(CourseCreated, course(30)))
        .when(command(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 }))
        .then(events(event(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 })))
    })

    it("rejects capacity change on nonexistent course", async () => {
      await courseApp()
        .when(command(ChangeCourseCapacity, { courseId: "cs-999", capacity: 50 }))
        .then(error("Course does not exist"), noEvents())
    })

    it("skips no-op capacity change", async () => {
      await courseApp()
        .given(event(CourseCreated, course(30)))
        .when(command(ChangeCourseCapacity, { courseId: "cs-101", capacity: 30 }))
        .then(noEvents())
    })

    it("rejects capacity below enrolled count", async () => {
      await courseApp()
        .given(
          event(CourseCreated, course(5)),
          event(StudentSubscribed, seat("stu-001")),
          event(StudentSubscribed, seat("stu-002")),
          event(StudentSubscribed, seat("stu-003")),
        )
        .when(command(ChangeCourseCapacity, { courseId: "cs-101", capacity: 2 }))
        .then(error("Cannot reduce capacity below enrolled count"), noEvents())
    })
  })

  describe("Student subscription", () => {
    it("subscribes a student to a course", async () => {
      await courseApp()
        .given(event(CourseCreated, course(30)))
        .when(command(SubscribeStudent, seat("stu-001")))
        .then(events(event(StudentSubscribed, seat("stu-001"))))
    })

    it("rejects subscription to nonexistent course", async () => {
      await courseApp()
        .when(command(SubscribeStudent, { courseId: "cs-999", studentId: "stu-001" }))
        .then(error("Course does not exist"))
    })

    it("rejects subscription when course is full", async () => {
      await courseApp()
        .given(event(CourseCreated, course(1)), event(StudentSubscribed, seat("stu-001")))
        .when(command(SubscribeStudent, seat("stu-002")))
        .then(error("Course is full"), noEvents())
    })

    it("rejects duplicate subscription", async () => {
      await courseApp()
        .given(event(CourseCreated, course(30)), event(StudentSubscribed, seat("stu-001")))
        .when(command(SubscribeStudent, seat("stu-001")))
        .then(error("Student already subscribed"), noEvents())
    })
  })

  describe("Student unsubscription", () => {
    it("unsubscribes a student from a course", async () => {
      await courseApp()
        .given(event(CourseCreated, course(30)), event(StudentSubscribed, seat("stu-001")))
        .when(command(UnsubscribeStudent, seat("stu-001")))
        .then(events(event(StudentUnsubscribed, seat("stu-001"))))
    })

    it("rejects unsubscription of non-subscribed student", async () => {
      await courseApp()
        .given(event(CourseCreated, course(30)))
        .when(command(UnsubscribeStudent, seat("stu-999")))
        .then(error("Student not subscribed"), noEvents())
    })
  })

  describe("The read model", () => {
    it("answers from what the given facts built", async () => {
      await courseApp()
        .given(event(CourseCreated, course(30)), event(StudentSubscribed, seat("stu-001")))
        .when(queryValue(GetCourseView, { courseId: "cs-101" }))
        .then(
          result({ courseId: "cs-101", name: "Intro", capacity: 30, enrolledCount: 1, students: ["stu-001"] }),
          noEvents(),
        )
    })

    it("walks through its states as a storyline", async () => {
      const view = (enrolledCount: number, students: string[]) => ({
        courseId: "cs-101",
        name: "Intro",
        capacity: 2,
        enrolledCount,
        students,
      })
      await courseApp()
        .storyline(queryValue(GetCourseView, { courseId: "cs-101" }))
        .after(event(CourseCreated, course(2)))
        .shows(view(0, []))
        .after(event(StudentSubscribed, seat("stu-001")), event(StudentSubscribed, seat("stu-002")))
        .shows(view(2, ["stu-001", "stu-002"]))
        .after(event(StudentUnsubscribed, seat("stu-001")))
        .shows(view(1, ["stu-002"]))
    })
  })

  describe("One world, several acts", () => {
    it("creates a course then subscribes students until it is full", async () => {
      await courseApp()
        .when(command(CreateCourse, course(3)))
        .then(events(event(CourseCreated, course(3))))
        .and.when(command(SubscribeStudent, seat("stu-001")))
        .then(events(event(StudentSubscribed, seat("stu-001"))))
        .and.when(command(SubscribeStudent, seat("stu-002")))
        .then(events(event(StudentSubscribed, seat("stu-002"))))
        .and.when(command(SubscribeStudent, seat("stu-003")))
        .then(events(event(StudentSubscribed, seat("stu-003"))))
        .and.when(command(SubscribeStudent, seat("stu-004")))
        .then(error("Course is full"))
    })

    it("subscribe then unsubscribe frees up a spot", async () => {
      await courseApp()
        .given(event(CourseCreated, course(1)), event(StudentSubscribed, seat("stu-001")))
        .when(command(UnsubscribeStudent, seat("stu-001")))
        .then(events(event(StudentUnsubscribed, seat("stu-001"))))
        .and.when(command(SubscribeStudent, seat("stu-002")))
        .then(events(event(StudentSubscribed, seat("stu-002"))))
    })

    it("the slice's projection runs inside the scenario, and answers a query act", async () => {
      await courseApp()
        .when(command(CreateCourse, course(30)))
        .then(events(event(CourseCreated, course(30))))
        .and.when(command(SubscribeStudent, seat("stu-001")))
        .then(events(event(StudentSubscribed, seat("stu-001"))))
        .and.when(queryValue(GetCourseView, { courseId: "cs-101" }))
        .then(
          result({ courseId: "cs-101", name: "Intro", capacity: 30, enrolledCount: 1, students: ["stu-001"] }),
          noEvents(),
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
  }, 60_000)

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
    await assert.rejects(
      send(buses.commandBus, SubscribeStudent, { courseId: "cs-101", studentId: "stu-003" }),
      /Course is full/,
    )
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
