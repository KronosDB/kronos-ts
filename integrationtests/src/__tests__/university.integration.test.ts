import { describe, expect, it, afterEach, beforeEach } from "bun:test"
import { kronos, type RunningApp } from "@kronos-ts/core"
import { createTestFixture, type TestFixture } from "@kronos-ts/test"
import { configureCourses } from "../domain/courses/configuration.js"
import { getCourseViews, clearCourseViews } from "../domain/courses/projections.js"
import {
  CreateCourse, ChangeCourseCapacity, SubscribeStudent, UnsubscribeStudent,
  CourseCreated, CourseCapacityChanged, StudentSubscribed, StudentUnsubscribed,
  GetCourseView, GetAllCourses,
} from "../domain/courses/messages.js"

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

// ============================================================================
// Given-When-Then Fixture Tests
// ============================================================================

describe("University — Given-When-Then Fixture", () => {
  let fixture: TestFixture

  afterEach(async () => { await fixture?.stop() })

  describe("Course creation", () => {
    it("creates a course with valid data", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given().noPriorActivity()
        .when().command(CreateCourse, { courseId: "cs-101", name: "Intro to CS", capacity: 30 })
        .then()
          .expectSuccess()
          .expectEvents([CourseCreated, { courseId: "cs-101", name: "Intro to CS", capacity: 30 }])
    })

    it("rejects duplicate course creation", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given().events([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }])
        .when().command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })
        .then()
          .expectException("Course already exists")
          .expectNoEvents()
    })
  })

  describe("Course capacity changes", () => {
    it("changes capacity on existing course", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given().events([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }])
        .when().command(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
        .then()
          .expectSuccess()
          .expectEvents([CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }])
    })

    it("rejects capacity change on nonexistent course", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given().noPriorActivity()
        .when().command(ChangeCourseCapacity, { courseId: "cs-999", capacity: 50 })
        .then()
          .expectException("Course does not exist")
          .expectNoEvents()
    })

    it("skips no-op capacity change", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given().events([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }])
        .when().command(ChangeCourseCapacity, { courseId: "cs-101", capacity: 30 })
        .then()
          .expectSuccess()
          .expectNoEvents()
    })

    it("rejects capacity below enrolled count", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given()
          .events(
            [CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 }],
            [StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }],
            [StudentSubscribed, { courseId: "cs-101", studentId: "stu-002" }],
            [StudentSubscribed, { courseId: "cs-101", studentId: "stu-003" }],
          )
        .when().command(ChangeCourseCapacity, { courseId: "cs-101", capacity: 2 })
        .then()
          .expectException("Cannot reduce capacity below enrolled count")
          .expectNoEvents()
    })
  })

  describe("Student subscription", () => {
    it("subscribes a student to a course", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given().events([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }])
        .when().command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
        .then()
          .expectSuccess()
          .expectEvents([StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }])
    })

    it("rejects subscription to nonexistent course", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given().noPriorActivity()
        .when().command(SubscribeStudent, { courseId: "cs-999", studentId: "stu-001" })
        .then()
          .expectException("Course does not exist")
    })

    it("rejects subscription when course is full", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given()
          .events(
            [CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }],
            [StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }],
          )
        .when().command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" })
        .then()
          .expectException("Course is full")
          .expectNoEvents()
    })

    it("rejects duplicate subscription", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given()
          .events(
            [CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }],
            [StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }],
          )
        .when().command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
        .then()
          .expectException("Student already subscribed")
          .expectNoEvents()
    })
  })

  describe("Student unsubscription", () => {
    it("unsubscribes a student from a course", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given()
          .events(
            [CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }],
            [StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }],
          )
        .when().command(UnsubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
        .then()
          .expectSuccess()
          .expectEvents([StudentUnsubscribed, { courseId: "cs-101", studentId: "stu-001" }])
    })

    it("rejects unsubscription of non-subscribed student", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given().events([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }])
        .when().command(UnsubscribeStudent, { courseId: "cs-101", studentId: "stu-999" })
        .then()
          .expectException("Student not subscribed")
          .expectNoEvents()
    })
  })

  describe("Chained scenarios", () => {
    it("creates a course then subscribes multiple students", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given().noPriorActivity()
        .when().command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 3 })
        .then()
          .expectSuccess()
          .expectEvents([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 3 }])
        .and()
        .given().noPriorActivity()
        .when().command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
        .then()
          .expectSuccess()
        .and()
        .given().noPriorActivity()
        .when().command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" })
        .then()
          .expectSuccess()
        .and()
        .given().noPriorActivity()
        .when().command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-003" })
        .then()
          .expectSuccess()
        .and()
        // Course should now be full
        .given().noPriorActivity()
        .when().command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-004" })
        .then()
          .expectException("Course is full")
    })

    it("subscribe then unsubscribe frees up a spot", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given()
          .events(
            [CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }],
            [StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }],
          )
        .when().command(UnsubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
        .then()
          .expectSuccess()
        .and()
        // Spot is now free
        .given().noPriorActivity()
        .when().command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" })
        .then()
          .expectSuccess()
          .expectEvents([StudentSubscribed, { courseId: "cs-101", studentId: "stu-002" }])
    })

    it("given with commands flows through the full bus", async () => {
      fixture = await createTestFixture(configureCourses)

      await fixture
        .given()
          .commands(
            [CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }],
          )
        .when().command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
        .then()
          .expectSuccess()
          .expectEvents([StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }])
    })
  })
})

// ============================================================================
// Full Application Flow Tests (no fixture — real event processors)
// ============================================================================

describe("University — Full Application Flow", () => {
  let app: RunningApp | undefined

  beforeEach(() => { clearCourseViews() })
  afterEach(async () => {
    if (app) { await app.stop(); app = undefined }
  })

  it("command → event → processor → projection → query", async () => {
    app = await kronos({ quiet: true }).use(configureCourses).start()

    // when
    await app.commandGateway.send(CreateCourse, {
      courseId: "cs-101", name: "Intro to CS", capacity: 30,
    })

    // then — wait for the event processor to deliver to the projection
    await waitFor(() => getCourseViews().has("cs-101"))

    const course = await app.queryGateway.query(GetCourseView, { courseId: "cs-101" })
    expect((course as any).name).toBe("Intro to CS")
    expect((course as any).capacity).toBe(30)
    expect((course as any).enrolledCount).toBe(0)
  })

  it("multiple commands update the projection correctly", async () => {
    app = await kronos({ quiet: true }).use(configureCourses).start()

    await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" })

    await waitFor(() => {
      const view = getCourseViews().get("cs-101")
      return view !== undefined && view.enrolledCount === 2
    })

    const course = await app.queryGateway.query(GetCourseView, { courseId: "cs-101" })
    expect((course as any).enrolledCount).toBe(2)
    expect((course as any).students).toContain("stu-001")
    expect((course as any).students).toContain("stu-002")
  })

  it("business rules enforced after state sourced from events", async () => {
    app = await kronos({ quiet: true }).use(configureCourses).start()

    await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 2 })
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
    await app.commandGateway.send(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" })

    // Course is now full — third subscription should fail
    expect(
      app.commandGateway.send(SubscribeStudent, { courseId: "cs-101", studentId: "stu-003" }),
    ).rejects.toThrow("Course is full")
  })

  // TODO(plan-09): the original test drove EventSourcingConfigurer.create()
  // + componentRegistry("tokenStore", ...) (configurer trio deleted in
  // Plan 08-04). Coverage: token-store position persistence across processor
  // restart. Re-enable once kronos() App exposes a typed `tokenStore` slot
  // (or ships an (app: App) => void extension equivalent to the deleted
  // componentRegistry callback). Tracked in
  // .planning/phases/08-configurer-deletion/deferred-items.md §"Plan 04".
  it.skip("token store tracks processor position across restart — deferred to Phase 9", () => {})

  it("query returns all courses", async () => {
    app = await kronos({ quiet: true }).use(configureCourses).start()

    await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })
    await app.commandGateway.send(CreateCourse, { courseId: "cs-201", name: "Advanced", capacity: 20 })

    await waitFor(() => getCourseViews().size >= 2)

    const all = await app.queryGateway.query(GetAllCourses, {}) as any[]
    expect(all.length).toBe(2)
    expect(all.find((c: any) => c.courseId === "cs-101")).toBeDefined()
    expect(all.find((c: any) => c.courseId === "cs-201")).toBeDefined()
  })
})
