import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  on,
  commandHandler,
  EventCriteria,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { load, append } from "@kronos-ts/eventsourcing"
import { createTestFixture, type TestFixture, FixtureAssertionError } from "../fixture.js"

// ============================================================================
// Domain — same as the real integration tests
// ============================================================================

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
})

const SubscribeStudent = command({
  name: qn("university", "SubscribeStudent"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
})

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const StudentSubscribed = event({
  name: qn("university", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId), tag("studentId", p.studentId)],
})

type CourseState = { created: boolean; name: string; capacity: number; enrolled: string[] }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0, enrolled: [] }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: [
    on(CourseCreated, (s: CourseState, { payload: e }) => ({ ...s, created: true, name: e.name, capacity: e.capacity })),
    on(StudentSubscribed, (s: CourseState, { payload: e }) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })),
  ],
})

const createCourse = commandHandler(CreateCourse, async ({ payload: cmd }) => {
  const course = await load(Course, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

const subscribeStudent = commandHandler(SubscribeStudent, async ({ payload: cmd }) => {
  const course = await load(Course, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Already subscribed")
  append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

// ============================================================================
// Tests
// ============================================================================

describe("Test Fixture", () => {
  let fixture: TestFixture

  afterEach(async () => {
    await fixture?.stop()
  })

  it("creates a course and verifies the event", async () => {
    fixture = await createTestFixture((app) => {
      app.states(Course)
      app.commands(createCourse, subscribeStudent)
    })

    await fixture
      .given()
        .noPriorActivity()
      .when()
        .command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })
      .then()
        .expectSuccess()
        .expectEvents(
          [CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }],
        )
  })

  it("rejects duplicate course creation", async () => {
    fixture = await createTestFixture((app) => {
      app.states(Course)
      app.commands(createCourse, subscribeStudent)
    })

    await fixture
      .given()
        .events([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }])
      .when()
        .command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })
      .then()
        .expectException("Course already exists")
        .expectNoEvents()
  })

  it("subscribes a student to a course", async () => {
    fixture = await createTestFixture((app) => {
      app.states(Course)
      app.commands(createCourse, subscribeStudent)
    })

    await fixture
      .given()
        .events([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }])
      .when()
        .command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
      .then()
        .expectSuccess()
        .expectEvents(
          [StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }],
        )
  })

  it("rejects subscription when course is full", async () => {
    fixture = await createTestFixture((app) => {
      app.states(Course)
      app.commands(createCourse, subscribeStudent)
    })

    await fixture
      .given()
        .events(
          [CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }],
          [StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }],
        )
      .when()
        .command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" })
      .then()
        .expectException("Course is full")
        .expectNoEvents()
  })

  it("supports given with commands", async () => {
    fixture = await createTestFixture((app) => {
      app.states(Course)
      app.commands(createCourse, subscribeStudent)
    })

    await fixture
      .given()
        .commands([CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }])
      .when()
        .command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
      .then()
        .expectSuccess()
        .expectEvents(
          [StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }],
        )
  })

  it("supports chained scenarios with and()", async () => {
    fixture = await createTestFixture((app) => {
      app.states(Course)
      app.commands(createCourse, subscribeStudent)
    })

    // First scenario: create a course
    await fixture
      .given()
        .noPriorActivity()
      .when()
        .command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 2 })
      .then()
        .expectSuccess()
        .expectEvents([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 2 }])
      // Second scenario: state carries over
      .and()
      .given()
        .noPriorActivity()
      .when()
        .command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
      .then()
        .expectSuccess()
        .expectEvents([StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }])
  })

  it("provides custom event assertion via expectEventsSatisfying", async () => {
    fixture = await createTestFixture((app) => {
      app.states(Course)
      app.commands(createCourse)
    })

    await fixture
      .given()
        .noPriorActivity()
      .when()
        .command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 })
      .then()
        .expectEventsSatisfying((events) => {
          expect(events).toHaveLength(1)
          expect((events[0]!.payload as any).courseId).toBe("cs-101")
        })
  })
})
