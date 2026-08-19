import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  any,
  cancelled,
  command,
  error,
  event,
  noCommands,
  noEvents,
  query,
  result,
  scheduled,
} from "../values.js"
import { given, scenario } from "../scenario.js"
import {
  CourseCreated,
  CreateCourse,
  CourseClosed,
  EnrolmentClosing,
  GetCourseView,
  StudentSubscribed,
  SubscribeStudent,
} from "./_university.js"

// ---------------------------------------------------------------------------
// The builder is PURE. Every test in here runs without a store, a bus or a
// clock, because a scenario is a value and building one cannot do anything.
// ---------------------------------------------------------------------------

describe("scenario — a value, not a script", () => {
  it("records given, when and then as steps in the order they were written", () => {
    const s = given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
      .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
      .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }))

    expect(s.steps.map((step) => step.kind)).toEqual(["given", "when"])
    expect(s.then).toHaveLength(1)
  })

  it("never mutates — a half-built scenario finishes several different ways", () => {
    const half = given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 2 }))

    const a = half
      .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
      .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }))
    const b = half
      .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" }))
      .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-002" }))

    expect(a).not.toBe(b)
    expect(a.steps).toHaveLength(2)
    expect(b.steps).toHaveLength(2)
    // The shared prefix is untouched by either continuation.
    expect(
      half.when(command(SubscribeStudent, { courseId: "x", studentId: "y" })).then().steps[0],
    ).toEqual(a.steps[0])
  })

  it("`wait` is chainable at either joint, and repeatable", () => {
    const s = given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
      .wait(1_000)
      .when(command(CreateCourse, { courseId: "cs-202", name: "Other", capacity: 1 }))
      .wait(500)
      .wait(500)
      .then(noEvents())

    expect(s.steps.map((step) => step.kind)).toEqual(["given", "wait", "when", "wait", "wait"])
  })

  it("`given()` with no facts is the same empty world as `scenario()`", () => {
    const a = given()
      .when(command(CreateCourse, { courseId: "c", name: "n", capacity: 1 }))
      .then()
    const b = scenario()
      .when(command(CreateCourse, { courseId: "c", name: "n", capacity: 1 }))
      .then()

    expect(a.steps.map((s) => s.kind)).toEqual(["when"])
    expect(b.steps.map((s) => s.kind)).toEqual(["when"])
  })

  it("takes exactly one act, of any of the three kinds", () => {
    expect(
      scenario()
        .when(command(CreateCourse, { courseId: "c", name: "n", capacity: 1 }))
        .then().steps,
    ).toHaveLength(1)
    expect(
      scenario()
        .when(query(GetCourseView, { courseId: "c" }))
        .then().steps,
    ).toHaveLength(1)
    expect(
      scenario()
        .when(event(CourseClosed, { courseId: "c" }))
        .then().steps,
    ).toHaveLength(1)
  })

  it("derives a description that reads as a sentence", () => {
    const s = given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
      .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
      .then(
        event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
        command(CreateCourse, { courseId: "cs-202", name: "Other", capacity: 1 }),
      )

    expect(s.description).toBe(
      "given CourseCreated, when SubscribeStudent, then StudentSubscribed, CreateCourse",
    )
  })

  it("names every assertion kind in the description, waits included", () => {
    const s = scenario()
      .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 1 }))
      .wait(30_000)
      .then(
        result({ ok: true }),
        error("nope"),
        noEvents(),
        noCommands(),
        scheduled(event(EnrolmentClosing, { courseId: "cs-101" }), 30_000),
        cancelled(event(EnrolmentClosing, { courseId: "cs-101" })),
      )

    expect(s.description).toBe(
      "when CreateCourse, wait 30000ms, then a result, an error, no events, no commands, " +
        "EnrolmentClosing scheduled after 30000ms, EnrolmentClosing cancelled",
    )
  })

  it("says so when nothing is claimed", () => {
    expect(
      scenario()
        .when(command(CreateCourse, { courseId: "c", name: "n", capacity: 1 }))
        .then().description,
    ).toBe("when CreateCourse, then nothing is claimed")
  })
})

describe("any — the payload hole", () => {
  it("is a value, with and without a schema", () => {
    expect(any()).toEqual({ kind: "any" })
    const withSchema = any(z.string())
    expect(withSchema.kind).toBe("any")
    expect(withSchema.schema).toBeDefined()
  })

  it("sits in a payload positionally, or in one field of it", () => {
    expect(event(CourseCreated, any()).payload).toEqual({ kind: "any" })
    expect(
      (event(CourseCreated, { courseId: "cs-101", name: any(), capacity: 1 }).payload as any).name,
    ).toEqual({ kind: "any" })
  })
})
