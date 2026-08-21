import { beforeEach, describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  inMemoryEventStore,
  inMemoryTokenStore,
  eventProcessor,
  unitOfWork,
} from "@kronos-ts/core"
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
import { FIXTURE_EPOCH, testFixture } from "../fixture.js"
import { ScenarioAssertionError } from "../diff.js"
import {
  CloseCourse,
  Course,
  CourseClosed,
  CourseCreated,
  CourseFull,
  CreateCourse,
  EnrolmentClosing,
  GetCourseView,
  StudentSubscribed,
  SubscribeStudent,
  ArmReminder,
  ReminderArmed,
  ReminderDue,
  clearViews,
  courseViews,
  decisions,
  reminders,
  university,
  withAutomation,
} from "./_university.js"

beforeEach(() => {
  clearViews()
})

// ===========================================================================
// Shape one — a STATE CHANGE. A command decides; events are the answer.
// ===========================================================================

describe("state change", () => {
  it("creates a course", async () => {
    await testFixture(decisions).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
        .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 })),
    )
  })

  it("subscribes a student, and answers with the seats left", async () => {
    await testFixture(decisions).run(
      given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }))
        .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
        .then(
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
          result({ remainingSeats: 29 }),
        ),
    )
  })

  it("refuses a duplicate course, and appends nothing", async () => {
    await testFixture(decisions).run(
      given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }))
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
        .then(error("Course already exists"), noEvents()),
    )
  })

  it("refuses a subscription to a full course", async () => {
    await testFixture(decisions).run(
      given(
        event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }),
        event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
      )
        .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" }))
        .then(
          error((e) => e instanceof CourseFull),
          noEvents(),
        ),
    )
  })

  it("matches an error by substring, by pattern, or by predicate", async () => {
    const full = given(
      event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }),
      event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
    ).when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" }))

    await testFixture(decisions).run(full.then(error("is full")))
    await testFixture(decisions).run(full.then(error(/^Course cs-101 is full$/)))
    await testFixture(decisions).run(full.then(error(() => true)))
  })

  it("a no-op decision appends nothing", async () => {
    const fixture = testFixture(decisions)
    await fixture.run(
      given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .when(command(CloseCourse, { courseId: "cs-101" }))
        .then(event(CourseClosed, { courseId: "cs-101" })),
    )
    await fixture.run(
      scenario()
        .when(command(CloseCourse, { courseId: "cs-101" }))
        .then(noEvents()),
    )
  })

  it("declines a field with `any()` and still pins the rest", async () => {
    await testFixture(decisions).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
        .then(event(CourseCreated, { courseId: "cs-101", name: any(z.string()), capacity: 30 })),
    )
    await testFixture(decisions).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
        .then(event(CourseCreated, any())),
    )
  })
})

// ===========================================================================
// Shape two — a STATE VIEW. A query answers from a projection the events fed.
// ===========================================================================

describe("state view", () => {
  it("answers from the read model the given facts built", async () => {
    const fixture = testFixture(university)

    // The projection is fed by the automation lane, so the view has to be built
    // by events ARRIVING rather than by `given` — which is exactly the
    // distinction the two shapes exist to make.
    await fixture.run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 })),
    )

    await fixture.run(
      scenario()
        .when(query(GetCourseView, { courseId: "cs-101" }))
        .then(
          result({ courseId: "cs-101", name: "Intro", enrolled: 0, closed: false }),
          noEvents(),
        ),
    )
  })

  it("a read model that has not heard of the id says so", async () => {
    await testFixture(university).run(
      scenario()
        .when(query(GetCourseView, { courseId: "cs-999" }))
        .then(error('Course "cs-999" not found')),
    )
  })
})

// ===========================================================================
// Shape three — an AUTOMATION. An event arrives; a command is the answer.
// ===========================================================================

describe("automation", () => {
  it("the last seat closes the course — the event ARRIVES and the reactor reacts", async () => {
    await testFixture(withAutomation).run(
      given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
        .when(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }))
        .then(
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
          event(CourseClosed, { courseId: "cs-101" }),
          command(CloseCourse, { courseId: "cs-101" }),
        ),
    )
  })

  it("a seat that is not the last one dispatches nothing", async () => {
    await testFixture(withAutomation).run(
      given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .when(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }))
        .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }), noCommands()),
    )
  })

  it("a command act also shows the commands its automations dispatched, not itself", async () => {
    const { commands, events } = await testFixture(withAutomation).run(
      given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
        .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
        .then(command(CloseCourse, { courseId: "cs-101" })),
    )

    expect(commands.map((c) => c.name.name)).toEqual(["CloseCourse"])
    expect(events.map((e) => e.name.name)).toEqual(["StudentSubscribed", "CourseClosed"])
  })

  it("GIVEN events do not fire the automations — they are established history", async () => {
    // The given subscription fills the course. If `given` replayed through the
    // automation lane, the course would close during `given` and this run's
    // events would carry a CourseClosed nobody asked for.
    const { events, commands } = await testFixture(withAutomation).run(
      given(
        event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }),
        event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
      )
        .when(command(CreateCourse, { courseId: "cs-202", name: "Other", capacity: 5 }))
        .then(
          event(CourseCreated, { courseId: "cs-202", name: "Other", capacity: 5 }),
          noCommands(),
        ),
    )

    expect(events.map((e) => e.name.name)).toEqual(["CourseCreated"])
    expect(commands).toHaveLength(0)
  })

  it("and the state the givens describe is real, even though nothing fired", async () => {
    await testFixture(withAutomation).run(
      given(
        event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }),
        event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
      )
        .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" }))
        .then(error((e) => e instanceof CourseFull)),
    )
  })
})

// ===========================================================================
// Time
// ===========================================================================

describe("wait", () => {
  it("jumps the clock, fires what is due, and settles what that set off", async () => {
    const { events } = await testFixture(university).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .wait(30_000)
        .then(
          event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 }),
          event(EnrolmentClosing, { courseId: "cs-101" }),
          event(CourseClosed, { courseId: "cs-101" }),
          command(CloseCourse, { courseId: "cs-101" }),
        ),
    )

    // Coherent timestamps: the act happened at the fixture's instant, the
    // deadline fired thirty seconds later, and what the deadline caused is
    // stamped from the instant it fired — not from the instant it was arranged.
    expect(events.map((e) => [e.name.name, e.timestamp])).toEqual([
      ["CourseCreated", FIXTURE_EPOCH],
      ["EnrolmentClosing", FIXTURE_EPOCH + 30_000],
      ["CourseClosed", FIXTURE_EPOCH + 30_000],
    ])
  })

  it("a wait shorter than the deadline fires nothing", async () => {
    await testFixture(university).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .wait(29_999)
        .then(
          event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 }),
          scheduled(event(EnrolmentClosing, { courseId: "cs-101" }), 30_000),
          noCommands(),
        ),
    )
  })

  it("repeated waits accumulate to the same instant as one long one", async () => {
    await testFixture(university).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .wait(20_000)
        .wait(10_000)
        .then(
          event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 }),
          event(EnrolmentClosing, { courseId: "cs-101" }),
          event(CourseClosed, { courseId: "cs-101" }),
        ),
    )
  })

  it("starts from the clock the fixture was given", async () => {
    const { events } = await testFixture(decisions, { clock: () => 1_700_000_000_000 }).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 })),
    )
    expect(events[0]!.timestamp).toBe(1_700_000_000_000)
  })

  it("reports a schedule as armed, then as cancelled when it is superseded", async () => {
    const fixture = testFixture(reminders)

    await fixture.run(
      scenario()
        .when(command(ArmReminder, { orderId: "o-1", afterMs: 30_000 }))
        .then(
          event(ReminderArmed, { orderId: "o-1", token: any(z.string()) }),
          scheduled(event(ReminderDue, { orderId: "o-1" }), 30_000),
        ),
    )

    // Re-arming reads the old token out of state and drops that schedule first.
    await fixture.run(
      scenario()
        .when(command(ArmReminder, { orderId: "o-1", afterMs: 90_000 }))
        .then(
          event(ReminderArmed, { orderId: "o-1", token: any(z.string()) }),
          cancelled(event(ReminderDue, { orderId: "o-1" })),
          scheduled(event(ReminderDue, { orderId: "o-1" }), 90_000),
        ),
    )

    // And only the surviving one fires.
    const { events } = await fixture.run(
      scenario()
        .when(command(ArmReminder, { orderId: "o-2", afterMs: 1_000 }))
        .wait(90_000)
        .then(
          event(ReminderArmed, { orderId: "o-2", token: any(z.string()) }),
          event(ReminderDue, { orderId: "o-2" }),
          event(ReminderDue, { orderId: "o-1" }),
        ),
    )
    expect(events.map((e) => e.name.name)).toEqual(["ReminderArmed", "ReminderDue", "ReminderDue"])
  })

  it("refuses to fake time for a scope whose resources it does not own", async () => {
    const foreign = inMemoryEventStore()
    const fixture = testFixture(() => decisions(foreign))

    const failure = await fixture
      .run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
          .wait(1_000)
          .then(noEvents()),
      )
      .catch((e: Error) => e)

    expect((failure as Error).message).toContain("cannot move time for this scope")
    expect((failure as Error).message).toContain("realTime: true")
  })

  it("under realTime a wait genuinely elapses", async () => {
    const started = Date.now()
    await testFixture(decisions, { realTime: true }).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .wait(30)
        .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 })),
    )
    expect(Date.now() - started).toBeGreaterThanOrEqual(25)
  })
})

// ===========================================================================
// The timeline
// ===========================================================================

describe("the timeline", () => {
  it("a prior act's events are the next act's history", async () => {
    const fixture = testFixture(decisions)
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

  it("a saga plays out across consecutive runs on one timeline", async () => {
    const fixture = testFixture(withAutomation)

    await fixture.run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 2 }))
        .then(
          event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 2 }),
          noCommands(),
        ),
    )

    // Seat one of two: no automation yet.
    await fixture.run(
      scenario()
        .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
        .then(
          result({ remainingSeats: 1 }),
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
          noCommands(),
        ),
    )

    // The last seat trips the automation. The cursor carried across both runs,
    // so only what THIS run caused shows up.
    await fixture.run(
      scenario()
        .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-002" }))
        .then(
          event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-002" }),
          event(CourseClosed, { courseId: "cs-101" }),
          command(CloseCourse, { courseId: "cs-101" }),
        ),
    )

    // And the course really is closed.
    await fixture.run(
      scenario()
        .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-003" }))
        .then(error("Course is closed"), noEvents()),
    )
  })

  it("one scenario VALUE runs against two different scopes", async () => {
    const lastSeat = given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
      .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
      .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }))

    // Decisions only: the subscription is the whole story.
    await testFixture(decisions).run(lastSeat)

    // With the automation wired in, the same act closes the course — so the same
    // scenario is now WRONG, and that difference is the thing being tested.
    const failure = await testFixture(withAutomation)
      .run(lastSeat)
      .catch((e: Error) => e)
    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect((failure as Error).message).toContain("university.CourseClosed")
  })

  it("the read model catches up before `run` returns — no polling, no sleep", async () => {
    const fixture = testFixture(university)
    await fixture.run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 })),
    )
    await fixture.run(
      scenario()
        .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
        .then(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" })),
    )
    expect(courseViews().get("cs-101")?.enrolled).toBe(1)
  })
})

// ===========================================================================
// The failure message IS the product
// ===========================================================================

describe("the diff", () => {
  it("names the event and the differing fields", async () => {
    const failure = (await testFixture(decisions)
      .run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(event(CourseCreated, { courseId: "cs-101", name: "Introduction", capacity: 40 })),
      )
      .catch((e: Error) => e)) as Error

    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(failure.message).toContain("events did not match what the act appended.")
    expect(failure.message).toContain("payload differences:")
    expect(failure.message).toContain("~ [0] university.CourseCreated")
    expect(failure.message).toContain('name: expected "Introduction", got "Intro"')
    expect(failure.message).toContain("capacity: expected 40, got 30")
    // A name-matched pair is a difference, never a missing/unexpected pair.
    expect(failure.message).not.toContain("missing — expected")
    expect(failure.message).not.toContain("unexpected — appended")
  })

  it("opens with the scenario's own sentence", async () => {
    const failure = (await testFixture(decisions)
      .run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(noEvents()),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message.split("\n")[0]).toBe("when CreateCourse, then no events")
  })

  it("lists both sides in full so the shape of the mismatch is visible", async () => {
    const failure = (await testFixture(decisions)
      .run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(event(CourseClosed, { courseId: "cs-101" })),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message).toContain("  expected (1):\n    [0] university.CourseClosed")
    expect(failure.message).toContain("  appended (1):\n    [0] university.CourseCreated")
    expect(failure.message).toContain("missing — expected, never appended:")
    expect(failure.message).toContain("unexpected — appended, never expected:")
  })

  it("reports an expected event that was never appended", async () => {
    const failure = (await testFixture(decisions)
      .run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(
            event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }),
            event(CourseClosed, { courseId: "cs-101" }),
          ),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message).toContain("expected (2):")
    expect(failure.message).toContain("appended (1):")
    expect(failure.message).toContain('- [1] university.CourseClosed  {"courseId":"cs-101"}')
    expect(failure.message).not.toContain("unexpected — appended")
  })

  it("aligns by name, so one missing event does not shift every later pair", async () => {
    const failure = (await testFixture(withAutomation)
      .run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .then(
            // The leading CourseCreated is a `given`, so it is never re-appended.
            event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }),
            event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }),
            event(CourseClosed, { courseId: "cs-101" }),
          ),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message).toContain("- [0] university.CourseCreated")
    expect(failure.message).not.toContain("unexpected — appended")
    expect(failure.message).not.toContain("payload differences:")
  })

  it("`noEvents()` against something appended names what showed up", async () => {
    const failure = (await testFixture(decisions)
      .run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(noEvents()),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message).toContain("  expected (0):\n    (none)")
    expect(failure.message).toContain("+ [0] university.CourseCreated")
  })

  it("renders a hole as `*` rather than as an object nobody wrote", async () => {
    const failure = (await testFixture(decisions)
      .run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(event(CourseClosed, any())),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message).toContain("- [0] university.CourseClosed  *")
  })

  it("reports the commands list the same exact way", async () => {
    const failure = (await testFixture(withAutomation)
      .run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .then(noCommands()),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message).toContain("commands did not match what the act dispatched.")
    expect(failure.message).toContain("+ [0] university.CloseCourse")
  })

  it("reports a wrong result field by field", async () => {
    const failure = (await testFixture(decisions)
      .run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .then(result({ remainingSeats: 3 })),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message).toContain("`result()`")
    expect(failure.message).toContain("remainingSeats: expected 3, got 29")
  })

  it("reports an expected throw that never came", async () => {
    const failure = (await testFixture(decisions)
      .run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 30 }))
          .then(error("Course already exists")),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message).toContain(
      'expected the act to throw an error whose message contains "Course already exists", ' +
        "but it completed successfully.",
    )
  })

  it("an unclaimed throw surfaces as itself, not as an events diff", async () => {
    const failure = (await testFixture(decisions)
      .run(
        scenario()
          .when(command(SubscribeStudent, { courseId: "cs-404", studentId: "stu-001" }))
          .then(event(StudentSubscribed, { courseId: "cs-404", studentId: "stu-001" })),
      )
      .catch((e: Error) => e)) as Error

    expect(failure).not.toBeInstanceOf(ScenarioAssertionError)
    expect(failure.message).toBe("Course does not exist")
  })

  it("reports a schedule that was never armed the way it was claimed", async () => {
    const failure = (await testFixture(university)
      .run(
        scenario()
          .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
          .then(
            event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 }),
            scheduled(event(EnrolmentClosing, { courseId: "cs-101" }), 60_000),
          ),
      )
      .catch((e: Error) => e)) as Error

    expect(failure.message).toContain(
      "no schedule matches `scheduled(university.EnrolmentClosing, 60000)`",
    )
    expect(failure.message).toContain("after 30000ms (pending)")
  })
})

// ===========================================================================
// Misuse — a bug in the test, said plainly
// ===========================================================================

describe("misuse", () => {
  it("refuses `result()` against an event arriving", async () => {
    const failure = await testFixture(withAutomation)
      .run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 }))
          .when(event(StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }))
          .then(result(undefined)),
      )
      .catch((e: Error) => e)

    expect((failure as Error).message).toContain("an event answers nobody")
  })

  it("refuses a hole in a fact", async () => {
    const failure = await testFixture(decisions)
      .run(
        given(event(CourseCreated, any()))
          .when(command(CloseCourse, { courseId: "cs-101" }))
          .then(noEvents()),
      )
      .catch((e: Error) => e)

    expect((failure as Error).message).toContain("this event is a FACT")
  })
})

// ===========================================================================
// The scope is a function of its resources
// ===========================================================================

describe("the scope", () => {
  it("is called with the fixture's own log — ONE store, capabilities and all", async () => {
    let seen: unknown
    await testFixture((eventStore) => {
      seen = eventStore
      return decisions(eventStore)
    }).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 })),
    )
    // ONE parameter, and it carries everything: the recorder's `appended`, the
    // snapshotting capability's `storeSnapshot`, and the log underneath. The
    // scope used to take a second snapshot store; there is nothing left for it
    // to take, because the capability rides on the store it belongs to.
    expect(seen).toBeDefined()
    expect(typeof (seen as { storeSnapshot?: unknown }).storeSnapshot).toBe("function")
    expect(Array.isArray((seen as { appended?: unknown }).appended)).toBe(true)
  })

  it("accepts a shorter parameter list", async () => {
    await testFixture((eventStore) => ({
      commandHandlers: decisions(eventStore).commandHandlers!.map((h) => ({
        ...h,
        eventStore,
      })),
    })).run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 })),
    )
  })

  it("completes a partial processor with the fixture's resources", async () => {
    // `university`'s projection is a PartialProcessor; if the fixture did not
    // call it full-handed the read model would never be fed.
    const fixture = testFixture(university)
    await fixture.run(
      scenario()
        .when(command(CreateCourse, { courseId: "cs-101", name: "Intro", capacity: 5 }))
        .then(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 5 })),
    )
    expect(courseViews().has("cs-101")).toBe(true)
  })

  it("takes an already-built processor, and then owns nothing", async () => {
    const foreignStore = inMemoryEventStore()
    const fixture = testFixture((eventStore) => ({
      ...decisions(eventStore),
      eventHandlers: [
        {
          ...withAutomation(eventStore).eventHandlers![0]!,
          processor: eventProcessor({
            name: "foreign",
            eventStore: foreignStore,
            tokenStore: inMemoryTokenStore(),
            unitOfWork,
          }),
        },
      ],
    }))

    // The processor reads a log the acts never write to, so the automation never
    // fires — and `wait` refuses, because the fixture is no longer the site.
    const failure = await fixture
      .run(
        given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
          .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
          .wait(1)
          .then(noEvents()),
      )
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("cannot move time for this scope")
  })
})
