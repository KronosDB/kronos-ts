import { afterEach, beforeEach, describe, expect, it } from "bun:test"
import {
  eventHandler,
  eventProcessor,
  inMemoryEventStore,
  inMemorySnapshottingEventStore,
  inMemoryTokenStore,
  localCommandBus,
  localQueryBus,
  queryHandler,
  type UnitOfWork,
  unitOfWork,
} from "@kronos-ts/core"
import { z } from "zod"
import { ScenarioAssertionError } from "../diff.js"
import { scenario, storyline } from "../scenario.js"
import { advanceableClock, configure, FIXTURE_EPOCH } from "../site.js"
import {
  any,
  cancelled,
  command,
  commands,
  error,
  event,
  events,
  noCommands,
  noEvents,
  type Outcome,
  query,
  result,
  scheduled,
} from "../values.js"
import {
  ArmReminder,
  CloseCourse,
  CourseClosed,
  CourseCreated,
  CourseFull,
  CreateCourse,
  clearViews,
  courseViews,
  decisions,
  EnrolmentClosing,
  GetCourseView,
  ReminderArmed,
  ReminderDue,
  reminders,
  StudentSubscribed,
  SubscribeStudent,
  university,
  withAutomation,
} from "./_university.js"

beforeEach(() => {
  clearViews()
})

afterEach(() => {
  configure({})
})

const intro = (capacity: number) => ({
  courseId: "cs-101",
  name: "Intro",
  capacity,
})
const seat = (studentId: string) => ({ courseId: "cs-101", studentId })

// ===========================================================================
// A STATE CHANGE: given events, when a command, then events or its answer.
// ===========================================================================

describe("state change", () => {
  it("creates a course", async () => {
    await scenario(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(events(event(CourseCreated, intro(30))))
  })

  it("subscribes a student, and answers with the seats left", async () => {
    await scenario(decisions)
      .given(event(CourseCreated, intro(30)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then(events(event(StudentSubscribed, seat("stu-001"))), result({ remainingSeats: 29 }))
  })

  it("refuses a duplicate course, and appends nothing", async () => {
    await scenario(decisions)
      .given(event(CourseCreated, intro(30)))
      .when(command(CreateCourse, intro(30)))
      .then(error("Course already exists"), noEvents())
  })

  it("refuses a subscription to a full course", async () => {
    await scenario(decisions)
      .given(event(CourseCreated, intro(1)), event(StudentSubscribed, seat("stu-001")))
      .when(command(SubscribeStudent, seat("stu-002")))
      .then(
        error((e) => e instanceof CourseFull),
        noEvents(),
      )
  })

  it("matches an error by substring, by pattern, or by predicate", async () => {
    const full = scenario(decisions)
      .given(event(CourseCreated, intro(1)), event(StudentSubscribed, seat("stu-001")))
      .when(command(SubscribeStudent, seat("stu-002")))

    await full.then(error("is full"))
    await full.then(error(/^Course cs-101 is full$/))
    await full.then(error(() => true))
  })

  it("accumulates repeated givens in order", async () => {
    await scenario(decisions)
      .given(event(CourseCreated, intro(1)))
      .given(event(StudentSubscribed, seat("stu-001")))
      .when(command(SubscribeStudent, seat("stu-002")))
      .then(error("is full"))
  })

  it("declines a field with `any()` and still pins the rest", async () => {
    await scenario(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(
        events(
          event(CourseCreated, {
            courseId: "cs-101",
            name: any(z.string()),
            capacity: 30,
          }),
        ),
      )
    await scenario(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(events(event(CourseCreated, any())))
  })

  it("resolves with the outcome, for anything the claims did not say", async () => {
    const outcome = await scenario(decisions)
      .given(event(CourseCreated, intro(30)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then()
    expect(outcome.act).toBe("command")
    expect(outcome.result).toEqual({ remainingSeats: 29 })
    expect(outcome.events.map((e) => e.name.name)).toEqual(["StudentSubscribed"])
  })
})

// ===========================================================================
// A STATE VIEW: given events, when a query, then the read model.
// ===========================================================================

describe("state view", () => {
  it("answers from the read model the given facts built", async () => {
    await scenario(university)
      .given(event(CourseCreated, intro(5)), event(StudentSubscribed, seat("stu-001")))
      .when(query(GetCourseView, { courseId: "cs-101" }))
      .then(
        result({
          courseId: "cs-101",
          name: "Intro",
          enrolled: 1,
          closed: false,
        }),
        noEvents(),
      )
  })

  it("a read model that has not heard of the id says so", async () => {
    await scenario(university)
      .when(query(GetCourseView, { courseId: "cs-999" }))
      .then(error('Course "cs-999" not found'))
  })

  it("folds the givens as a REPLAY — `ctx.isReplay()` is true for history, false for what arrives", async () => {
    const seen: Array<[string, boolean]> = []
    const Watched = {
      ...eventHandler(StudentSubscribed, ({ payload }, ctx) => void seen.push([payload.studentId, ctx.isReplay()])),
    }
    await scenario({ eventHandlers: [Watched] })
      .given(event(StudentSubscribed, seat("history")))
      .when(event(StudentSubscribed, seat("now")))
      .then()
    expect(seen).toEqual([
      ["history", true],
      ["now", false],
    ])
  })
})

// ===========================================================================
// An AUTOMATION: given events, when an event arrives, then the commands.
// ===========================================================================

describe("automation", () => {
  it("the last seat closes the course — the event ARRIVES and the reactor reacts", async () => {
    await scenario(withAutomation)
      .given(event(CourseCreated, intro(1)))
      .when(event(StudentSubscribed, seat("stu-001")))
      .then(
        events(event(StudentSubscribed, seat("stu-001")), event(CourseClosed, { courseId: "cs-101" })),
        commands(command(CloseCourse, { courseId: "cs-101" })),
      )
  })

  it("a seat that is not the last one dispatches nothing", async () => {
    await scenario(withAutomation)
      .given(event(CourseCreated, intro(5)))
      .when(event(StudentSubscribed, seat("stu-001")))
      .then(events(event(StudentSubscribed, seat("stu-001"))), noCommands())
  })

  it("a command act shows the commands its automations dispatched, not itself", async () => {
    const outcome = await scenario(withAutomation)
      .given(event(CourseCreated, intro(1)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then(commands(command(CloseCourse, { courseId: "cs-101" })))

    expect(outcome.commands.map((c) => c.name.name)).toEqual(["CloseCourse"])
    expect(outcome.events.map((e) => e.name.name)).toEqual(["StudentSubscribed", "CourseClosed"])
  })

  it("GIVEN events do not fire the automations — what they would dispatch is held back", async () => {
    // The given subscription fills the course. The automation folds it like
    // everyone else, but its CloseCourse is history that already happened (or
    // did not): it is not dispatched, and nothing it would have appended is.
    await scenario(withAutomation)
      .given(event(CourseCreated, intro(1)), event(StudentSubscribed, seat("stu-001")))
      .when(
        command(CreateCourse, {
          courseId: "cs-202",
          name: "Other",
          capacity: 5,
        }),
      )
      .then(
        events(
          event(CourseCreated, {
            courseId: "cs-202",
            name: "Other",
            capacity: 5,
          }),
        ),
        noCommands(),
      )
  })

  it("and the state the givens describe is real, even though nothing fired", async () => {
    await scenario(withAutomation)
      .given(event(CourseCreated, intro(1)), event(StudentSubscribed, seat("stu-001")))
      .when(command(SubscribeStudent, seat("stu-002")))
      .then(error((e) => e instanceof CourseFull))
  })

  it("GIVEN events arm no deadlines either", async () => {
    await scenario(university)
      .given(event(CourseCreated, intro(5)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .advance(60_000)
      .then(events(event(StudentSubscribed, seat("stu-001"))), noCommands())
  })
})

// ===========================================================================
// A STORYLINE: one read model walked through its states.
// ===========================================================================

describe("storyline", () => {
  it("judges each state once the events before it are handled", async () => {
    await storyline(university, query(GetCourseView, { courseId: "cs-101" }))
      .after(event(CourseCreated, intro(2)))
      .shows({ courseId: "cs-101", name: "Intro", enrolled: 0, closed: false })
      .after(event(StudentSubscribed, seat("stu-001")))
      .shows({ courseId: "cs-101", name: "Intro", enrolled: 1, closed: false })
      .after(event(StudentSubscribed, seat("stu-002")))
      .shows({ courseId: "cs-101", name: "Intro", enrolled: 2, closed: true })
  })

  it("names the beat that failed, and what the story had got to", async () => {
    const failure = (await storyline(university, query(GetCourseView, { courseId: "cs-101" }))
      .after(event(CourseCreated, intro(2)))
      .shows({ courseId: "cs-101", name: "Intro", enrolled: 0, closed: false })
      .after(event(StudentSubscribed, seat("stu-001")))
      .shows({ courseId: "cs-101", name: "Intro", enrolled: 5, closed: false })
      .then(
        () => undefined,
        (e: Error) => e,
      )) as Error

    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(failure.message).toContain(
      "storyline of GetCourseView, beat 4 (after CourseCreated, shows, after StudentSubscribed)",
    )
    expect(failure.message).toContain("enrolled: expected 5, got 1")
  })

  it("takes an assertion function for a state as well as a value", async () => {
    await storyline(university, query(GetCourseView, { courseId: "cs-101" }))
      .after(event(CourseCreated, intro(2)))
      .shows(({ result }: Outcome) => expect((result as { enrolled: number }).enrolled).toBe(0))
  })
})

// ===========================================================================
// Assertions are functions of the outcome
// ===========================================================================

describe("your own assertions", () => {
  it("is any function of the outcome — passing when it returns", async () => {
    const seatsLeft =
      (n: number) =>
      ({ result }: Outcome) =>
        expect(result).toEqual({ remainingSeats: n })
    await scenario(decisions)
      .given(event(CourseCreated, intro(3)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then(seatsLeft(2), events(event(StudentSubscribed, seat("stu-001"))))
  })

  it("and failing when it throws, under the scenario's sentence", async () => {
    const nothingLeft = function nothingLeft({ result }: Outcome) {
      expect(result).toEqual({ remainingSeats: 0 })
    }
    const failure = (await scenario(decisions)
      .given(event(CourseCreated, intro(3)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then(nothingLeft)
      .catch((e: Error) => e)) as Error

    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(failure.message.split("\n")[0]).toBe("given CourseCreated, when SubscribeStudent, then nothingLeft")
    expect(failure.message).toContain("remainingSeats")
  })

  it("reaches the running world through `app`", async () => {
    await scenario(decisions)
      .when(command(CreateCourse, intro(5)))
      .then(async ({ app }) => {
        const stored = await app.eventStore.source({
          query: { types: [CourseCreated] },
        })
        expect(stored.events).toHaveLength(1)
      })
  })

  it("can claim a throw by saying so", async () => {
    const refused = Object.assign(({ threw }: Outcome) => expect(threw).toBe(true), { claimsError: true })
    await scenario(decisions)
      .given(event(CourseCreated, intro(30)))
      .when(command(CreateCourse, intro(30)))
      .then(refused)
  })
})

// ===========================================================================
// Time
// ===========================================================================

describe("advance", () => {
  it("jumps the clock, fires what is due, and settles what that set off", async () => {
    const outcome = await scenario(university)
      .when(command(CreateCourse, intro(5)))
      .advance(30_000)
      .then(
        events(
          event(CourseCreated, intro(5)),
          event(EnrolmentClosing, { courseId: "cs-101" }),
          event(CourseClosed, { courseId: "cs-101" }),
        ),
        commands(command(CloseCourse, { courseId: "cs-101" })),
      )

    expect(outcome.events.map((e) => [e.name.name, e.timestamp])).toEqual([
      ["CourseCreated", FIXTURE_EPOCH],
      ["EnrolmentClosing", FIXTURE_EPOCH + 30_000],
      ["CourseClosed", FIXTURE_EPOCH + 30_000],
    ])
  })

  it("an advance shorter than the deadline fires nothing", async () => {
    await scenario(university)
      .when(command(CreateCourse, intro(5)))
      .advance(29_999)
      .then(
        events(event(CourseCreated, intro(5))),
        scheduled(event(EnrolmentClosing, { courseId: "cs-101" }), 30_000),
        noCommands(),
      )
  })

  it("repeated advances accumulate to the same instant as one long one", async () => {
    await scenario(university)
      .when(command(CreateCourse, intro(5)))
      .advance(20_000)
      .advance(10_000)
      .then(
        events(
          event(CourseCreated, intro(5)),
          event(EnrolmentClosing, { courseId: "cs-101" }),
          event(CourseClosed, { courseId: "cs-101" }),
        ),
      )
  })

  it("starts from the clock it was given", async () => {
    const outcome = await scenario(decisions, {
      clock: () => 1_700_000_000_000,
    })
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
    expect(outcome.events[0]!.timestamp).toBe(1_700_000_000_000)
  })

  it("reports a schedule as armed, then as cancelled when it is superseded", async () => {
    await scenario(reminders)
      .when(command(ArmReminder, { orderId: "o-1", afterMs: 30_000 }))
      .then(
        events(event(ReminderArmed, { orderId: "o-1", token: any(z.string()) })),
        scheduled(event(ReminderDue, { orderId: "o-1" }), 30_000),
      )
      .and.when(command(ArmReminder, { orderId: "o-1", afterMs: 90_000 }))
      .then(
        events(event(ReminderArmed, { orderId: "o-1", token: any(z.string()) })),
        cancelled(event(ReminderDue, { orderId: "o-1" })),
        scheduled(event(ReminderDue, { orderId: "o-1" }), 90_000),
      )
      .and.when(command(ArmReminder, { orderId: "o-2", afterMs: 1_000 }))
      .advance(90_000)
      .then(
        events(
          event(ReminderArmed, { orderId: "o-2", token: any(z.string()) }),
          event(ReminderDue, { orderId: "o-2" }),
          event(ReminderDue, { orderId: "o-1" }),
        ),
      )
  })

  it("a scenario on a plain clock cannot move it — and says so", async () => {
    const failure = (await scenario(decisions, {
      clock: () => 1_700_000_000_000,
    })
      .when(command(CreateCourse, intro(5)))
      .advance(1_000)
      .then(noEvents())
      .catch((e: Error) => e)) as Error
    expect(failure.message).toContain("cannot move time")
    expect(failure.message).toContain("advanceableClock()")
  })

  it("a processor the scenario holds settles on its own — no waiting needed", async () => {
    const clock = advanceableClock(1_700_000_000_000)
    const outcome = await scenario(withAutomation, { clock })
      .given(event(CourseCreated, intro(1)))
      .when(command(SubscribeStudent, seat("s-1")))
      .then(events(event(StudentSubscribed, seat("s-1")), event(CourseClosed, { courseId: "cs-101" })))
    expect(outcome.events.length).toBe(2)
    expect(clock()).toBe(1_700_000_000_000)
  })

  it("in memory, a claim that does not hold fails at once — there is nothing to wait for", async () => {
    const started = Date.now()
    const failure = await scenario(decisions, { within: 5_000 })
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseClosed, { courseId: "never-happens" })))
      .catch((e: Error) => e)
    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(Date.now() - started).toBeLessThan(1_000)
  })

  it("on foreign infrastructure, claims are re-judged until they hold or `within` passes", async () => {
    const started = Date.now()
    const failure = await scenario(decisions, {
      within: 120,
      infrastructure: (uow) => ({
        unitOfWork: uow,
        eventStore: inMemorySnapshottingEventStore(inMemoryEventStore()),
      }),
    })
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseClosed, { courseId: "never-happens" })))
      .catch((e: Error) => e)
    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(Date.now() - started).toBeGreaterThanOrEqual(100)
  })
})

// ===========================================================================
// One world, continued: `and`
// ===========================================================================

describe("and", () => {
  it("a prior act's events are the next act's history", async () => {
    await scenario(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(events(event(CourseCreated, intro(30))))
      .and.when(command(SubscribeStudent, seat("stu-001")))
      .then(events(event(StudentSubscribed, seat("stu-001"))))
  })

  it("a saga plays out across steps of one world", async () => {
    await scenario(withAutomation)
      .when(command(CreateCourse, intro(2)))
      .then(events(event(CourseCreated, intro(2))), noCommands())
      .and.when(command(SubscribeStudent, seat("stu-001")))
      .then(result({ remainingSeats: 1 }), events(event(StudentSubscribed, seat("stu-001"))), noCommands())
      .and.when(command(SubscribeStudent, seat("stu-002")))
      .then(
        events(event(StudentSubscribed, seat("stu-002")), event(CourseClosed, { courseId: "cs-101" })),
        commands(command(CloseCourse, { courseId: "cs-101" })),
      )
      .and.when(command(SubscribeStudent, seat("stu-003")))
      .then(error("Course is closed"), noEvents())
  })

  it("a step after a failed one does not run", async () => {
    let ran = false
    const marking = () => {
      ran = true
    }
    const failure = await scenario(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(noEvents())
      .and.when(command(SubscribeStudent, seat("stu-001")))
      .then(marking)
      .catch((e: Error) => e)
    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(ran).toBe(false)
  })

  it("the read model catches up before `then` resolves — no polling, no sleep", async () => {
    await scenario(university)
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
      .and.when(command(SubscribeStudent, seat("stu-001")))
      .then(events(event(StudentSubscribed, seat("stu-001"))))
    expect(courseViews().get("cs-101")?.enrolled).toBe(1)
  })
})

// ===========================================================================
// The builder is a value
// ===========================================================================

describe("the builder", () => {
  it("one half-built scenario finishes several ways", async () => {
    const lastSeat = (subject: typeof decisions) =>
      scenario(subject)
        .given(event(CourseCreated, intro(1)))
        .when(command(SubscribeStudent, seat("stu-001")))

    await lastSeat(decisions).then(events(event(StudentSubscribed, seat("stu-001"))))
    const failure = await lastSeat(withAutomation)
      .then(events(event(StudentSubscribed, seat("stu-001"))))
      .catch((e: Error) => e)
    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect((failure as Error).message).toContain("university.CourseClosed")
  })

  it("awaiting it without `then` runs it with no claims, and resolves with the outcome", async () => {
    const outcome = (await scenario(decisions).when(command(CreateCourse, intro(5)))) as unknown as Outcome
    expect(outcome.events.map((e) => e.name.name)).toEqual(["CourseCreated"])
  })

  it("and an unclaimed throw rejects that await instead of hanging it", async () => {
    const failure = await (async () => {
      try {
        await scenario(decisions).when(command(SubscribeStudent, { courseId: "cs-404", studentId: "s" }))
      } catch (e) {
        return e as Error
      }
    })()
    expect(failure?.message).toBe("Course does not exist")
  })
})

// ===========================================================================
// The failure message IS the product
// ===========================================================================

describe("the diff", () => {
  const failing = async (judged: Promise<unknown>) =>
    (await judged.then(
      () => undefined,
      (e: Error) => e,
    )) as Error

  it("names the event and the differing fields", async () => {
    const failure = await failing(
      scenario(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(
          events(
            event(CourseCreated, {
              courseId: "cs-101",
              name: "Introduction",
              capacity: 40,
            }),
          ),
        ),
    )
    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(failure.message).toContain("events did not match what the act appended.")
    expect(failure.message).toContain("payload differences:")
    expect(failure.message).toContain("~ [0] university.CourseCreated")
    expect(failure.message).toContain('name: expected "Introduction", got "Intro"')
    expect(failure.message).toContain("capacity: expected 40, got 30")
    expect(failure.message).not.toContain("missing — expected")
    expect(failure.message).not.toContain("unexpected — appended")
  })

  it("opens with the scenario's own sentence", async () => {
    const failure = await failing(
      scenario(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(noEvents()),
    )
    expect(failure.message.split("\n")[0]).toBe("when CreateCourse, then no events")
  })

  it("lists both sides in full so the shape of the mismatch is visible", async () => {
    const failure = await failing(
      scenario(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(events(event(CourseClosed, { courseId: "cs-101" }))),
    )
    expect(failure.message).toContain("  expected (1):\n    [0] university.CourseClosed")
    expect(failure.message).toContain("  appended (1):\n    [0] university.CourseCreated")
    expect(failure.message).toContain("missing — expected, never appended:")
    expect(failure.message).toContain("unexpected — appended, never expected:")
  })

  it("reports an expected event that was never appended", async () => {
    const failure = await failing(
      scenario(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(events(event(CourseCreated, intro(30)), event(CourseClosed, { courseId: "cs-101" }))),
    )
    expect(failure.message).toContain("expected (2):")
    expect(failure.message).toContain("appended (1):")
    expect(failure.message).toContain('- [1] university.CourseClosed  {"courseId":"cs-101"}')
    expect(failure.message).not.toContain("unexpected — appended")
  })

  it("aligns by name, so one missing event does not shift every later pair", async () => {
    const failure = await failing(
      scenario(withAutomation)
        .given(event(CourseCreated, intro(1)))
        .when(command(SubscribeStudent, seat("stu-001")))
        .then(
          events(
            event(CourseCreated, intro(1)),
            event(StudentSubscribed, seat("stu-001")),
            event(CourseClosed, { courseId: "cs-101" }),
          ),
        ),
    )
    expect(failure.message).toContain("- [0] university.CourseCreated")
    expect(failure.message).not.toContain("unexpected — appended")
    expect(failure.message).not.toContain("payload differences:")
  })

  it("`noEvents()` against something appended names what showed up", async () => {
    const failure = await failing(
      scenario(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(noEvents()),
    )
    expect(failure.message).toContain("  expected (0):\n    (none)")
    expect(failure.message).toContain("+ [0] university.CourseCreated")
  })

  it("renders a hole as `*` rather than as an object nobody wrote", async () => {
    const failure = await failing(
      scenario(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(events(event(CourseClosed, any()))),
    )
    expect(failure.message).toContain("- [0] university.CourseClosed  *")
  })

  it("reports the commands list the same exact way", async () => {
    const failure = await failing(
      scenario(withAutomation)
        .given(event(CourseCreated, intro(1)))
        .when(command(SubscribeStudent, seat("stu-001")))
        .then(noCommands()),
    )
    expect(failure.message).toContain("commands did not match what the act dispatched.")
    expect(failure.message).toContain("+ [0] university.CloseCourse")
  })

  it("reports a wrong result field by field", async () => {
    const failure = await failing(
      scenario(decisions)
        .given(event(CourseCreated, intro(30)))
        .when(command(SubscribeStudent, seat("stu-001")))
        .then(result({ remainingSeats: 3 })),
    )
    expect(failure.message).toContain("`result()`")
    expect(failure.message).toContain("remainingSeats: expected 3, got 29")
  })

  it("reports every failing claim, not only the first", async () => {
    const failure = await failing(
      scenario(decisions)
        .given(event(CourseCreated, intro(30)))
        .when(command(SubscribeStudent, seat("stu-001")))
        .then(result({ remainingSeats: 3 }), noEvents()),
    )
    expect(failure.message).toContain("remainingSeats: expected 3, got 29")
    expect(failure.message).toContain("+ [0] university.StudentSubscribed")
  })

  it("reports an expected throw that never came", async () => {
    const failure = await failing(
      scenario(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(error("Course already exists")),
    )
    expect(failure.message).toContain(
      'expected the act to throw an error whose message contains "Course already exists", ' +
        "but it completed successfully.",
    )
  })

  it("an unclaimed throw surfaces as itself, not as an events diff", async () => {
    const failure = await failing(
      scenario(decisions)
        .when(
          command(SubscribeStudent, {
            courseId: "cs-404",
            studentId: "stu-001",
          }),
        )
        .then(
          events(
            event(StudentSubscribed, {
              courseId: "cs-404",
              studentId: "stu-001",
            }),
          ),
        ),
    )
    expect(failure).not.toBeInstanceOf(ScenarioAssertionError)
    expect(failure.message).toBe("Course does not exist")
  })

  it("reports a schedule that was never armed the way it was claimed", async () => {
    const failure = await failing(
      scenario(university)
        .when(command(CreateCourse, intro(5)))
        .then(
          events(event(CourseCreated, intro(5))),
          scheduled(event(EnrolmentClosing, { courseId: "cs-101" }), 60_000),
        ),
    )
    expect(failure.message).toContain("no schedule matches `scheduled(university.EnrolmentClosing, 60000)`")
    expect(failure.message).toContain("after 30000ms (pending)")
  })
})

// ===========================================================================
// Misuse — a bug in the test, said plainly
// ===========================================================================

describe("misuse", () => {
  it("refuses `result()` against an event arriving", async () => {
    const failure = await scenario(withAutomation)
      .given(event(CourseCreated, intro(5)))
      .when(event(StudentSubscribed, seat("stu-001")))
      .then(result(undefined))
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("an event answers nobody")
  })

  it("refuses a hole in a fact", async () => {
    const failure = await scenario(decisions)
      .given(event(CourseCreated, any()))
      .when(command(CloseCourse, { courseId: "cs-101" }))
      .then(noEvents())
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("this event is a FACT")
  })
})

// ===========================================================================
// The subject: a slice's lists, or a composition root over the resources
// ===========================================================================

describe("the subject", () => {
  it("takes a slice's lists as they are — no store, no bus, no wiring", async () => {
    const getCourse = queryHandler(GetCourseView, async () => ({
      courseId: "x",
      name: "X",
      enrolled: 0,
      closed: false,
    }))
    await scenario({ queryHandlers: [getCourse] })
      .when(query(GetCourseView, { courseId: "x" }))
      .then(result({ courseId: "x", name: "X", enrolled: 0, closed: false }))
  })

  it("calls a scope with the site's own log — ONE store, capabilities and all", async () => {
    let seen: unknown
    await scenario((resources) => {
      seen = resources.eventStore
      return decisions(resources)
    })
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
    expect(typeof (seen as { storeSnapshot?: unknown }).storeSnapshot).toBe("function")
    expect(Array.isArray((seen as { appended?: unknown }).appended)).toBe(true)
  })

  it("completes a partial processor with the site's resources", async () => {
    await scenario(university)
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
    expect(courseViews().has("cs-101")).toBe(true)
  })

  it("takes an already-built processor, which then reads its own log", async () => {
    const foreignStore = inMemoryEventStore()
    const outcome = await scenario((resources) => ({
      ...decisions(resources),
      eventHandlers: [
        {
          ...withAutomation(resources).eventHandlers![0]!,
          processor: eventProcessor("foreign", foreignStore, inMemoryTokenStore(), unitOfWork),
        },
      ],
    }))
      .given(event(CourseCreated, intro(1)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then(events(event(StudentSubscribed, seat("stu-001"))))
    expect(outcome.events.map((e) => e.name.name)).toEqual(["StudentSubscribed"])
  })
})

// ===========================================================================
// Infrastructure: configured once, overridden per scenario
// ===========================================================================

describe("infrastructure", () => {
  it("runs against buses and a log the host supplied", async () => {
    let builtWith: (() => unknown) | undefined
    const log = inMemorySnapshottingEventStore(inMemoryEventStore())
    await scenario(decisions, {
      infrastructure: (unitOfWork) => {
        builtWith = unitOfWork
        return {
          unitOfWork,
          eventStore: log,
          commandBus: localCommandBus(unitOfWork),
          queryBus: localQueryBus(unitOfWork),
        }
      },
    })
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))

    const stored = await log.source({ query: { types: [CourseCreated] } })
    expect(stored.events.map((e) => e.name.name)).toEqual(["CourseCreated"])
    expect(builtWith).toBeDefined()
  })

  it("hands over a task that is already on the scenario's clock, and still carries", async () => {
    let minted: UnitOfWork | undefined
    await scenario(decisions, {
      clock: () => 1_700_000_000_000,
      infrastructure: (unitOfWork) => {
        const uow = () => {
          minted = unitOfWork()
          return minted
        }
        return {
          unitOfWork: uow,
          eventStore: inMemorySnapshottingEventStore(inMemoryEventStore()),
        }
      },
    })
      .when(command(CreateCourse, { courseId: "cs-1", name: "N", capacity: 1 }))
      .then(
        events(
          event(
            CourseCreated,
            { courseId: "cs-1", name: "N", capacity: 1 },
            { correlationId: any(), causationId: any() },
          ),
        ),
      )
    expect(minted!.now()).toBe(1_700_000_000_000)
  })

  it("is set once with `configure`, and a scenario's own options win", async () => {
    let built = 0
    configure({
      infrastructure: (unitOfWork) => {
        built++
        return { unitOfWork }
      },
    })
    await scenario(decisions)
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
    await scenario(decisions)
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
    expect(built).toBe(2)

    await scenario(decisions, {
      infrastructure: (unitOfWork) => ({ unitOfWork }),
    })
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
    expect(built).toBe(2)
  })

  it("wraps every handler, closes what it opened, and rides along on `app`", async () => {
    const kinds: string[] = []
    let closed = false
    await scenario(university, {
      infrastructure: (unitOfWork) => ({
        unitOfWork,
        wrap: (handler, kind) => {
          kinds.push(kind)
          return handler
        },
        close: () => {
          closed = true
        },
        marker: "brought along",
      }),
    })
      .when(command(CreateCourse, intro(5)))
      .then(({ app }) => expect(app.marker).toBe("brought along"))

    expect(new Set(kinds)).toEqual(new Set(["command", "query", "event"]))
    expect(closed).toBe(true)
  })

  it("keeps the log in memory unless it brings one — so it can still move time", async () => {
    await scenario(university, {
      infrastructure: (unitOfWork) => ({ unitOfWork }),
    })
      .when(command(CreateCourse, intro(5)))
      .advance(30_000)
      .then(commands(command(CloseCourse, { courseId: "cs-101" })))
  })
})
