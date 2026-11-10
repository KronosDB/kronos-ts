import { afterEach, beforeEach, describe, expect, it } from "bun:test"
import {
  correlation,
  eventHandler,
  eventProcessor,
  inMemoryEventStore,
  inMemorySnapshottingEventStore,
  inMemoryTokenStore,
  interceptingCommandBus,
  interceptingQueryBus,
  kronos,
  localCommandBus,
  localQueryBus,
  queryHandler,
  unitOfWork,
} from "@kronos-ts/core"
import { z } from "zod"
import { ScenarioAssertionError } from "../diff.js"
import { type AdvanceableClock, advanceableClock, FIXTURE_EPOCH } from "../clock.js"
import { recordingCommandBus, recordingEventStore } from "../recording.js"
import { fixture } from "../scenario.js"
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
  type Lists,
  ReminderArmed,
  ReminderDue,
  reminders,
  type Shared,
  StudentSubscribed,
  SubscribeStudent,
  university,
  withAutomation,
} from "./_university.js"

beforeEach(() => {
  clearViews()
})

/**
 * A fresh app per scenario, built the way a process builds one, with the
 * infrastructure wrapped once at the top: the log and the command bus recorded,
 * and the log holding its deadlines against the clock the app's tasks read.
 */
/** A composition root: a function of the shared objects, keeping their types. */
type Root = <S extends Shared>(shared: S) => Lists<S>

/** Every app a test built: the fixture never stops one, so the test does. */
const running: Array<{ stop(): Promise<void> }> = []
afterEach(async () => {
  await Promise.all(running.splice(0).map((a) => a.stop()))
})

function app(lists: Root, options: { clock?: AdvanceableClock } = {}) {
  const clock = options.clock ?? advanceableClock()
  const uow = () => unitOfWork(clock)
  const eventStore = recordingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore()), { clock })
  const commandBus = recordingCommandBus(interceptingCommandBus(localCommandBus(uow), correlation))
  const queryBus = interceptingQueryBus(localQueryBus(uow), correlation)
  const built = kronos(lists({ eventStore, commandBus, queryBus, tokenStore: inMemoryTokenStore(), unitOfWork: uow }))
  running.push(built)
  return built
}

function built(lists: Root, options: { clock?: AdvanceableClock } = {}) {
  return fixture(app(lists, options))
}

const intro = (capacity: number) => ({ courseId: "cs-101", name: "Intro", capacity })
const seat = (studentId: string) => ({ courseId: "cs-101", studentId })

// ===========================================================================
// A STATE CHANGE: given events, when a command, then events or its answer.
// ===========================================================================

describe("state change", () => {
  it("creates a course", async () => {
    await built(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(events(event(CourseCreated, intro(30))))
  })

  it("subscribes a student, and answers with the seats left", async () => {
    await built(decisions)
      .given(event(CourseCreated, intro(30)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then(events(event(StudentSubscribed, seat("stu-001"))), result({ remainingSeats: 29 }))
  })

  it("refuses a duplicate course, and appends nothing", async () => {
    await built(decisions)
      .given(event(CourseCreated, intro(30)))
      .when(command(CreateCourse, intro(30)))
      .then(error("Course already exists"), noEvents())
  })

  it("refuses a subscription to a full course", async () => {
    await built(decisions)
      .given(event(CourseCreated, intro(1)), event(StudentSubscribed, seat("stu-001")))
      .when(command(SubscribeStudent, seat("stu-002")))
      .then(
        error((e) => e instanceof CourseFull),
        noEvents(),
      )
  })

  it("matches an error by substring, by pattern, or by predicate", async () => {
    const full = () =>
      built(decisions)
        .given(event(CourseCreated, intro(1)), event(StudentSubscribed, seat("stu-001")))
        .when(command(SubscribeStudent, seat("stu-002")))

    await full().then(error("is full"))
    await full().then(error(/^Course cs-101 is full$/))
    await full().then(error(() => true))
  })

  it("accumulates repeated givens in order", async () => {
    await built(decisions)
      .given(event(CourseCreated, intro(1)))
      .given(event(StudentSubscribed, seat("stu-001")))
      .when(command(SubscribeStudent, seat("stu-002")))
      .then(error("is full"))
  })

  it("declines a field with `any()` and still pins the rest", async () => {
    await built(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(events(event(CourseCreated, { courseId: "cs-101", name: any(z.string()), capacity: 30 })))
    await built(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(events(event(CourseCreated, any())))
  })

  it("resolves with the outcome, for anything the claims did not say", async () => {
    const outcome = await built(decisions)
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
    await built(university)
      .given(event(CourseCreated, intro(5)), event(StudentSubscribed, seat("stu-001")))
      .when(query(GetCourseView, { courseId: "cs-101" }))
      .then(result({ courseId: "cs-101", name: "Intro", enrolled: 1, closed: false }), noEvents())
  })

  it("a read model that has not heard of the id says so", async () => {
    await built(university)
      .when(query(GetCourseView, { courseId: "cs-999" }))
      .then(error('Course "cs-999" not found'))
  })

  it("folds the givens as a REPLAY — `ctx.isReplay()` is true for history, false for what arrives", async () => {
    const seen: Array<[string, boolean]> = []
    const watched = eventHandler(StudentSubscribed, ({ payload }, ctx) => {
      seen.push([payload.studentId, ctx.isReplay()])
    })
    await built(<S extends Shared>({ eventStore, commandBus, queryBus, tokenStore, unitOfWork }: S): Lists<S> => ({
      eventHandlers: [
        { ...watched, commandBus, queryBus, processor: eventProcessor("watch", eventStore, tokenStore, unitOfWork) },
      ],
    }))
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
    await built(withAutomation)
      .given(event(CourseCreated, intro(1)))
      .when(event(StudentSubscribed, seat("stu-001")))
      .then(
        events(event(StudentSubscribed, seat("stu-001")), event(CourseClosed, { courseId: "cs-101" })),
        commands(command(CloseCourse, { courseId: "cs-101" })),
      )
  })

  it("a seat that is not the last one dispatches nothing", async () => {
    await built(withAutomation)
      .given(event(CourseCreated, intro(5)))
      .when(event(StudentSubscribed, seat("stu-001")))
      .then(events(event(StudentSubscribed, seat("stu-001"))), noCommands())
  })

  it("a command act shows the commands its automations dispatched, not itself", async () => {
    const outcome = await built(withAutomation)
      .given(event(CourseCreated, intro(1)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then(commands(command(CloseCourse, { courseId: "cs-101" })))

    expect(outcome.commands.map((c) => c.name.name)).toEqual(["CloseCourse"])
    expect(outcome.events.map((e) => e.name.name)).toEqual(["StudentSubscribed", "CourseClosed"])
  })

  it("GIVEN events do not fire the automations — what they would dispatch is held", async () => {
    await built(withAutomation)
      .given(event(CourseCreated, intro(1)), event(StudentSubscribed, seat("stu-001")))
      .when(command(CreateCourse, { courseId: "cs-202", name: "Other", capacity: 5 }))
      .then(events(event(CourseCreated, { courseId: "cs-202", name: "Other", capacity: 5 })), noCommands())
  })

  it("and the state the givens describe is real, even though nothing fired", async () => {
    await built(withAutomation)
      .given(event(CourseCreated, intro(1)), event(StudentSubscribed, seat("stu-001")))
      .when(command(SubscribeStudent, seat("stu-002")))
      .then(error((e) => e instanceof CourseFull))
  })

  it("GIVEN events arm no deadlines either", async () => {
    await built(university)
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
    await built(university)
      .storyline(query(GetCourseView, { courseId: "cs-101" }))
      .after(event(CourseCreated, intro(2)))
      .shows({ courseId: "cs-101", name: "Intro", enrolled: 0, closed: false })
      .after(event(StudentSubscribed, seat("stu-001")))
      .shows({ courseId: "cs-101", name: "Intro", enrolled: 1, closed: false })
      .after(event(StudentSubscribed, seat("stu-002")))
      .shows({ courseId: "cs-101", name: "Intro", enrolled: 2, closed: true })
  })

  it("names the beat that failed, and what the story had got to", async () => {
    const failure = (await built(university)
      .storyline(query(GetCourseView, { courseId: "cs-101" }))
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
    await built(university)
      .storyline(query(GetCourseView, { courseId: "cs-101" }))
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
    await built(decisions)
      .given(event(CourseCreated, intro(3)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then(seatsLeft(2), events(event(StudentSubscribed, seat("stu-001"))))
  })

  it("and failing when it throws, under the scenario's sentence", async () => {
    const nothingLeft = function nothingLeft({ result }: Outcome) {
      expect(result).toEqual({ remainingSeats: 0 })
    }
    const failure = (await built(decisions)
      .given(event(CourseCreated, intro(3)))
      .when(command(SubscribeStudent, seat("stu-001")))
      .then(nothingLeft)
      .catch((e: Error) => e)) as Error

    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(failure.message.split("\n")[0]).toBe("given CourseCreated, when SubscribeStudent, then nothingLeft")
    expect(failure.message).toContain("remainingSeats")
  })

  it("reaches the running app through `app`", async () => {
    await built(decisions)
      .when(command(CreateCourse, intro(5)))
      .then(async ({ app }) => {
        const [log] = app.eventStores
        const stored = await log!.source({ query: { types: [CourseCreated] } })
        expect(stored.events).toHaveLength(1)
      })
  })

  it("can claim a throw by saying so", async () => {
    const refused = Object.assign(({ threw }: Outcome) => expect(threw).toBe(true), { claimsError: true })
    await built(decisions)
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
    const outcome = await built(university)
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
    await built(university)
      .when(command(CreateCourse, intro(5)))
      .advance(29_999)
      .then(
        events(event(CourseCreated, intro(5))),
        scheduled(event(EnrolmentClosing, { courseId: "cs-101" }), 30_000),
        noCommands(),
      )
  })

  it("repeated advances accumulate to the same instant as one long one", async () => {
    await built(university)
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

  it("stamps from the clock the app was built with", async () => {
    const outcome = await built(decisions, { clock: advanceableClock(1_700_000_000_000) })
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
    expect(outcome.events[0]!.timestamp).toBe(1_700_000_000_000)
  })

  it("reports a schedule as armed, then as cancelled when it is superseded", async () => {
    await built(reminders)
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

  it("a processor the fixture holds settles on its own — no waiting needed", async () => {
    const clock = advanceableClock(1_700_000_000_000)
    const outcome = await built(withAutomation, { clock })
      .given(event(CourseCreated, intro(1)))
      .when(command(SubscribeStudent, seat("s-1")))
      .then(events(event(StudentSubscribed, seat("s-1")), event(CourseClosed, { courseId: "cs-101" })))
    expect(outcome.events.length).toBe(2)
    expect(clock()).toBe(1_700_000_000_000)
  })

  it("a claim that does not hold fails at once — there is nothing to wait for", async () => {
    const started = Date.now()
    const failure = await built(decisions)
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseClosed, { courseId: "never-happens" })))
      .catch((e: Error) => e)
    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(Date.now() - started).toBeLessThan(1_000)
  })

})

// ===========================================================================
// One world, continued: `and`
// ===========================================================================

describe("and", () => {
  it("a prior act's events are the next act's history", async () => {
    await built(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(events(event(CourseCreated, intro(30))))
      .and.when(command(SubscribeStudent, seat("stu-001")))
      .then(events(event(StudentSubscribed, seat("stu-001"))))
  })

  it("a saga plays out across steps of one world", async () => {
    await built(withAutomation)
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
    const failure = await built(decisions)
      .when(command(CreateCourse, intro(30)))
      .then(noEvents())
      .and.when(command(SubscribeStudent, seat("stu-001")))
      .then(marking)
      .catch((e: Error) => e)
    expect(failure).toBeInstanceOf(ScenarioAssertionError)
    expect(ran).toBe(false)
  })

  it("the read model catches up before `then` resolves — no polling, no sleep", async () => {
    await built(university)
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
      .and.when(command(SubscribeStudent, seat("stu-001")))
      .then(events(event(StudentSubscribed, seat("stu-001"))))
    expect(courseViews().get("cs-101")?.enrolled).toBe(1)
  })
})

// ===========================================================================
// The builder
// ===========================================================================

describe("the builder", () => {
  it("is a value: the same steps against two apps", async () => {
    const lastSeat = (lists: Root) =>
      built(lists)
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
    const outcome = (await built(decisions).when(command(CreateCourse, intro(5)))) as unknown as Outcome
    expect(outcome.events.map((e) => e.name.name)).toEqual(["CourseCreated"])
  })

  it("and an unclaimed throw rejects that await instead of hanging it", async () => {
    const failure = await (async () => {
      try {
        await built(decisions).when(command(SubscribeStudent, { courseId: "cs-404", studentId: "s" }))
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
      built(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(events(event(CourseCreated, { courseId: "cs-101", name: "Introduction", capacity: 40 }))),
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
      built(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(noEvents()),
    )
    expect(failure.message.split("\n")[0]).toBe("when CreateCourse, then no events")
  })

  it("lists both sides in full so the shape of the mismatch is visible", async () => {
    const failure = await failing(
      built(decisions)
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
      built(decisions)
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
      built(withAutomation)
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
      built(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(noEvents()),
    )
    expect(failure.message).toContain("  expected (0):\n    (none)")
    expect(failure.message).toContain("+ [0] university.CourseCreated")
  })

  it("renders a hole as `*` rather than as an object nobody wrote", async () => {
    const failure = await failing(
      built(decisions)
        .when(command(CreateCourse, intro(30)))
        .then(events(event(CourseClosed, any()))),
    )
    expect(failure.message).toContain("- [0] university.CourseClosed  *")
  })

  it("reports the commands list the same exact way", async () => {
    const failure = await failing(
      built(withAutomation)
        .given(event(CourseCreated, intro(1)))
        .when(command(SubscribeStudent, seat("stu-001")))
        .then(noCommands()),
    )
    expect(failure.message).toContain("commands did not match what the act dispatched.")
    expect(failure.message).toContain("+ [0] university.CloseCourse")
  })

  it("reports a wrong result field by field", async () => {
    const failure = await failing(
      built(decisions)
        .given(event(CourseCreated, intro(30)))
        .when(command(SubscribeStudent, seat("stu-001")))
        .then(result({ remainingSeats: 3 })),
    )
    expect(failure.message).toContain("`result()`")
    expect(failure.message).toContain("remainingSeats: expected 3, got 29")
  })

  it("reports every failing claim, not only the first", async () => {
    const failure = await failing(
      built(decisions)
        .given(event(CourseCreated, intro(30)))
        .when(command(SubscribeStudent, seat("stu-001")))
        .then(result({ remainingSeats: 3 }), noEvents()),
    )
    expect(failure.message).toContain("remainingSeats: expected 3, got 29")
    expect(failure.message).toContain("+ [0] university.StudentSubscribed")
  })

  it("reports an expected throw that never came", async () => {
    const failure = await failing(
      built(decisions)
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
      built(decisions)
        .when(command(SubscribeStudent, { courseId: "cs-404", studentId: "stu-001" }))
        .then(events(event(StudentSubscribed, { courseId: "cs-404", studentId: "stu-001" }))),
    )
    expect(failure).not.toBeInstanceOf(ScenarioAssertionError)
    expect(failure.message).toBe("Course does not exist")
  })

  it("reports a schedule that was never armed the way it was claimed", async () => {
    const failure = await failing(
      built(university)
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
  const plain = () => {
    const uow = () => unitOfWork()
    const eventStore = inMemorySnapshottingEventStore(inMemoryEventStore())
    const commandBus = localCommandBus(uow)
    const queryBus = localQueryBus(uow)
    return { uow, eventStore, commandBus, queryBus }
  }

  it("refuses `result()` against an event arriving", async () => {
    const failure = await built(withAutomation)
      .given(event(CourseCreated, intro(5)))
      .when(event(StudentSubscribed, seat("stu-001")))
      .then(result(undefined))
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("an event answers nobody")
  })

  it("refuses a hole in a fact", async () => {
    const failure = await built(decisions)
      .given(event(CourseCreated, any()))
      .when(command(CloseCourse, { courseId: "cs-101" }))
      .then(noEvents())
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("this event is a FACT")
  })

  // The type refuses these apps; the runtime says the same, for JavaScript and casts.

  it("names the recorder to wrap when the log is not recorded", async () => {
    const { eventStore, commandBus, queryBus } = plain()
    const unrecorded = kronos(decisions({ eventStore, commandBus: recordingCommandBus(commandBus), queryBus } as never))
    const failure = await fixture(unrecorded as never)
      .when(command(CloseCourse, { courseId: "cs-101" }))
      .then()
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("recordingEventStore(")
  })

  it("names the recorder to wrap when the command bus is not recorded", async () => {
    const { eventStore, commandBus, queryBus } = plain()
    const unrecorded = kronos(decisions({ eventStore: recordingEventStore(eventStore), commandBus, queryBus } as never))
    const failure = await fixture(unrecorded as never)
      .when(command(CloseCourse, { courseId: "cs-101" }))
      .then()
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("recordingCommandBus(")
  })

  it("says what `advance` needs: a log given a clock", async () => {
    const { eventStore, commandBus, queryBus } = plain()
    const unclocked = kronos(
      decisions({ eventStore: recordingEventStore(eventStore), commandBus: recordingCommandBus(commandBus), queryBus } as never),
    )
    const failure = await (fixture(unclocked as never) as unknown as ReturnType<typeof built>)
      .when(command(CloseCourse, { courseId: "cs-101" }))
      .advance(1_000)
      .then()
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("recordingEventStore(store, { clock })")
  })

  it("refuses an app with more than one event store: a fixture drives one module", async () => {
    const { eventStore, commandBus, queryBus } = plain()
    const recorded = { commandBus: recordingCommandBus(commandBus), queryBus }
    const first = decisions({ ...recorded, eventStore: recordingEventStore(eventStore) } as never)
    const second = reminders({ ...recorded, eventStore: recordingEventStore(inMemoryEventStore()) } as never)
    const twoLogs = kronos({ commandHandlers: [...first.commandHandlers!, ...second.commandHandlers!] } as never)
    const failure = await fixture(twoLogs as never)
      .when(command(CloseCourse, { courseId: "cs-101" }))
      .then()
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("this app has 2 event stores")
    expect((failure as Error).message).toContain("configuration of the module under test")
  })

  it("refuses an app with more than one command bus, wrappers over one bus included", async () => {
    const { eventStore, commandBus, queryBus } = plain()
    const log = recordingEventStore(eventStore)
    const first = decisions({ eventStore: log, commandBus: recordingCommandBus(commandBus), queryBus } as never)
    const second = reminders({ eventStore: log, commandBus: recordingCommandBus(commandBus), queryBus } as never)
    const twoBuses = kronos({ commandHandlers: [...first.commandHandlers!, ...second.commandHandlers!] } as never)
    const failure = await fixture(twoBuses as never)
      .when(command(CloseCourse, { courseId: "cs-101" }))
      .then()
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("this app has 2 command buses")
  })


  it("says so when a query is the act and the app has no query bus", async () => {
    const failure = await fixture(kronos({}))
      .when(query(GetCourseView, { courseId: "x" }))
      .then()
      .catch((e: Error) => e)
    expect((failure as Error).message).toContain("a query act needs a query bus")
  })
})

// ===========================================================================
// What the fixture is handed
// ===========================================================================

describe("the app", () => {
  it("may be a promise, for an app opened asynchronously", async () => {
    const opened = (async () => app(decisions))()
    await fixture(opened)
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5))))
  })

  it("is never stopped by the fixture — the test stops it", async () => {
    let stopped = 0
    const built = app(university)
    const watched = {
      ...built,
      async stop() {
        stopped++
        await built.stop()
      },
    }
    const f = fixture(watched)
    await f.when(command(CreateCourse, intro(5))).then()
    await f.when(command(SubscribeStudent, seat("stu-001"))).then()
    expect(stopped).toBe(0)
  })

  it("carries what the app's wiring carries: correlation is the app's own", async () => {
    await built(decisions)
      .when(command(CreateCourse, intro(5)))
      .then(events(event(CourseCreated, intro(5), { correlationId: any(), causationId: any() })))
  })

  it("does not need the query bus recorded", async () => {
    await built(university)
      .given(event(CourseCreated, intro(5)))
      .when(query(GetCourseView, { courseId: "cs-101" }))
      .then(result({ courseId: "cs-101", name: "Intro", enrolled: 0, closed: false }))
  })
})

// ===========================================================================
// One fixture, many scenarios
// ===========================================================================

describe("one fixture, many scenarios", () => {
  const course = (id: string, capacity = 5) => ({ courseId: id, name: "Intro", capacity })
  const seatIn = (courseId: string, studentId: string) => ({ courseId, studentId })

  it("runs scenario after scenario on one app, each seeing only what its own act did", async () => {
    const f = built(decisions)
    await f.when(command(CreateCourse, course("c-1"))).then(events(event(CourseCreated, course("c-1"))))
    await f.when(command(CreateCourse, course("c-2"))).then(events(event(CourseCreated, course("c-2"))))
    await f
      .given(event(CourseCreated, course("c-3", 1)))
      .when(command(SubscribeStudent, seatIn("c-3", "s-1")))
      .then(events(event(StudentSubscribed, seatIn("c-3", "s-1"))), result({ remainingSeats: 0 }))
  })

  it("resets nothing: scenarios stay apart by their own ids, and share what they share", async () => {
    const f = built(decisions)
    await f.when(command(CreateCourse, course("shared"))).then()
    // The log carries on, so a later scenario about the same id meets it.
    await f.when(command(CreateCourse, course("shared"))).then(error("Course already exists"), noEvents())
  })

  it("over the whole app, a scenario sees what other slices set off; over one slice, it does not", async () => {
    const whole = built(withAutomation)
    await whole
      .given(event(CourseCreated, course("w-1", 1)))
      .when(command(SubscribeStudent, seatIn("w-1", "s-1")))
      .then(commands(command(CloseCourse, { courseId: "w-1" })))
    const slice = built(decisions)
    await slice
      .given(event(CourseCreated, course("d-1", 1)))
      .when(command(SubscribeStudent, seatIn("d-1", "s-1")))
      .then(noCommands())
  })

  it("runs scenarios one at a time, even when they are started together", async () => {
    const f = built(decisions)
    await Promise.all(
      ["p-1", "p-2", "p-3", "p-4"].map((id) =>
        f.when(command(CreateCourse, course(id))).then(events(event(CourseCreated, course(id)))),
      ),
    )
  })

  it("keeps a deadline through its scenario's `.and` steps, and never into the next scenario", async () => {
    const f = built(reminders)
    await f
      .when(command(ArmReminder, { orderId: "r-1", afterMs: 30_000 }))
      .then(scheduled(event(ReminderDue, { orderId: "r-1" }), 30_000))
      .and.when(command(ArmReminder, { orderId: "r-2", afterMs: 60_000 }))
      .advance(30_000)
      .then(events(event(ReminderArmed, { orderId: "r-2", token: any(z.string()) }), event(ReminderDue, { orderId: "r-1" })))
    // r-2's deadline was armed in the scenario above and never fired there: it does not fire here.
    await f
      .when(command(ArmReminder, { orderId: "r-3", afterMs: 1_000 }))
      .advance(60_000)
      .then(events(event(ReminderArmed, { orderId: "r-3", token: any(z.string()) }), event(ReminderDue, { orderId: "r-3" })))
  })

  it("lets the test's clock carry on from one scenario to the next", async () => {
    const clock = advanceableClock()
    const f = built(reminders, { clock })
    await f.when(command(ArmReminder, { orderId: "t-1", afterMs: 1_000 })).advance(10_000).then()
    const outcome = await f.when(command(ArmReminder, { orderId: "t-2", afterMs: 1_000 })).then()
    expect(outcome.events[0]!.timestamp).toBe(FIXTURE_EPOCH + 10_000)
    expect(clock()).toBe(FIXTURE_EPOCH + 10_000)
  })

  it("runs storylines on the same fixture as scenarios", async () => {
    const f = built(university)
    await f.when(command(CreateCourse, course("v-1", 2))).then()
    await f
      .storyline(query(GetCourseView, { courseId: "v-1" }))
      .shows({ courseId: "v-1", name: "Intro", enrolled: 0, closed: false })
      .after(event(StudentSubscribed, seatIn("v-1", "s-1")))
      .shows({ courseId: "v-1", name: "Intro", enrolled: 1, closed: false })
  })
})
