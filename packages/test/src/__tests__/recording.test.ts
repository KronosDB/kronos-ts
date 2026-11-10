import { describe, expect, it } from "bun:test"
import {
  emptyMetadata,
  generateIdentifier,
  inMemoryEventStore,
  inMemorySchedulingEventStore,
  qn,
  send,
  localCommandBus,
  localQueryBus,
  unitOfWork,
  tagsOf,
} from "@kronos-ts/core"
import type { EventDescriptor, EventMessage } from "@kronos-ts/core"
import { advanceableClock } from "../clock.js"
import { recordingCommandBus, recordingEventStore, recordingQueryBus } from "../recording.js"
import { CloseCourse, CourseClosed, GetCourseView } from "./_university.js"

// ---------------------------------------------------------------------------
// The recorders on their own: thing-first, same shape, readable log. Nothing
// here needs a fixture — which is the point of them being decorators.
// ---------------------------------------------------------------------------

const FROZEN = 1_700_000_000_000

function message(descriptor: EventDescriptor<any>, payload: any): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: descriptor.name,
    version: descriptor.version,
    payload,
    metadata: emptyMetadata(),
    timestamp: FROZEN,
    tags: tagsOf(descriptor, payload, emptyMetadata()),
  }
}

describe("recordingEventStore", () => {
  it("wraps the store it is given and starts empty", () => {
    expect(recordingEventStore(inMemoryEventStore()).appended).toHaveLength(0)
  })

  it("records what `append` commits, in order, and still serves it", async () => {
    const store = recordingEventStore(inMemoryEventStore())

    await store.append([message(CourseClosed, { courseId: "cs-101" })])
    await store.append([message(CourseClosed, { courseId: "cs-202" })])

    expect(store.appended.map((e) => (e.payload as { courseId: string }).courseId)).toEqual([
      "cs-101",
      "cs-202",
    ])
    const sourced = await store.source({ query: { tags: { courseId: "cs-101" } } })
    expect(sourced.events).toHaveLength(1)
    expect(await store.getHeadPosition()).toBe(2n)
  })

  it("records a two-phase append at COMMIT, and nothing at all on rollback", async () => {
    const store = recordingEventStore(inMemoryEventStore())

    const staged = await store.appendEvents([message(CourseClosed, { courseId: "staged" })])
    expect(store.appended).toHaveLength(0)
    await staged.commit()
    expect(store.appended).toHaveLength(1)

    const doomed = await store.appendEvents([message(CourseClosed, { courseId: "doomed" })])
    doomed.rollback()
    expect(store.appended).toHaveLength(1)
  })

  it("`reset` forgets the recording, not the events", async () => {
    const store = recordingEventStore(inMemoryEventStore())
    await store.append([message(CourseClosed, { courseId: "cs-101" })])

    store.reset()

    expect(store.appended).toHaveLength(0)
    expect(await store.getHeadPosition()).toBe(1n)
  })
})

describe("recordingCommandBus", () => {
  it("records at entry, delegates, and returns the handler's result", async () => {
    const bus = recordingCommandBus(localCommandBus(() => unitOfWork(() => FROZEN)))
    bus.subscribe("university.CloseCourse", async () => "closed")

    const result = await send(bus, CloseCourse, { courseId: "cs-101" })

    expect(result).toBe("closed")
    expect(bus.dispatched.map((m) => m.name.name)).toEqual(["CloseCourse"])
    expect(bus.dispatched[0]!.payload).toEqual({ courseId: "cs-101" })

    bus.reset()
    expect(bus.dispatched).toHaveLength(0)
  })

  it("records the commands a handler dispatches AFTER the one that caused them", async () => {
    const bus = recordingCommandBus(localCommandBus(() => unitOfWork(() => FROZEN)))
    let nested = false
    bus.subscribe("university.CloseCourse", async (message) => {
      if (nested) return undefined
      nested = true
      await bus.dispatch({ ...message, identifier: "nested" })
      return undefined
    })

    await send(bus, CloseCourse, { courseId: "cs-101" })

    expect(bus.dispatched.map((m) => m.identifier).at(-1)).toBe("nested")
  })

  it("keeps the delegate's subscription rules — it adds nothing but the log", () => {
    const bus = recordingCommandBus(localCommandBus(() => unitOfWork(() => FROZEN)))
    bus.subscribe("university.CloseCourse", async () => undefined)
    expect(() => bus.subscribe("university.CloseCourse", async () => undefined)).toThrow(
      "A different handler is already registered",
    )
  })
})

describe("recordingQueryBus", () => {
  it("records what is asked, and answers it", async () => {
    const bus = recordingQueryBus(localQueryBus(() => unitOfWork(() => FROZEN)))
    bus.subscribe("university.GetCourseView", async () => ({ courseId: "cs-101" }))

    const answer = await bus.query({
      kind: "query",
      identifier: generateIdentifier(),
      name: GetCourseView.name,
      payload: { courseId: "cs-101" },
      metadata: emptyMetadata(),
    })

    expect(answer).toEqual({ courseId: "cs-101" })
    expect(bus.queried.map((m) => m.name.name)).toEqual(["GetCourseView"])
  })
})

describe("recordingEventStore with a clock", () => {
  const clocked = () => {
    const clock = advanceableClock(FROZEN)
    return { clock, store: recordingEventStore(inMemoryEventStore(), { clock }) }
  }
  const courses = (events: ReadonlyArray<EventMessage>) => events.map((e) => (e.payload as { courseId: string }).courseId)

  it("holds a deadline until `advance` reaches it, then appends it through the store, recorded", async () => {
    const { clock, store } = clocked()

    const token = await unitOfWork(clock).execute((uow) =>
      store.schedule(message(CourseClosed, { courseId: "cs-101" }), new Date(FROZEN + 1_000), uow),
    )
    expect(store.schedules).toHaveLength(1)

    await store.advance(999)
    expect(store.appended).toHaveLength(0)

    await store.advance(1)
    expect(courses(store.appended)).toEqual(["cs-101"])
    // Born when it fires, not when it was arranged.
    expect(store.appended[0]!.timestamp).toBe(FROZEN + 1_000)
    // In the real log, too.
    expect(await store.getHeadPosition()).toBe(1n)
    // And never twice.
    await store.advance(10_000)
    expect(store.appended).toHaveLength(1)
    expect(store.schedules[0]!.status).toBe("fired")
    expect(token.id).toBeDefined()
    expect(store.clock).toBe(clock)
  })

  it("fires in fire-time order, not in the order they were arranged", async () => {
    const { clock, store } = clocked()

    await unitOfWork(clock).execute(async (uow) => {
      await store.schedule(message(CourseClosed, { courseId: "late" }), new Date(FROZEN + 900), uow)
      await store.schedule(message(CourseClosed, { courseId: "early" }), new Date(FROZEN + 100), uow)
    })

    await store.advance(1_000)
    expect(courses(store.appended)).toEqual(["early", "late"])
  })

  it("a cancelled schedule never fires, and says it was cancelled", async () => {
    const { clock, store } = clocked()

    await unitOfWork(clock).execute(async (uow) => {
      const token = await store.schedule(message(CourseClosed, { courseId: "cs-101" }), new Date(FROZEN + 1_000), uow)
      expect(await store.cancelSchedule(token, uow)).toEqual({ kind: "cancelled" })
    })

    await store.advance(5_000)
    expect(store.appended).toHaveLength(0)
    expect(store.schedules[0]!.status).toBe("cancelled")
  })

  it("a schedule a rolled-back handler asked for was never asked for", async () => {
    const { clock, store } = clocked()

    await unitOfWork(clock)
      .execute(async (uow) => {
        await store.schedule(message(CourseClosed, { courseId: "cs-101" }), new Date(FROZEN + 1_000), uow)
        throw new Error("the handler blew up")
      })
      .catch(() => undefined)

    await store.advance(5_000)
    expect(store.schedules).toHaveLength(0)
    expect(store.appended).toHaveLength(0)
  })

  it("holds the deadlines in place of the store's own scheduling tier", async () => {
    const clock = advanceableClock(FROZEN)
    const store = recordingEventStore(inMemorySchedulingEventStore(inMemoryEventStore(), { clock }), { clock })

    await unitOfWork(clock).execute((uow) =>
      store.schedule(message(CourseClosed, { courseId: "cs-101" }), new Date(FROZEN), uow),
    )
    // A timer-driven tier would have appended by now; the held book waits for `advance`.
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(store.appended).toHaveLength(0)
    await store.advance(0)
    expect(courses(store.appended)).toEqual(["cs-101"])
  })

  it("while held, arms nothing and cancels nothing", async () => {
    const { clock, store } = clocked()

    store.hold(true)
    await unitOfWork(clock).execute((uow) =>
      store.schedule(message(CourseClosed, { courseId: "cs-101" }), new Date(FROZEN + 1_000), uow),
    )
    store.hold(false)

    expect(store.schedules).toHaveLength(0)
  })

  it("refuses to be called outside a handler", async () => {
    const { store } = clocked()
    await expect(store.schedule(message(CourseClosed, { courseId: "cs-101" }), new Date(FROZEN))).rejects.toThrow(
      "requires a UnitOfWork",
    )
  })

  it("reports a token it has never seen as not-found", async () => {
    const { store } = clocked()
    expect(await store.cancelSchedule({ id: "nope" })).toEqual({ kind: "not-found" })
  })

  it("without a clock, has no deadline book of its own", () => {
    const store = recordingEventStore(inMemoryEventStore())
    expect("advance" in store).toBe(false)
    expect("schedules" in store).toBe(false)
  })
})
