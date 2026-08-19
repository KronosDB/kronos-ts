import { describe, expect, it } from "bun:test"
import {
  emptyMetadata,
  generateIdentifier,
  inMemoryEventStore,
  qn,
  send,
  simpleCommandBus,
  simpleQueryBus,
  unitOfWork,
} from "@kronos-ts/core"
import type { EventDescriptor, EventMessage } from "@kronos-ts/core"
import {
  controllableScheduler,
  recordingCommandBus,
  recordingEventStore,
  recordingQueryBus,
} from "../recording.js"
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
    tags: descriptor.tags ? descriptor.tags(payload) : [],
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
    const bus = recordingCommandBus(simpleCommandBus(() => unitOfWork(() => FROZEN)))
    bus.subscribe("university.CloseCourse", async () => "closed")

    const result = await send(bus, CloseCourse, { courseId: "cs-101" })

    expect(result).toBe("closed")
    expect(bus.dispatched.map((m) => m.name.name)).toEqual(["CloseCourse"])
    expect(bus.dispatched[0]!.payload).toEqual({ courseId: "cs-101" })

    bus.reset()
    expect(bus.dispatched).toHaveLength(0)
  })

  it("records the commands a handler dispatches AFTER the one that caused them", async () => {
    const bus = recordingCommandBus(simpleCommandBus(() => unitOfWork(() => FROZEN)))
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
    const bus = recordingCommandBus(simpleCommandBus(() => unitOfWork(() => FROZEN)))
    bus.subscribe("university.CloseCourse", async () => undefined)
    expect(() => bus.subscribe("university.CloseCourse", async () => undefined)).toThrow(
      "A different handler is already registered",
    )
  })
})

describe("recordingQueryBus", () => {
  it("records what is asked, and answers it", async () => {
    const bus = recordingQueryBus(simpleQueryBus(() => unitOfWork(() => FROZEN)))
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

describe("controllableScheduler", () => {
  it("fires nothing until the clock is moved and `due` is asked", async () => {
    let now = FROZEN
    const scheduler = controllableScheduler(() => now)

    const token = await unitOfWork(() => now).execute((uow) =>
      scheduler.schedule(message(CourseClosed, { courseId: "cs-101" }), new Date(now + 1_000), uow),
    )

    expect(scheduler.schedules).toHaveLength(1)
    expect(scheduler.due()).toHaveLength(0)

    now = FROZEN + 999
    expect(scheduler.due()).toHaveLength(0)

    now = FROZEN + 1_000
    const fired = scheduler.due()
    expect(fired).toHaveLength(1)
    // Born when it fires, not when it was arranged.
    expect(fired[0]!.timestamp).toBe(FROZEN + 1_000)
    // And never twice.
    expect(scheduler.due()).toHaveLength(0)
    expect(scheduler.schedules[0]!.status).toBe("fired")
    expect(token.id).toBeDefined()
  })

  it("fires in fire-time order, not in the order they were arranged", async () => {
    let now = FROZEN
    const scheduler = controllableScheduler(() => now)

    await unitOfWork(() => now).execute(async (uow) => {
      await scheduler.schedule(
        message(CourseClosed, { courseId: "late" }),
        new Date(now + 900),
        uow,
      )
      await scheduler.schedule(
        message(CourseClosed, { courseId: "early" }),
        new Date(now + 100),
        uow,
      )
    })

    now = FROZEN + 1_000
    expect(scheduler.due().map((e) => (e.payload as { courseId: string }).courseId)).toEqual([
      "early",
      "late",
    ])
  })

  it("a cancelled schedule never fires, and says it was cancelled", async () => {
    let now = FROZEN
    const scheduler = controllableScheduler(() => now)

    await unitOfWork(() => now).execute(async (uow) => {
      const token = await scheduler.schedule(
        message(CourseClosed, { courseId: "cs-101" }),
        new Date(now + 1_000),
        uow,
      )
      expect(await scheduler.cancel(token, uow)).toEqual({ kind: "cancelled" })
    })

    now = FROZEN + 5_000
    expect(scheduler.due()).toHaveLength(0)
    expect(scheduler.schedules[0]!.status).toBe("cancelled")
  })

  it("a schedule a rolled-back handler asked for was never asked for", async () => {
    let now = FROZEN
    const scheduler = controllableScheduler(() => now)

    await unitOfWork(() => now)
      .execute(async (uow) => {
        await scheduler.schedule(
          message(CourseClosed, { courseId: "cs-101" }),
          new Date(now + 1_000),
          uow,
        )
        throw new Error("the handler blew up")
      })
      .catch(() => undefined)

    now = FROZEN + 5_000
    expect(scheduler.schedules).toHaveLength(0)
    expect(scheduler.due()).toHaveLength(0)
  })

  it("refuses to be called outside a handler", async () => {
    const scheduler = controllableScheduler(() => FROZEN)
    await expect(
      scheduler.schedule(message(CourseClosed, { courseId: "cs-101" }), new Date(FROZEN)),
    ).rejects.toThrow("requires a UnitOfWork")
  })

  it("reports a token it has never seen as not-found", async () => {
    const scheduler = controllableScheduler(() => FROZEN)
    expect(await scheduler.cancel({ id: "nope" })).toEqual({ kind: "not-found" })
  })
})
