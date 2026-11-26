import { afterEach, describe, expect, it } from "bun:test"
import { emptyMetadata, qn, type TaggedEvent } from "../../messaging/messages.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { inMemorySchedulingEventStore } from "../in-memory-scheduling-event-store.js"
import type { ScheduleToken } from "../scheduler.js"

const DELAY_MS = 40
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

function reminder(id: string): TaggedEvent {
  return {
    event: {
      kind: "event",
      identifier: id,
      name: qn("test", "ReminderDue"),
      version: "1.0",
      payload: { id },
      metadata: emptyMetadata(),
      timestamp: 0,
    },
    tags: [],
  }
}

/** A scheduling store over a log that only records what is appended to it. */
function schedulingStore() {
  const appended: string[] = []
  const log = {
    append: async (events: ReadonlyArray<TaggedEvent>) => {
      for (const { event } of events) appended.push(event.identifier)
    },
  } as unknown as EventStore
  return { store: inMemorySchedulingEventStore(log), appended }
}

/** Run `work` in a unit of work that then fails. */
async function inFailedTask(work: (uow: ReturnType<typeof unitOfWork>) => Promise<void>): Promise<void> {
  await unitOfWork()
    .execute(async (uow) => {
      await work(uow)
      throw new Error("task failed")
    })
    .catch((error: Error) => {
      if (error.message !== "task failed") throw error
    })
}

describe("inMemorySchedulingEventStore", () => {
  let stop: (() => Promise<void>) | undefined
  afterEach(async () => {
    await stop?.()
    stop = undefined
  })

  function started() {
    const made = schedulingStore()
    stop = () => made.store.stopScheduling()
    return made
  }

  /** Schedule `id` to fire after `delayMs`, in a unit of work that commits. */
  function scheduled(store: ReturnType<typeof schedulingStore>["store"], id: string, delayMs = DELAY_MS): Promise<ScheduleToken> {
    return unitOfWork().execute((uow) => store.schedule(reminder(id), new Date(Date.now() + delayMs), uow))
  }

  it("fires a committed schedule at its time", async () => {
    const { store, appended } = started()
    await scheduled(store, "r-1")

    expect(appended).toEqual([])
    await sleep(DELAY_MS * 3)
    expect(appended).toEqual(["r-1"])
  })

  it("a cancel in a unit of work that commits stays cancelled", async () => {
    const { store, appended } = started()
    const token = await scheduled(store, "r-1")

    const result = await unitOfWork().execute((uow) => store.cancelSchedule(token, uow))

    expect(result).toEqual({ kind: "cancelled" })
    await sleep(DELAY_MS * 3)
    expect(appended).toEqual([])
    expect(await store.cancelSchedule(token)).toEqual({ kind: "not-found" })
  })

  it("a cancel in a unit of work that fails is undone, and the schedule fires at its time", async () => {
    const { store, appended } = started()
    const token = await scheduled(store, "r-1")

    await inFailedTask(async (uow) => {
      expect(await store.cancelSchedule(token, uow)).toEqual({ kind: "cancelled" })
    })

    expect(appended).toEqual([])
    await sleep(DELAY_MS * 3)
    expect(appended).toEqual(["r-1"])
    expect(await store.cancelSchedule(token)).toEqual({ kind: "already-appended" })
  })

  it("a cancel undone after the fire time has passed fires the schedule at once", async () => {
    const { store, appended } = started()
    const token = await scheduled(store, "r-1")

    await inFailedTask(async (uow) => {
      await store.cancelSchedule(token, uow)
      await sleep(DELAY_MS * 2)
      expect(appended).toEqual([])
    })

    await sleep(DELAY_MS)
    expect(appended).toEqual(["r-1"])
  })

  it("a schedule cancelled again after an undone cancel does not fire", async () => {
    const { store, appended } = started()
    const token = await scheduled(store, "r-1")
    await inFailedTask(async (uow) => {
      await store.cancelSchedule(token, uow)
    })

    expect(await store.cancelSchedule(token)).toEqual({ kind: "cancelled" })

    await sleep(DELAY_MS * 3)
    expect(appended).toEqual([])
  })

  it("a schedule and its cancel in one failed unit of work leave nothing behind", async () => {
    const { store, appended } = started()
    let token: ScheduleToken | undefined

    await inFailedTask(async (uow) => {
      token = await store.schedule(reminder("r-1"), new Date(Date.now() + DELAY_MS), uow)
      expect(await store.cancelSchedule(token, uow)).toEqual({ kind: "cancelled" })
    })

    expect(await store.cancelSchedule(token!)).toEqual({ kind: "not-found" })
    await sleep(DELAY_MS * 3)
    expect(appended).toEqual([])
  })

  it("a cancel stands when the unit of work fails after its commit", async () => {
    const { store, appended } = started()
    const token = await scheduled(store, "r-1")

    await unitOfWork()
      .execute(async (uow) => {
        await store.cancelSchedule(token, uow)
        uow.onAfterCommit(() => {
          throw new Error("after commit")
        })
      })
      .catch(() => {})

    await sleep(DELAY_MS * 3)
    expect(appended).toEqual([])
  })
})
