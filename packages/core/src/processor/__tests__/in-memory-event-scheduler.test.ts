import { describe, it, expect } from "bun:test"
import { qn } from "../../primitives/qualified-name.js"
import { type Metadata } from "../../primitives/metadata.js"
import type { EventMessage } from "../../messages/message.js"
import type { EventSink } from "../../buses/event-sink.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { inMemoryEventScheduler } from "../in-memory-event-scheduler.js"

function makeEvent(payload: unknown = { ok: true }): EventMessage {
  return {
    identifier: "evt-" + Math.random().toString(36).slice(2),
    name: qn("com.example", "TestEvent"),
    payload,
    metadata: new Map() as Metadata,
    timestamp: Date.now(),
    version: "1",
    tags: [],
  }
}

function makeRecordingSink(): { sink: EventSink; published: EventMessage[] } {
  const published: EventMessage[] = []
  const sink: EventSink = {
    async publish(events) {
      for (const e of events) published.push(e)
    },
  }
  return { sink, published }
}

describe("inMemoryEventScheduler", () => {
  it("schedule() with no UnitOfWork throws (must be called as ctx.schedule)", async () => {
    const { sink } = makeRecordingSink()
    const scheduler = inMemoryEventScheduler({ eventSink: sink })

    await expect(scheduler.schedule(makeEvent(), new Date())).rejects.toThrow()
    await scheduler.stop()
  })

  it("fires an event approximately at the requested time", async () => {
    const { sink, published } = makeRecordingSink()
    const scheduler = inMemoryEventScheduler({ eventSink: sink })
    const event = makeEvent({ id: 1 })

    await unitOfWork().execute(async (uow) => {
      await scheduler.schedule(event, new Date(Date.now() + 30), uow)
    })

    expect(published).toEqual([])
    await new Promise((r) => setTimeout(r, 60))
    expect(published).toHaveLength(1)
    expect(published[0]?.identifier).toBe(event.identifier)

    await scheduler.stop()
  })

  it("cancel() before fire returns 'cancelled' and the event never publishes", async () => {
    const { sink, published } = makeRecordingSink()
    const scheduler = inMemoryEventScheduler({ eventSink: sink })
    const event = makeEvent()

    const token = await unitOfWork().execute(async (uow) => {
      return scheduler.schedule(event, new Date(Date.now() + 30), uow)
    })

    const result = await scheduler.cancel(token)
    expect(result).toEqual({ kind: "cancelled" })

    await new Promise((r) => setTimeout(r, 60))
    expect(published).toEqual([])
    await scheduler.stop()
  })

  it("cancel() after fire returns 'already-appended'", async () => {
    const { sink } = makeRecordingSink()
    const scheduler = inMemoryEventScheduler({ eventSink: sink })

    const token = await unitOfWork().execute(async (uow) => {
      return scheduler.schedule(makeEvent(), new Date(Date.now()), uow)
    })

    await new Promise((r) => setTimeout(r, 20))
    const result = await scheduler.cancel(token)
    expect(result).toEqual({ kind: "already-appended" })

    await scheduler.stop()
  })

  it("cancel() with an unknown token returns 'not-found'", async () => {
    const { sink } = makeRecordingSink()
    const scheduler = inMemoryEventScheduler({ eventSink: sink })

    const result = await scheduler.cancel({ id: "no-such-token" })
    expect(result).toEqual({ kind: "not-found" })

    await scheduler.stop()
  })

  it("rolling back the UoW that schedule()d drops the schedule (token resolves 'not-found')", async () => {
    const { sink, published } = makeRecordingSink()
    const scheduler = inMemoryEventScheduler({ eventSink: sink })
    const event = makeEvent()
    let captured: { id: string } | undefined

    await expect(
      unitOfWork().execute(async (uow) => {
        captured = await scheduler.schedule(event, new Date(Date.now() + 30), uow)
        throw new Error("rollback")
      }),
    ).rejects.toThrow("rollback")

    expect(captured).toBeDefined()
    const result = await scheduler.cancel(captured!)
    expect(result).toEqual({ kind: "not-found" })

    await new Promise((r) => setTimeout(r, 60))
    expect(published).toEqual([])
    await scheduler.stop()
  })

  it("calling cancel() twice on the same token returns 'cancelled' then 'not-found'", async () => {
    // After cancel transitions a row to 'cancelled', subsequent cancels see
    // the row as effectively gone — distinguishing this from the 'cancelled'
    // case would require a fourth result kind, which the design rejects.
    const { sink } = makeRecordingSink()
    const scheduler = inMemoryEventScheduler({ eventSink: sink })

    const token = await unitOfWork().execute(async (uow) => {
      return scheduler.schedule(makeEvent(), new Date(Date.now() + 1000), uow)
    })

    expect(await scheduler.cancel(token)).toEqual({ kind: "cancelled" })
    expect(await scheduler.cancel(token)).toEqual({ kind: "not-found" })

    await scheduler.stop()
  })

  it("stop() clears armed timers — events do not fire after stop", async () => {
    const { sink, published } = makeRecordingSink()
    const scheduler = inMemoryEventScheduler({ eventSink: sink })

    await unitOfWork().execute(async (uow) => {
      await scheduler.schedule(makeEvent(), new Date(Date.now() + 20), uow)
    })

    await scheduler.stop()
    await new Promise((r) => setTimeout(r, 50))
    expect(published).toEqual([])
  })
})
