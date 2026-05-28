import { describe, it, expect } from "bun:test"
import { qn, type Metadata } from "@kronos-ts/common"
import type { EventMessage } from "../message.js"
import type { EventSink } from "../event-sink.js"
import { runInNewUoW } from "../unit-of-work.js"
import { createInMemoryEventScheduler } from "../in-memory-event-scheduler.js"

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

describe("createInMemoryEventScheduler", () => {
  it("schedule() outside a UoW throws (must be called from INVOCATION phase)", async () => {
    const { sink } = makeRecordingSink()
    const scheduler = createInMemoryEventScheduler({ eventSink: sink })

    await expect(scheduler.schedule(makeEvent(), new Date())).rejects.toThrow()
    await scheduler.stop()
  })

  it("fires an event approximately at the requested time", async () => {
    const { sink, published } = makeRecordingSink()
    const scheduler = createInMemoryEventScheduler({ eventSink: sink })
    const event = makeEvent({ id: 1 })

    await runInNewUoW(undefined, async () => {
      await scheduler.schedule(event, new Date(Date.now() + 30))
    })

    expect(published).toEqual([])
    await new Promise((r) => setTimeout(r, 60))
    expect(published).toHaveLength(1)
    expect(published[0]?.identifier).toBe(event.identifier)

    await scheduler.stop()
  })

  it("cancel() before fire returns 'cancelled' and the event never publishes", async () => {
    const { sink, published } = makeRecordingSink()
    const scheduler = createInMemoryEventScheduler({ eventSink: sink })
    const event = makeEvent()

    const token = await runInNewUoW(undefined, async () => {
      return scheduler.schedule(event, new Date(Date.now() + 30))
    })

    const result = await scheduler.cancel(token)
    expect(result).toEqual({ kind: "cancelled" })

    await new Promise((r) => setTimeout(r, 60))
    expect(published).toEqual([])
    await scheduler.stop()
  })

  it("cancel() after fire returns 'already-appended'", async () => {
    const { sink } = makeRecordingSink()
    const scheduler = createInMemoryEventScheduler({ eventSink: sink })

    const token = await runInNewUoW(undefined, async () => {
      return scheduler.schedule(makeEvent(), new Date(Date.now()))
    })

    await new Promise((r) => setTimeout(r, 20))
    const result = await scheduler.cancel(token)
    expect(result).toEqual({ kind: "already-appended" })

    await scheduler.stop()
  })

  it("cancel() with an unknown token returns 'not-found'", async () => {
    const { sink } = makeRecordingSink()
    const scheduler = createInMemoryEventScheduler({ eventSink: sink })

    const result = await scheduler.cancel({ id: "no-such-token" })
    expect(result).toEqual({ kind: "not-found" })

    await scheduler.stop()
  })

  it("rolling back the UoW that schedule()d drops the schedule (token resolves 'not-found')", async () => {
    const { sink, published } = makeRecordingSink()
    const scheduler = createInMemoryEventScheduler({ eventSink: sink })
    const event = makeEvent()
    let captured: { id: string } | undefined

    await expect(
      runInNewUoW(undefined, async () => {
        captured = await scheduler.schedule(event, new Date(Date.now() + 30))
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
    const scheduler = createInMemoryEventScheduler({ eventSink: sink })

    const token = await runInNewUoW(undefined, async () => {
      return scheduler.schedule(makeEvent(), new Date(Date.now() + 1000))
    })

    expect(await scheduler.cancel(token)).toEqual({ kind: "cancelled" })
    expect(await scheduler.cancel(token)).toEqual({ kind: "not-found" })

    await scheduler.stop()
  })

  it("stop() clears armed timers — events do not fire after stop", async () => {
    const { sink, published } = makeRecordingSink()
    const scheduler = createInMemoryEventScheduler({ eventSink: sink })

    await runInNewUoW(undefined, async () => {
      await scheduler.schedule(makeEvent(), new Date(Date.now() + 20))
    })

    await scheduler.stop()
    await new Promise((r) => setTimeout(r, 50))
    expect(published).toEqual([])
  })
})
