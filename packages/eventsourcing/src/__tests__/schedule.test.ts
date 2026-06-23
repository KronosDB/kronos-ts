import { describe, it, expect, mock } from "bun:test"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { runInNewUoW } from "@kronos-ts/messaging"
import { setResource } from "@kronos-ts/messaging/processing-state"
import { schedule, scheduleAfter, cancelSchedule, EVENT_SCHEDULER_KEY } from "../schedule.js"

const EVENT_NAME = qn("test", "Scheduled")

// Minimal EventDescriptor shape the helper reads (name, version, tags).
const descriptor = {
  name: EVENT_NAME,
  version: "1.0",
  tags: (p: { id: string }) => [{ key: "id", value: p.id }],
} as any

function mockScheduler() {
  const scheduled: Array<{ event: any; at: Date }> = []
  const cancelled: unknown[] = []
  return {
    scheduled,
    cancelled,
    schedule: mock(async (event: any, at: Date) => {
      scheduled.push({ event, at })
      return { id: `tok-${scheduled.length}` }
    }),
    cancel: mock(async (token: unknown) => {
      cancelled.push(token)
      return { kind: "cancelled" as const }
    }),
  }
}

describe("schedule helpers", () => {
  it("builds the event message (name/version/payload/tags) and delegates to the scheduler", async () => {
    const sm = mockScheduler()
    const at = new Date(Date.now() + 60_000)
    let token: unknown
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(EVENT_SCHEDULER_KEY, sm as any)
      token = await schedule(descriptor, { id: "A", v: 1 }, at)
    })

    expect(token).toEqual({ id: "tok-1" })
    expect(sm.scheduled).toHaveLength(1)
    const { event, at: usedAt } = sm.scheduled[0]!
    expect(event.name).toEqual(EVENT_NAME)
    expect(event.version).toBe("1.0")
    expect(event.payload).toEqual({ id: "A", v: 1 })
    expect(event.tags).toEqual([{ key: "id", value: "A" }])
    expect(event.identifier).toBeDefined()
    expect(usedAt).toBe(at)
  })

  it("scheduleAfter fires delayMs from now", async () => {
    const sm = mockScheduler()
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(EVENT_SCHEDULER_KEY, sm as any)
      const before = Date.now()
      await scheduleAfter(descriptor, { id: "B" }, 5_000)
      const at = sm.scheduled[0]!.at.getTime()
      expect(at).toBeGreaterThanOrEqual(before + 5_000)
      expect(at).toBeLessThanOrEqual(Date.now() + 5_000)
    })
  })

  it("cancelSchedule delegates to the scheduler", async () => {
    const sm = mockScheduler()
    let result: unknown
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(EVENT_SCHEDULER_KEY, sm as any)
      result = await cancelSchedule({ id: "tok-1" })
    })
    expect(result).toEqual({ kind: "cancelled" })
    expect(sm.cancelled).toEqual([{ id: "tok-1" }])
  })

  it("throws when no scheduler is configured", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      await expect(schedule(descriptor, { id: "X" }, new Date())).rejects.toThrow(/event scheduler/)
    })
  })

  it("throws outside a UnitOfWork", async () => {
    await expect(schedule(descriptor, { id: "X" }, new Date())).rejects.toThrow()
  })

  it("rejects an Invalid Date `at`", async () => {
    const sm = mockScheduler()
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(EVENT_SCHEDULER_KEY, sm as any)
      await expect(schedule(descriptor, { id: "X" }, new Date(NaN))).rejects.toThrow(/valid Date/)
    })
    expect(sm.scheduled).toHaveLength(0)
  })

  it("allows a past `at` (fires ASAP)", async () => {
    const sm = mockScheduler()
    const past = new Date(Date.now() - 60_000)
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(EVENT_SCHEDULER_KEY, sm as any)
      await schedule(descriptor, { id: "P" }, past)
    })
    expect(sm.scheduled).toHaveLength(1)
    expect(sm.scheduled[0]!.at).toBe(past)
  })

  it("scheduleAfter rejects a non-finite delay", async () => {
    const sm = mockScheduler()
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(EVENT_SCHEDULER_KEY, sm as any)
      await expect(scheduleAfter(descriptor, { id: "X" }, NaN)).rejects.toThrow(/finite number/)
      await expect(scheduleAfter(descriptor, { id: "X" }, Infinity)).rejects.toThrow(/finite number/)
    })
    expect(sm.scheduled).toHaveLength(0)
  })

  it("scheduleAfter allows a negative delay (fires ASAP)", async () => {
    const sm = mockScheduler()
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(EVENT_SCHEDULER_KEY, sm as any)
      const before = Date.now()
      await scheduleAfter(descriptor, { id: "N" }, -5_000)
      expect(sm.scheduled[0]!.at.getTime()).toBeLessThanOrEqual(before)
    })
  })
})
