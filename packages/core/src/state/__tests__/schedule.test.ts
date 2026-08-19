import { describe, it, expect, mock } from "bun:test"
import { qn } from "../../primitives/qualified-name.js"
import { emptyMetadata } from "../../primitives/metadata.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { scheduleFunctions } from "../schedule.js"

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
    schedule: mock(async (event: any, at: Date, _uow?: unknown) => {
      scheduled.push({ event, at })
      return { id: `tok-${scheduled.length}` }
    }),
    cancel: mock(async (token: unknown, _uow?: unknown) => {
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
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
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

  it("merges the active UoW correlation data onto the scheduled event", async () => {
    const sm = mockScheduler()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
      // Simulates what the invocation wrapper contributes from the incoming
      // message — lineage lives on the unit of work, not in its metadata.
      uow.contributeCorrelationData({ correlationId: "corr-1", causationId: "cause-1" })
      await schedule(descriptor, { id: "A" }, new Date(Date.now() + 60_000))
    })

    expect(sm.scheduled[0]!.event.metadata).toMatchObject({
      correlationId: "corr-1",
      causationId: "cause-1",
    })
  })

  it("merges correlation data over explicitly provided metadata", async () => {
    const sm = mockScheduler()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
      uow.contributeCorrelationData({ correlationId: "corr-1", causationId: "cause-1" })
      await schedule(descriptor, { id: "A" }, new Date(Date.now() + 60_000), { tenant: "acme" } as any)
    })

    expect(sm.scheduled[0]!.event.metadata).toEqual({
      tenant: "acme",
      correlationId: "corr-1",
      causationId: "cause-1",
    })
  })

  it("leaves event metadata untouched when no correlation data is set", async () => {
    const sm = mockScheduler()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
      await schedule(descriptor, { id: "A" }, new Date(Date.now() + 60_000), { tenant: "acme" } as any)
    })

    expect(sm.scheduled[0]!.event.metadata).toEqual({ tenant: "acme" })
  })

  it("scheduleAfter fires delayMs from now", async () => {
    const sm = mockScheduler()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
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
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
      result = await cancelSchedule({ id: "tok-1" })
    })
    expect(result).toEqual({ kind: "cancelled" })
    expect(sm.cancelled).toEqual([{ id: "tok-1" }])
  })

  it("throws when no scheduler is configured", async () => {
    await unitOfWork().execute(async (uow) => {
      const { schedule } = scheduleFunctions({ uow })
      await expect(schedule(descriptor, { id: "X" }, new Date())).rejects.toThrow(/event scheduler/)
    })
  })

  it("throws outside the INVOCATION phase", async () => {
    const { schedule } = scheduleFunctions({ uow: unitOfWork(), eventScheduler: mockScheduler() as never })
    await expect(schedule(descriptor, { id: "X" }, new Date())).rejects.toThrow()
  })

  it("hands the unit of work to the scheduler so the insert joins its transaction", async () => {
    const sm = mockScheduler()
    let captured: unknown
    await unitOfWork().execute(async (uow) => {
      captured = uow
      const { schedule } = scheduleFunctions({ uow, eventScheduler: sm as never })
      await schedule(descriptor, { id: "A" }, new Date(Date.now() + 60_000))
    })
    expect(sm.schedule.mock.calls[0]![2]).toBe(captured)
  })

  it("rejects an Invalid Date `at`", async () => {
    const sm = mockScheduler()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
      await expect(schedule(descriptor, { id: "X" }, new Date(NaN))).rejects.toThrow(/valid Date/)
    })
    expect(sm.scheduled).toHaveLength(0)
  })

  it("allows a past `at` (fires ASAP)", async () => {
    const sm = mockScheduler()
    const past = new Date(Date.now() - 60_000)
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
      await schedule(descriptor, { id: "P" }, past)
    })
    expect(sm.scheduled).toHaveLength(1)
    expect(sm.scheduled[0]!.at).toBe(past)
  })

  it("scheduleAfter rejects a non-finite delay", async () => {
    const sm = mockScheduler()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
      await expect(scheduleAfter(descriptor, { id: "X" }, NaN)).rejects.toThrow(/finite number/)
      await expect(scheduleAfter(descriptor, { id: "X" }, Infinity)).rejects.toThrow(/finite number/)
    })
    expect(sm.scheduled).toHaveLength(0)
  })

  it("scheduleAfter allows a negative delay (fires ASAP)", async () => {
    const sm = mockScheduler()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventScheduler: sm as never,
      })
      const before = Date.now()
      await scheduleAfter(descriptor, { id: "N" }, -5_000)
      expect(sm.scheduled[0]!.at.getTime()).toBeLessThanOrEqual(before)
    })
  })
})
