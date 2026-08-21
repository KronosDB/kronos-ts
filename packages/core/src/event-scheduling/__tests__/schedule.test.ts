import { describe, it, expect, mock } from "bun:test"
import { qn, emptyMetadata, type Message } from "../../messaging/messages.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { correlating } from "../../correlation/correlating.js"
import { correlatingHandler } from "../../correlation/correlating-handler.js"
import { scheduleFunctions } from "../schedule.js"
// The id-pair cargo, written out as any host writes it: the chain is inherited
// or seeded; the cause is the parent, unconditionally.
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

/** The message a handler is handling, in the shape the cargo function reads. */
const causingCommand = {
  kind: "command",
  identifier: "cmd-1",
  name: qn("test", "Cause"),
  payload: {},
  metadata: { correlationId: "corr-1" },
  timestamp: 0,
} as any

const EVENT_NAME = qn("test", "Scheduled")

// Minimal EventDescriptor shape the helper reads (name, version, tags).
const descriptor = {
  name: EVENT_NAME,
  version: "1.0",
  tags: (p: { id: string }) => [{ key: "id", value: p.id }],
} as any

/**
 * A LOG THAT CAN SCHEDULE, in the shape `scheduleFunctions` reads: the two
 * capability members and nothing else. It used to be a standalone
 * `EventScheduler`; it is a tier on the store now, so the helper reads it off
 * `eventStore` and the mock is named for what it stands in for.
 */
function mockSchedulingLog() {
  const scheduled: Array<{ event: any; at: Date }> = []
  const cancelled: unknown[] = []
  return {
    scheduled,
    cancelled,
    schedule: mock(async (event: any, at: Date, _uow?: unknown) => {
      scheduled.push({ event, at })
      return { id: `tok-${scheduled.length}` }
    }),
    cancelSchedule: mock(async (token: unknown, _uow?: unknown) => {
      cancelled.push(token)
      return { kind: "cancelled" as const }
    }),
  }
}

describe("schedule helpers", () => {
  it("builds the event message (name/version/payload/tags) and delegates to the scheduler", async () => {
    const sm = mockSchedulingLog()
    const at = new Date(Date.now() + 60_000)
    let token: unknown
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventStore: sm as never,
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

  it("carries the handled message's correlation onto the scheduled event", async () => {
    // SCHEDULE-TIME is the only point where this can happen: when the schedule
    // fires there is no originating task left to ask. The verb itself carries
    // nothing — `correlatingHandler` overlays through its `metadata` parameter.
    const sm = mockSchedulingLog()
    const uow = correlating(unitOfWork())
    await uow.execute(async () => {
      const ctx = { ...scheduleFunctions({ uow, eventStore: sm as never }), unitOfWork: uow }
      const handler = correlatingHandler(async (_m: any, c: typeof ctx) => {
        await c.schedule(descriptor, { id: "A" }, new Date(Date.now() + 60_000))
      }, correlationFrom)
      await handler(causingCommand, ctx)
    })

    expect(sm.scheduled[0]!.event.metadata).toMatchObject({
      correlationId: "corr-1",
      causationId: "cmd-1",
    })
  })

  it("lets explicitly provided metadata ride alongside the carried correlation", async () => {
    const sm = mockSchedulingLog()
    const uow = correlating(unitOfWork())
    await uow.execute(async () => {
      const ctx = { ...scheduleFunctions({ uow, eventStore: sm as never }), unitOfWork: uow }
      const handler = correlatingHandler(async (_m: any, c: typeof ctx) => {
        await c.schedule(descriptor, { id: "A" }, new Date(Date.now() + 60_000), {
          tenant: "acme",
        } as any)
      }, correlationFrom)
      await handler(causingCommand, ctx)
    })

    expect(sm.scheduled[0]!.event.metadata).toEqual({
      tenant: "acme",
      correlationId: "corr-1",
      causationId: "cmd-1",
    })
  })

  it("carries nothing at all when the handler was never wrapped", async () => {
    const sm = mockSchedulingLog()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventStore: sm as never,
      })
      await schedule(descriptor, { id: "A" }, new Date(Date.now() + 60_000), { tenant: "acme" } as any)
    })

    expect(sm.scheduled[0]!.event.metadata).toEqual({ tenant: "acme" })
  })

  it("scheduleAfter fires delayMs from now", async () => {
    const sm = mockSchedulingLog()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventStore: sm as never,
      })
      const before = Date.now()
      await scheduleAfter(descriptor, { id: "B" }, 5_000)
      const at = sm.scheduled[0]!.at.getTime()
      expect(at).toBeGreaterThanOrEqual(before + 5_000)
      expect(at).toBeLessThanOrEqual(Date.now() + 5_000)
    })
  })

  it("cancelSchedule delegates to the scheduler", async () => {
    const sm = mockSchedulingLog()
    let result: unknown
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventStore: sm as never,
      })
      result = await cancelSchedule({ id: "tok-1" })
    })
    expect(result).toEqual({ kind: "cancelled" })
    expect(sm.cancelled).toEqual([{ id: "tok-1" }])
  })

  it("throws when the entry's log cannot hold a future event — the one defensive assert", async () => {
    // A caller with a compiler cannot get here: the verbs are structurally
    // absent from a context whose `E` is bare. This is the JavaScript path, and
    // the message names the CAPABILITY and the wrapper PATTERN — never a
    // specific family, because core does not know which one this host chose.
    await unitOfWork().execute(async (uow) => {
      const { schedule } = scheduleFunctions({ uow })
      await expect(schedule(descriptor, { id: "X" }, new Date())).rejects.toThrow(
        /<family>SchedulingEventStore/,
      )
    })
  })

  it("throws outside the INVOCATION phase", async () => {
    const { schedule } = scheduleFunctions({ uow: unitOfWork(), eventStore: mockSchedulingLog() as never })
    await expect(schedule(descriptor, { id: "X" }, new Date())).rejects.toThrow()
  })

  it("hands the unit of work to the scheduler so the insert joins its transaction", async () => {
    const sm = mockSchedulingLog()
    let captured: unknown
    await unitOfWork().execute(async (uow) => {
      captured = uow
      const { schedule } = scheduleFunctions({ uow, eventStore: sm as never })
      await schedule(descriptor, { id: "A" }, new Date(Date.now() + 60_000))
    })
    expect(sm.schedule.mock.calls[0]![2]).toBe(captured)
  })

  it("rejects an Invalid Date `at`", async () => {
    const sm = mockSchedulingLog()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventStore: sm as never,
      })
      await expect(schedule(descriptor, { id: "X" }, new Date(NaN))).rejects.toThrow(/valid Date/)
    })
    expect(sm.scheduled).toHaveLength(0)
  })

  it("allows a past `at` (fires ASAP)", async () => {
    const sm = mockSchedulingLog()
    const past = new Date(Date.now() - 60_000)
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventStore: sm as never,
      })
      await schedule(descriptor, { id: "P" }, past)
    })
    expect(sm.scheduled).toHaveLength(1)
    expect(sm.scheduled[0]!.at).toBe(past)
  })

  it("scheduleAfter rejects a non-finite delay", async () => {
    const sm = mockSchedulingLog()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventStore: sm as never,
      })
      await expect(scheduleAfter(descriptor, { id: "X" }, NaN)).rejects.toThrow(/finite number/)
      await expect(scheduleAfter(descriptor, { id: "X" }, Infinity)).rejects.toThrow(/finite number/)
    })
    expect(sm.scheduled).toHaveLength(0)
  })

  it("scheduleAfter allows a negative delay (fires ASAP)", async () => {
    const sm = mockSchedulingLog()
    await unitOfWork().execute(async (uow) => {
      const { schedule, scheduleAfter, cancelSchedule } = scheduleFunctions({
        uow,
        eventStore: sm as never,
      })
      const before = Date.now()
      await scheduleAfter(descriptor, { id: "N" }, -5_000)
      expect(sm.scheduled[0]!.at.getTime()).toBeLessThanOrEqual(before)
    })
  })
})
