import { describe, expect, it } from "bun:test"
import { Status } from "nice-grpc"
import { inMemoryEventStore, qn, tag } from "@kronos-ts/core"
import type { EventMessage } from "@kronos-ts/core"
import {
  kronosDbSchedulingEventStore,
  ScheduleAlreadyExistsError,
} from "../kronosdb-scheduling-event-store.js"

const textDecoder = new TextDecoder()

const serializer = {
  serialize(value: unknown, type: string, revision?: string) {
    return { data: new TextEncoder().encode(JSON.stringify(value)), type, revision }
  },
  deserialize(obj: { data: Uint8Array }) {
    return JSON.parse(textDecoder.decode(obj.data))
  },
}

function sampleEvent(): EventMessage {
  return {
    kind: "event",
    identifier: "evt-1",
    name: qn("billing", "PaymentTimedOut"),
    version: "1.0",
    payload: { orderId: "A-1" },
    metadata: new Map(),
    timestamp: 1_700_000_000_000,
    tags: [tag("orderId", "A-1")],
  }
}

/** Wraps a log in the tier over a stubbed gRPC client; returns captured requests. */
function stubbedScheduler(overrides: Partial<Record<string, unknown>> = {}) {
  const calls: Record<string, unknown[]> = { scheduleAppend: [], cancelSchedule: [], listSchedules: [] }
  const connection = {
    config: { context: "test-ctx", token: "" },
    scheduler: {
      async scheduleAppend(request: unknown) {
        calls.scheduleAppend!.push(request)
        if (overrides.scheduleAppend) throw overrides.scheduleAppend
        return { token: "server-token" }
      },
      async cancelSchedule(request: unknown) {
        calls.cancelSchedule!.push(request)
        if (overrides.cancelSchedule) throw overrides.cancelSchedule
        return {}
      },
      async listSchedules() {
        if (overrides.listSchedules) throw overrides.listSchedules
        return {
          schedules: [
            { token: "b", dueMs: 2_000n, eventName: "Second" },
            { token: "a", dueMs: 1_000n, eventName: "First" },
          ],
        }
      },
    },
  }
  // The stub covers exactly the surface the tier touches.
  const scheduler = kronosDbSchedulingEventStore(inMemoryEventStore(), connection as never, {
    serializer,
  })
  return { scheduler, calls }
}

function grpcError(code: Status): Error & { code: Status } {
  return Object.assign(new Error(`grpc ${code}`), { code })
}

describe("kronosDbSchedulingEventStore", () => {
  it("maps the event, tags, and due time onto the wire request", async () => {
    const { scheduler, calls } = stubbedScheduler()

    const token = await scheduler.schedule(sampleEvent(), new Date(1_800_000_000_000))

    expect(token).toEqual({ id: "server-token" })
    const request = calls.scheduleAppend![0] as {
      dueMs: bigint
      token: string
      event: { event: { name: string; payload: Uint8Array }; tags: { key: Uint8Array; value: Uint8Array }[] }
    }
    expect(request.dueMs).toBe(1_800_000_000_000n)
    // THE SERVER MINTS THE TOKEN. `ScheduleStoreCapability.schedule` has no
    // caller-supplied idempotency key, because two of the three families cannot
    // honour one and a capability is what all of them can promise.
    expect(request.token).toBe("")
    expect(request.event.event.name).toBe("billing.PaymentTimedOut")
    expect(JSON.parse(textDecoder.decode(request.event.event.payload))).toEqual({ orderId: "A-1" })
    expect(textDecoder.decode(request.event.tags[0]!.key)).toBe("orderId")
    expect(textDecoder.decode(request.event.tags[0]!.value)).toBe("A-1")
  })

  it("turns ALREADY_EXISTS into ScheduleAlreadyExistsError", async () => {
    const { scheduler } = stubbedScheduler({ scheduleAppend: grpcError(Status.ALREADY_EXISTS) })
    expect(scheduler.schedule(sampleEvent(), new Date(0)))
      .rejects.toThrow(ScheduleAlreadyExistsError)
  })

  // THE THREE OUTCOMES ARE NEWS, NOT THROWS. A gRPC status is the wire's
  // vocabulary; `CancelResult` is the capability's, and every family answers in
  // it so a caller compensating for a fired deadline writes one branch.
  it("reads FAILED_PRECONDITION on cancel as already-appended — the compensating branch", async () => {
    const { scheduler } = stubbedScheduler({ cancelSchedule: grpcError(Status.FAILED_PRECONDITION) })
    expect(await scheduler.cancelSchedule({ id: "tok" })).toEqual({ kind: "already-appended" })
  })

  it("reads NOT_FOUND on cancel as not-found", async () => {
    const { scheduler } = stubbedScheduler({ cancelSchedule: grpcError(Status.NOT_FOUND) })
    expect(await scheduler.cancelSchedule({ id: "tok" })).toEqual({ kind: "not-found" })
  })

  it("answers cancelled when the server accepts the cancel", async () => {
    const { scheduler, calls } = stubbedScheduler()
    expect(await scheduler.cancelSchedule({ id: "tok" })).toEqual({ kind: "cancelled" })
    expect(calls.cancelSchedule![0]).toEqual({ token: "tok" })
  })

  it("lets UNAVAILABLE (not the leader) propagate for the caller's failover to handle", async () => {
    const { scheduler } = stubbedScheduler({ scheduleAppend: grpcError(Status.UNAVAILABLE) })
    expect(scheduler.schedule(sampleEvent(), new Date(0))).rejects.toThrow("grpc 14")
  })

  it("is ADDITIVE — the wrapped log is still a log", async () => {
    const { scheduler } = stubbedScheduler()
    // The append path is the inner store's, untouched: a scheduling tier adds a
    // write that happens LATER, it does not change what the log does now.
    await scheduler.append([sampleEvent()])
    expect(await scheduler.getHeadPosition()).toBe(1n)
  })

  it("returns pending schedules with numeric due times", async () => {
    const { scheduler } = stubbedScheduler()
    const pending = await scheduler.listSchedules()
    expect(pending).toEqual([
      { token: "b", dueAt: 2_000, eventName: "Second" },
      { token: "a", dueAt: 1_000, eventName: "First" },
    ])
  })
})
