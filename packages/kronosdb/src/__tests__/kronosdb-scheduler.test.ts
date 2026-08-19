import { describe, expect, it } from "bun:test"
import { Status } from "nice-grpc"
import { qn, tag } from "@kronos-ts/core"
import type { EventMessage } from "@kronos-ts/core"
import {
  createKronosDbScheduler,
  ScheduleAlreadyExistsError,
  ScheduleAlreadyResolvedError,
  ScheduleNotFoundError,
} from "../kronosdb-scheduler.js"

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

/** Builds a scheduler over a stubbed gRPC client; returns captured requests. */
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
  // The stub covers exactly the surface the scheduler touches.
  const scheduler = createKronosDbScheduler(connection as never, serializer)
  return { scheduler, calls }
}

function grpcError(code: Status): Error & { code: Status } {
  return Object.assign(new Error(`grpc ${code}`), { code })
}

describe("createKronosDbScheduler", () => {
  it("maps the event, tags, and due time onto the wire request", async () => {
    const { scheduler, calls } = stubbedScheduler()

    const token = await scheduler.schedule(sampleEvent(), new Date(1_800_000_000_000), { token: "my-token" })

    expect(token).toBe("server-token")
    const request = calls.scheduleAppend![0] as {
      dueMs: bigint
      token: string
      event: { event: { name: string; payload: Uint8Array }; tags: { key: Uint8Array; value: Uint8Array }[] }
    }
    expect(request.dueMs).toBe(1_800_000_000_000n)
    expect(request.token).toBe("my-token")
    expect(request.event.event.name).toBe("billing.PaymentTimedOut")
    expect(JSON.parse(textDecoder.decode(request.event.event.payload))).toEqual({ orderId: "A-1" })
    expect(textDecoder.decode(request.event.tags[0]!.key)).toBe("orderId")
    expect(textDecoder.decode(request.event.tags[0]!.value)).toBe("A-1")
  })

  it("sends an empty token when none is supplied, letting the server generate one", async () => {
    const { scheduler, calls } = stubbedScheduler()
    await scheduler.schedule(sampleEvent(), 1_800_000_000_000)
    expect((calls.scheduleAppend![0] as { token: string }).token).toBe("")
  })

  it("turns ALREADY_EXISTS into ScheduleAlreadyExistsError — the idempotent-retry signal", async () => {
    const { scheduler } = stubbedScheduler({ scheduleAppend: grpcError(Status.ALREADY_EXISTS) })
    expect(scheduler.schedule(sampleEvent(), 0, { token: "dup" }))
      .rejects.toThrow(ScheduleAlreadyExistsError)
  })

  it("turns FAILED_PRECONDITION on cancel into ScheduleAlreadyResolvedError", async () => {
    const { scheduler } = stubbedScheduler({ cancelSchedule: grpcError(Status.FAILED_PRECONDITION) })
    expect(scheduler.cancel("tok")).rejects.toThrow(ScheduleAlreadyResolvedError)
  })

  it("turns NOT_FOUND on cancel into ScheduleNotFoundError", async () => {
    const { scheduler } = stubbedScheduler({ cancelSchedule: grpcError(Status.NOT_FOUND) })
    expect(scheduler.cancel("tok")).rejects.toThrow(ScheduleNotFoundError)
  })

  it("lets UNAVAILABLE (not the leader) propagate for the caller's failover to handle", async () => {
    const { scheduler } = stubbedScheduler({ scheduleAppend: grpcError(Status.UNAVAILABLE) })
    expect(scheduler.schedule(sampleEvent(), 0)).rejects.toThrow("grpc 14")
  })

  it("returns pending schedules with numeric due times", async () => {
    const { scheduler } = stubbedScheduler()
    const pending = await scheduler.list()
    expect(pending).toEqual([
      { token: "b", dueAt: 2_000, eventName: "Second" },
      { token: "a", dueAt: 1_000, eventName: "First" },
    ])
  })
})
