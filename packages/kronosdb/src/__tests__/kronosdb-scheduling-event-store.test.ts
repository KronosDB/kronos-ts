import { afterEach, describe, expect, it } from "bun:test"
import { inMemoryEventStore, qn, tag } from "@kronos-ts/core"
import type { EventMessage } from "@kronos-ts/core"
import { connectToKronosDb, type KronosDbConnection } from "../connection.js"
import {
  kronosDbSchedulingEventStore,
  ScheduleAlreadyExistsError,
} from "../kronosdb-scheduling-event-store.js"
import { ErrorCode, FLAG_END, type Frame } from "../protocol/kronosdb-protocol.js"
import { fakeNativeServer, type FakeNativeServer, type ReceivedFrame } from "./fake-native-server.js"

const textDecoder = new TextDecoder()

const serializer = {
  serialize(value: unknown, type: string, revision = "") {
    return { data: new TextEncoder().encode(JSON.stringify(value)), type, revision }
  },
  deserialize(obj: { data: Uint8Array }) {
    return JSON.parse(textDecoder.decode(obj.data))
  },
  canConvert: () => true,
}

function sampleEvent(): EventMessage {
  return {
    kind: "event",
    identifier: "evt-1",
    name: qn("billing", "PaymentTimedOut"),
    version: "1.0",
    payload: { orderId: "A-1" },
    metadata: { source: "test" },
    timestamp: 1_700_000_000_000,
    tags: [tag("orderId", "A-1")],
  }
}

let server: FakeNativeServer | undefined
let connection: KronosDbConnection | undefined

afterEach(async () => {
  connection?.close()
  connection = undefined
  await server?.close()
  server = undefined
})

/**
 * Wraps a log in the tier over a fake native server that answers each request
 * with `answer(frame)`; returns what the server was sent.
 */
async function scheduledAgainst(answer: (received: ReceivedFrame) => Frame) {
  server = await fakeNativeServer({
    onFrame(conn, received) {
      conn.reply(received.stream, FLAG_END, answer(received))
    },
  })
  connection = connectToKronosDb({ host: "127.0.0.1", port: server.port, componentName: "scheduling-test" })
  const scheduler = kronosDbSchedulingEventStore(inMemoryEventStore(), { connection, serializer })
  const sent = () => server!.connections[0]!.received
  return { scheduler, sent }
}

const failed = (code: number, message = "m"): Frame => ({ name: "RequestFailed", body: { code, message } })

describe("kronosDbSchedulingEventStore", () => {
  it("maps the event, tags, and due time onto the wire request", async () => {
    const { scheduler, sent } = await scheduledAgainst(() => ({ name: "Scheduled", body: { token: "server-token" } }))

    const token = await scheduler.schedule(sampleEvent(), new Date(1_800_000_000_000))

    expect(token).toEqual({ id: "server-token" })
    const request = sent()[0]!.frame
    expect(request.name).toBe("ScheduleAppend")
    if (request.name !== "ScheduleAppend") return
    expect(request.body.due).toBe(1_800_000_000_000)
    // THE SERVER MINTS THE TOKEN. `ScheduleStoreCapability.schedule` has no
    // caller-supplied idempotency key, because two of the three families cannot
    // honour one and a capability is what all of them can promise.
    expect(request.body.token).toBe("")
    expect(request.body.context).toBe(0)
    const record = request.body.event.event
    expect(record.name).toBe("billing.PaymentTimedOut")
    expect(record.identifier).toBe("evt-1")
    expect(record.version).toBe("1.0")
    expect(record.position).toBe(0n)
    expect(record.metadata).toEqual([{ key: "source", value: "test" }])
    expect(JSON.parse(textDecoder.decode(record.payload))).toEqual({ orderId: "A-1" })
    expect(textDecoder.decode(record.tags[0]!.key)).toBe("orderId")
    expect(textDecoder.decode(record.tags[0]!.value)).toBe("A-1")
  })

  it("turns already_exists into ScheduleAlreadyExistsError", async () => {
    const { scheduler } = await scheduledAgainst(() => failed(ErrorCode.already_exists))
    const error = await scheduler.schedule(sampleEvent(), new Date(0)).then(
      () => undefined,
      (e) => e,
    )
    expect(error).toBeInstanceOf(ScheduleAlreadyExistsError)
  })

  // THE THREE OUTCOMES ARE NEWS, NOT THROWS. A protocol status is the wire's
  // vocabulary; `CancelResult` is the capability's, and every family answers in
  // it so a caller compensating for a fired deadline writes one branch.
  it("reads `rejected` on cancel as already-appended — the compensating branch", async () => {
    const { scheduler } = await scheduledAgainst(() => failed(ErrorCode.rejected, "this schedule has already fired or been cancelled"))
    expect(await scheduler.cancelSchedule({ id: "tok" })).toEqual({ kind: "already-appended" })
  })

  it("reads not_found on cancel as not-found", async () => {
    const { scheduler } = await scheduledAgainst(() => failed(ErrorCode.not_found))
    expect(await scheduler.cancelSchedule({ id: "tok" })).toEqual({ kind: "not-found" })
  })

  it("answers cancelled when the server accepts the cancel", async () => {
    const { scheduler, sent } = await scheduledAgainst(() => ({ name: "ScheduleCancelled", body: {} }))
    expect(await scheduler.cancelSchedule({ id: "tok" })).toEqual({ kind: "cancelled" })
    expect(sent()[0]!.frame).toEqual({ name: "CancelSchedule", body: { context: 0, token: "tok" } })
  })

  it("lets `unavailable` (not the leader) propagate, transient, for the caller's failover to handle", async () => {
    const { scheduler } = await scheduledAgainst(() => failed(ErrorCode.unavailable, "scheduling must be directed to the leader node"))
    const error = await scheduler.schedule(sampleEvent(), new Date(0)).then(
      () => undefined,
      (e) => e,
    )
    expect(error.message).toBe("scheduling must be directed to the leader node")
    expect(error.transient).toBe(true)
  })

  it("is ADDITIVE — the wrapped log is still a log", async () => {
    const { scheduler } = await scheduledAgainst(() => ({ name: "Scheduled", body: { token: "t" } }))
    // The append path is the inner store's, untouched: a scheduling tier adds a
    // write that happens LATER, it does not change what the log does now.
    await scheduler.append([sampleEvent()])
    expect(await scheduler.getHeadPosition()).toBe(1n)
  })

  it("returns pending schedules with numeric due times", async () => {
    const { scheduler } = await scheduledAgainst(() => ({
      name: "Schedules",
      body: {
        schedules: [
          { token: "b", due: 2_000, eventName: "Second" },
          { token: "a", due: 1_000, eventName: "First" },
        ],
      },
    }))
    expect(await scheduler.listSchedules()).toEqual([
      { token: "b", dueAt: 2_000, eventName: "Second" },
      { token: "a", dueAt: 1_000, eventName: "First" },
    ])
  })
})
