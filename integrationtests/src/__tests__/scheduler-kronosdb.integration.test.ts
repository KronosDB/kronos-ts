/**
 * Integration test for KronosDB's event scheduler (server-side scheduled
 * appends) through @kronos-ts/kronosdb, over the native protocol.
 *
 * Needs a server that speaks it: a local build via KRONOSDB_SERVER_BIN, or an
 * image of one via KRONOSDB_IMAGE (see ../kronosdb-server.ts).
 *
 * What is pinned here, deliberately from the *consumer's* seat:
 *  - a due schedule appends the event with no client involvement
 *  - the schedule bookkeeping is invisible: no phantom events, and a drained
 *    cursor reaches GetHead exactly (the visible-head contract)
 *  - cancellation prevents the append, and cancelling late says so
 *  - token idempotency refuses a duplicate schedule
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { inMemoryEventStore, qn, qualifiedNameToString, tag } from "@kronos-ts/core"
import type { EventMessage, ScheduleStoreCapability } from "@kronos-ts/core"
import {
  connectToKronosDb,
  kronosDbEventStore,
  kronosDbSchedulingEventStore,
  ScheduleAlreadyExistsError,
  type KronosDbConnection,
  type KronosDbSchedulingControl,
} from "@kronos-ts/kronosdb"
import { startKronosDb, type KronosDbServer } from "../kronosdb-server.js"

const serializer = {
  serialize(value: unknown, type: string, revision?: string) {
    return { data: new TextEncoder().encode(JSON.stringify(value)), type, revision }
  },
  deserialize(obj: { data: Uint8Array }) {
    return JSON.parse(new TextDecoder().decode(obj.data))
  },
}

function reminderEvent(orderId: string): EventMessage {
  return {
    kind: "event",
    identifier: crypto.randomUUID(),
    name: qn("scheduler-e2e", "ReminderDue"),
    version: "1.0",
    payload: { orderId },
    metadata: new Map(),
    timestamp: Date.now(),
    tags: [tag("orderId", orderId)],
  }
}

async function sourceByOrder(connection: KronosDbConnection, orderId: string) {
  const { events } = await kronosDbEventStore({ connection, serializer }).source({ query: { tags: { orderId } } })
  return { events: events.map((e) => ({ name: qualifiedNameToString(e.name) })) }
}

const waitUntil = async (probe: () => Promise<boolean>, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await probe()) return true
    await new Promise((resolve) => setTimeout(resolve, 250))
  }
  return false
}

describe("KronosDB scheduler (e2e)", () => {
  let server: KronosDbServer
  let connection: KronosDbConnection
  let scheduler: ScheduleStoreCapability & KronosDbSchedulingControl

  beforeAll(async () => {
    server = await startKronosDb()

    connection = await connectToKronosDb({
      host: server.host,
      port: server.port,
      componentName: "scheduler-e2e",
    })
    // THE TIER, over a throwaway in-memory log. The wrapped store is irrelevant
    // to every claim here: KronosDB appends the fired event SERVER-SIDE, into
    // the log the connection already owns, which is exactly the property this
    // file exists to prove.
    scheduler = kronosDbSchedulingEventStore(inMemoryEventStore(), connection, { serializer })
  }, 120_000)

  afterAll(async () => {
    await connection?.close()
    await server?.stop()
  })

  it("appends a due event server-side, invisibly until it fires", async () => {
    const orderId = `A-${crypto.randomUUID()}`
    await scheduler.schedule(reminderEvent(orderId), new Date(Date.now() + 1_500))

    // Before due: the schedule exists, but the client sees no event.
    expect((await sourceByOrder(connection, orderId)).events).toHaveLength(0)
    expect((await scheduler.listSchedules()).length).toBeGreaterThan(0)

    // After due: the event landed without any client appending it.
    const fired = await waitUntil(async () =>
      (await sourceByOrder(connection, orderId)).events.length === 1)
    expect(fired).toBe(true)

    const { events } = await sourceByOrder(connection, orderId)
    expect(events[0]!.name).toBe("scheduler-e2e.ReminderDue")

    // The visible-head contract: a drained cursor reaches GetHead exactly,
    // despite the schedule's hidden bookkeeping occupying positions.
    const store = kronosDbEventStore({ connection, serializer })
    const head = await store.getHeadPosition()
    const stream = store.open({ position: 0n })
    let lastSequence = -1n
    await waitUntil(async () => {
      for (let item = stream.next(); item !== undefined; item = stream.next()) lastSequence = item.sequence
      return lastSequence + 1n === head
    }, 5_000)
    stream.close()
    expect(lastSequence + 1n).toBe(head)
  }, 30_000)

  it("cancels before firing, and reports the loss honestly after", async () => {
    const orderId = `B-${crypto.randomUUID()}`
    const token = await scheduler.schedule(reminderEvent(orderId), new Date(Date.now() + 1_000))
    expect(await scheduler.cancelSchedule(token)).toEqual({ kind: "cancelled" })

    // Past the due time: nothing may appear.
    await new Promise((resolve) => setTimeout(resolve, 2_500))
    expect((await sourceByOrder(connection, orderId)).events).toHaveLength(0)

    // Cancelling again is NEWS, not a throw: the schedule has resolved, and the
    // server does not say whether it fired or was cancelled — so the capability
    // reports the reading that makes a caller check for compensation.
    expect(await scheduler.cancelSchedule(token)).toEqual({ kind: "already-appended" })
  }, 30_000)

  // `ScheduleStoreCapability.schedule` has no idempotency token, because neither
  // the postgres nor the in-memory tier can honour one and a capability is what
  // every member of the tier can promise. KronosDB's tier adds it beside the
  // capability, as `scheduleIdempotent`.
  it("refuses a second schedule under the same token", async () => {
    const orderId = `C-${crypto.randomUUID()}`
    const token = `retry-${orderId}`
    const due = new Date(Date.now() + 60_000)

    expect(await scheduler.scheduleIdempotent(reminderEvent(orderId), due, token)).toEqual({ id: token })
    const refusal = await scheduler.scheduleIdempotent(reminderEvent(orderId), due, token).then(
      () => undefined,
      (error) => error,
    )
    expect(refusal).toBeInstanceOf(ScheduleAlreadyExistsError)
    expect((refusal as ScheduleAlreadyExistsError).token).toBe(token)

    expect(await scheduler.cancelSchedule({ id: token })).toEqual({ kind: "cancelled" })
  })
})
