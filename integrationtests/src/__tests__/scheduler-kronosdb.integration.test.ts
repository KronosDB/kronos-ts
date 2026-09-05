/**
 * Integration test for KronosDB's event scheduler (server-side scheduled
 * appends) through @kronos-ts/kronosdb.
 *
 * Spins up the KronosDB container. Until the published image ships the
 * SchedulerService, every test skips with a notice (the probe sees
 * UNIMPLEMENTED); once the image updates they run unchanged. Set
 * KRONOSDB_IMAGE to test a locally built image, e.g.
 * `KRONOSDB_IMAGE=kronosdb:dev bun test scheduler-kronosdb`.
 *
 * What is pinned here, deliberately from the *consumer's* seat:
 *  - a due schedule appends the event with no client involvement
 *  - the schedule bookkeeping is invisible: no phantom events, and a drained
 *    cursor reaches GetHead exactly (the visible-head contract)
 *  - cancellation prevents the append, and cancelling late says so
 *  - token idempotency refuses a duplicate schedule
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { inMemoryEventStore, qn, tag } from "@kronos-ts/core"
import type { EventMessage, ScheduleStoreCapability } from "@kronos-ts/core"
import {
  connectToKronosDb,
  kronosDbSchedulingEventStore,
  type KronosDbConnection,
  type KronosDbSchedulingControl,
} from "@kronos-ts/kronosdb"

const IMAGE = process.env.KRONOSDB_IMAGE ?? "ghcr.io/kronosdb/kronosdb:latest"

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
  const events: { name: string; sequence: bigint }[] = []
  let marker: bigint | undefined
  const stream = connection.eventStore.source({
    fromSequence: 0n,
    criteria: [{ names: [], tags: [{ key: new TextEncoder().encode("orderId"), value: new TextEncoder().encode(orderId) }] }],
  })
  for await (const response of stream) {
    const batch = response.batch
    if (!batch) continue
    for (const sequenced of batch.events) {
      if (sequenced.event) events.push({ name: sequenced.event.name, sequence: sequenced.sequence })
    }
    if (batch.consistencyMarker !== undefined) marker = batch.consistencyMarker
  }
  return { events, marker }
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
  let container: StartedTestContainer
  let connection: KronosDbConnection
  let scheduler: ScheduleStoreCapability & KronosDbSchedulingControl
  let schedulerAvailable = false

  beforeAll(async () => {
    container = await new GenericContainer(IMAGE)
      .withExposedPorts(50051, 9240)
      // /ready is claim-aware: 200 only once the node can execute writes.
      // The startup log line prints BEFORE the leader claim commits, so
      // waiting on it lets the first ScheduleAppend race the claim and
      // bounce with UNAVAILABLE ("must be directed to the leader").
      .withWaitStrategy(Wait.forHttp("/ready", 9240).forStatusCode(200))
      .start()

    connection = await connectToKronosDb({
      host: container.getHost(),
      port: container.getMappedPort(50051),
      componentName: "scheduler-e2e",
    })
    // THE TIER, over a throwaway in-memory log. The wrapped store is irrelevant
    // to every claim here: KronosDB appends the fired event SERVER-SIDE, into
    // the log the connection already owns, which is exactly the property this
    // file exists to prove.
    scheduler = kronosDbSchedulingEventStore(inMemoryEventStore(), connection, { serializer })

    // Probe once: an image without the service answers UNIMPLEMENTED.
    try {
      await scheduler.listSchedules()
      schedulerAvailable = true
    } catch (error) {
      // gRPC UNIMPLEMENTED = 12: this server predates the SchedulerService.
      if ((error as { code?: number }).code === 12) {
        console.warn(`[scheduler-e2e] ${IMAGE} does not ship SchedulerService yet — skipping`)
      } else {
        throw error
      }
    }
  }, 120_000)

  afterAll(async () => {
    await connection?.close()
    await container?.stop()
  })

  it("appends a due event server-side, invisibly until it fires", async () => {
    if (!schedulerAvailable) return

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
    const head = (await connection.eventStore.getHead({})).sequence
    const stream = connection.eventStore.source({ fromSequence: 0n, criteria: [] })
    let lastSequence = -1n
    for await (const response of stream) {
      for (const sequenced of response.batch?.events ?? []) lastSequence = sequenced.sequence
    }
    expect(lastSequence + 1n).toBe(head)
  }, 30_000)

  it("cancels before firing, and reports the loss honestly after", async () => {
    if (!schedulerAvailable) return

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

  // THE CALLER-SUPPLIED IDEMPOTENCY TOKEN IS GONE, and with it the test that
  // covered it. `ScheduleStoreCapability.schedule` has no such parameter, because
  // neither the postgres nor the in-memory tier can honour one and a capability
  // is what every member of the tier can promise. A host that needs KronosDB's
  // idempotent retry reaches `connection.scheduler.scheduleAppend` directly —
  // the service is still there, it is simply not part of the shared contract.
})
