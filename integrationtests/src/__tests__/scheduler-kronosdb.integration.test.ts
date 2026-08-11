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
import { qn, tag } from "@kronos-ts/common"
import type { EventMessage } from "@kronos-ts/messaging"
import {
  connectToKronosDb,
  createKronosDbScheduler,
  ScheduleAlreadyExistsError,
  ScheduleAlreadyResolvedError,
  type KronosDbConnection,
  type KronosDbScheduler,
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
  let scheduler: KronosDbScheduler
  let schedulerAvailable = false

  beforeAll(async () => {
    container = await new GenericContainer(IMAGE)
      .withExposedPorts(50051, 9240)
      .withWaitStrategy(Wait.forLogMessage(/KronosDB starting/))
      .start()

    connection = await connectToKronosDb({
      host: container.getHost(),
      port: container.getMappedPort(50051),
      componentName: "scheduler-e2e",
    })
    scheduler = createKronosDbScheduler(connection, serializer)

    // Probe once: an image without the service answers UNIMPLEMENTED.
    try {
      await scheduler.list()
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
    await scheduler.schedule(reminderEvent(orderId), Date.now() + 1_500)

    // Before due: the schedule exists, but the client sees no event.
    expect((await sourceByOrder(connection, orderId)).events).toHaveLength(0)
    expect((await scheduler.list()).length).toBeGreaterThan(0)

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
    const token = await scheduler.schedule(reminderEvent(orderId), Date.now() + 1_000, {
      token: `cancel-${orderId}`,
    })
    await scheduler.cancel(token)

    // Past the due time: nothing may appear.
    await new Promise((resolve) => setTimeout(resolve, 2_500))
    expect((await sourceByOrder(connection, orderId)).events).toHaveLength(0)

    // Cancelling again reports the schedule already resolved.
    expect(scheduler.cancel(token)).rejects.toThrow(ScheduleAlreadyResolvedError)
  }, 30_000)

  it("refuses a duplicate token — retried schedules are idempotent", async () => {
    if (!schedulerAvailable) return

    const orderId = `C-${crypto.randomUUID()}`
    const token = `idem-${orderId}`
    await scheduler.schedule(reminderEvent(orderId), Date.now() + 60_000, { token })
    expect(scheduler.schedule(reminderEvent(orderId), Date.now() + 60_000, { token }))
      .rejects.toThrow(ScheduleAlreadyExistsError)
    await scheduler.cancel(token)
  }, 30_000)
})
