import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, type EventMessage } from "../../messaging/messages.js"
import { batched, concurrent, deadLettering, eventProcessor } from "../processor.js"
import { runEventProcessor } from "../running-processor.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import { inMemoryTokenStore } from "../token-store.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import type { StreamableEventSource, SequencedEvent } from "../source.js"
import type { EventHandler } from "../handler.js"
import { inMemoryDeadLetterQueue } from "../dead-letter-queue.js"

// ---------------------------------------------------------------------------
// Test helpers (mirrors dead-letter-wiring.test.ts)
// ---------------------------------------------------------------------------

const EVENT_NAME = qn("test", "SomethingHappened")

type Payload = { v: string; bucket: string; fail?: boolean; slow?: boolean }

function makeEvent(payload: Payload, sequence: bigint): SequencedEvent {
  return {
    sequence,
    event: {
      identifier: `evt-${sequence}`,
      name: EVENT_NAME,
      version: "1.0",
      payload,
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [{ key: "bucket", value: payload.bucket }],
    },
  }
}

function createInMemoryEventSource(events: SequencedEvent[]): StreamableEventSource {
  return {
    open(condition) {
      let cursor = condition.position
      return {
        next() {
          const found = events.find((e) => e.sequence >= cursor)
          if (!found) return undefined
          cursor = found.sequence + 1n
          return found
        },
        peek() { return events.find((e) => e.sequence >= cursor) },
        hasNextAvailable() { return events.some((e) => e.sequence >= cursor) },
        isCompleted() { return false },
        error() { return undefined },
        setCallback() {},
        close() {},
      }
    },
    async getHeadPosition() {
      if (events.length === 0) return 0n
      return events[events.length - 1]!.sequence + 1n
    },
  }
}

async function waitForPosition(proc: { position: bigint }, target: bigint, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (proc.position < target && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 5))
  }
  if (proc.position < target) throw new Error(`did not reach position ${target} (at ${proc.position})`)
}

const byBucket = (e: EventMessage) => (e.payload as Payload).bucket
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function recordingHandler(log: string[], failures = new Set<string>()): EventHandler<any> {
  return {
    kind: "event-handler",
    descriptor: { kind: "event", name: EVENT_NAME, version: "1.0", payload: {} as any },
    handler: async ({ payload }: { payload: Payload }) => {
      log.push(`start ${payload.v}`)
      if (payload.slow) await sleep(20)
      else await sleep(1)
      if (payload.fail && !failures.has(payload.v)) {
        failures.add(payload.v)
        throw new Error(`${payload.v} failed`)
      }
      log.push(`end ${payload.v}`)
    },
  }
}

const base = (name: string, events: SequencedEvent[], uow: () => ReturnType<typeof unitOfWork> = unitOfWork) =>
  eventProcessor(
    name,
    createInMemoryEventSource(events) as unknown as EventStore,
    inMemoryTokenStore(),
    uow,
  )

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("concurrent — buckets side by side, each bucket in log order", () => {
  it("runs different buckets at the same time and one bucket's events one after another", async () => {
    const events = [
      makeEvent({ v: "a1", bucket: "A", slow: true }, 0n),
      makeEvent({ v: "b1", bucket: "B" }, 1n),
      makeEvent({ v: "a2", bucket: "A" }, 2n),
    ]
    const log: string[] = []
    const processor = runEventProcessor({
      processor: concurrent(batched(base("side-by-side", events), 10), byBucket),
      handlers: [{ definition: recordingHandler(log) }],
      pollingIntervalMs: 10,
    })

    await processor.start()
    await waitForPosition(processor, 3n)
    processor.stop()

    // b1 is not held up by a1, which is slow; a2 waits for a1.
    expect(log.indexOf("end b1")).toBeLessThan(log.indexOf("end a1"))
    expect(log.indexOf("start a2")).toBeGreaterThan(log.indexOf("end a1"))
  })

  it("without it, delivers one event at a time in log order, whatever the buckets", async () => {
    const events = [
      makeEvent({ v: "a1", bucket: "A", slow: true }, 0n),
      makeEvent({ v: "b1", bucket: "B" }, 1n),
    ]
    const log: string[] = []
    const processor = runEventProcessor({
      processor: batched(base("one-at-a-time", events), 10),
      handlers: [{ definition: recordingHandler(log) }],
      pollingIntervalMs: 10,
    })

    await processor.start()
    await waitForPosition(processor, 2n)
    processor.stop()

    expect(log).toEqual(["start a1", "end a1", "start b1", "end b1"])
  })

  it("fails the batch only once every bucket has settled, and redelivers all of it", async () => {
    const events = [
      makeEvent({ v: "a1", bucket: "A", fail: true }, 0n),
      makeEvent({ v: "b1", bucket: "B", slow: true }, 1n),
    ]
    const log: string[] = []
    const uow = () => {
      const task = unitOfWork()
      task.onError(() => {
        log.push("rolled back")
      })
      return task
    }
    const processor = runEventProcessor({
      processor: concurrent(batched(base("settle-then-fail", events, uow), 10), byBucket),
      handlers: [{ definition: recordingHandler(log) }],
      pollingIntervalMs: 5,
    })

    await processor.start()
    await waitForPosition(processor, 2n)
    processor.stop()

    // b1 finished before the batch rolled back, and was delivered again after.
    expect(log.indexOf("end b1")).toBeLessThan(log.indexOf("rolled back"))
    expect(log.filter((line) => line === "end b1")).toHaveLength(2)
    expect(processor.position).toBe(2n)
  })
})

describe("concurrent with deadLettering — the failing bucket parks, the others go on", () => {
  it("parks the failed event and the rest of its bucket, commits the other buckets, and moves the token past all of it", async () => {
    const events = [
      makeEvent({ v: "a1", bucket: "A", fail: true }, 0n),
      makeEvent({ v: "b1", bucket: "B" }, 1n),
      makeEvent({ v: "a2", bucket: "A" }, 2n),
      makeEvent({ v: "b2", bucket: "B" }, 3n),
    ]
    const log: string[] = []
    const dlq = inMemoryDeadLetterQueue()
    const processor = runEventProcessor({
      processor: deadLettering(concurrent(batched(base("park-one-bucket", events), 10), byBucket), dlq, byBucket),
      handlers: [{ definition: recordingHandler(log, new Set(["never"])) }],
      pollingIntervalMs: 10,
    })

    await processor.start()
    await waitForPosition(processor, 4n)
    processor.stop()

    expect(log.filter((line) => line.startsWith("end"))).toEqual(["end b1", "end b2"])
    const parked = await dlq.deadLetterSequence("park-one-bucket", "A")
    expect(parked.map((letter) => (letter.message.payload as Payload).v)).toEqual(["a1", "a2"])
    expect(await dlq.contains("park-one-bucket", "B")).toBe(false)
  })

  it("refuses two different bucket functions on one processor", () => {
    const other = (e: EventMessage) => e.identifier
    expect(() => deadLettering(concurrent(base("two-functions", []), byBucket), inMemoryDeadLetterQueue(), other)).toThrow(
      /concurrent and deadLettering must be given the same one/,
    )
    expect(() => concurrent(deadLettering(base("two-functions", []), inMemoryDeadLetterQueue(), other), byBucket)).toThrow(
      /concurrent and deadLettering must be given the same one/,
    )
  })
})

describe("batched", () => {
  it("takes a whole number of events, at least one", () => {
    expect(() => batched(base("no-batch", []), 0)).toThrow(/at least 1/)
    expect(() => batched(base("half-batch", []), 1.5)).toThrow(/at least 1/)
    expect(batched(base("thousand", []), 1000)).toMatchObject({ batchSize: 1000 })
  })
})
