import { describe, expect, it } from "bun:test"
import { qn, type QualifiedName, emptyMetadata, type EventMessage } from "../../messaging/messages.js"
import { deadLettering, eventProcessor, polling } from "../processor.js"
import { runEventProcessor } from "../running-processor.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import { inMemoryTokenStore } from "../token-store.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import type { StreamableEventSource, SequencedEvent } from "../source.js"
import { globalSequenceToken } from "../tracking-token.js"
import type { EventHandler } from "../handler.js"
import { inMemoryDeadLetterQueue } from "../dead-letter-queue.js"

const byId = (e: EventMessage) => e.tags.find((t) => t.key === "id")?.value ?? e.identifier

// ---------------------------------------------------------------------------
// Test helpers (mirrors tracking-event-processor.test.ts, with tags)
// ---------------------------------------------------------------------------

const EVENT_NAME = qn("test", "SomethingHappened")

function makeEvent(
  payload: unknown,
  sequence: bigint,
  tags: ReadonlyArray<{ key: string; value: string }> = [],
): SequencedEvent {
  return {
    sequence,
    event: {
      kind: "event",
      identifier: `evt-${sequence}`,
      name: EVENT_NAME,
      version: "1.0",
      payload,
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags,
    },
  }
}

function createInMemoryEventSource(events: SequencedEvent[]): StreamableEventSource {
  return {
    open(condition) {
      let cursor = condition.position
      let callback: (() => void) | null = null
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
        setCallback(cb) { callback = cb },
        close() { callback = null },
      }
    },
    async getHeadPosition() {
      if (events.length === 0) return 0n
      return events[events.length - 1]!.sequence + 1n
    },
    async firstToken() {
      return globalSequenceToken(0n)
    },
    async latestToken() {
      return globalSequenceToken(await this.getHeadPosition())
    },
  }
}

async function waitForPosition(
  proc: { position: bigint },
  target: bigint,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (proc.position < target && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 5))
  }
  if (proc.position < target) {
    throw new Error(`did not reach position ${target} (at ${proc.position})`)
  }
}

function tag(value: string) {
  return [{ key: "id", value }]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("dead-letter wiring — park the pill, advance the token", () => {
  it("parks a poison pill, advances the token past it, and blocks the rest of that sequence while other sequences keep processing", async () => {
    // given: sequence A has a poison pill (seq 0); B is healthy (seq 2).
    const events = [
      makeEvent({ v: 1, poison: true }, 0n, tag("A")), // fails -> dead-lettered
      makeEvent({ v: 2 }, 1n, tag("A")),               // blocked behind the poison pill
      makeEvent({ v: 3 }, 2n, tag("B")),               // different sequence -> processed
      makeEvent({ v: 4 }, 3n, tag("A")),               // still blocked (A has dead letters)
    ]
    const eventSource = createInMemoryEventSource(events)
    const dlq = inMemoryDeadLetterQueue()

    const delivered: number[] = []
    const handler: EventHandler<any> = {
      kind: "event-handler",
      descriptor: { kind: "event", name: EVENT_NAME, version: "1.0", payload: {} as any },
      handler: ({ payload }: { payload: { v: number; poison?: boolean } }) => {
        if (payload.poison) throw new Error("poison pill")
        delivered.push(payload.v)
      },
    }

    const processor = runEventProcessor({
      processor: polling(
        deadLettering(eventProcessor("dlq-proc", eventSource as unknown as EventStore, inMemoryTokenStore(), unitOfWork), dlq, byId),
        10,
      ),
      handlers: [{ definition: handler }],
    })

    // when
    await processor.start()
    await waitForPosition(processor, 4n) // token advanced past the poison pill — no infinite redelivery
    processor.stop()

    // then
    // Only the healthy B event ran; the poison pill never succeeded.
    expect(delivered).toEqual([3])
    // Token advanced to the end despite the failure (Option A).
    expect(processor.position).toBe(4n)
    // Sequence A holds 3 letters (poison + two blocked); B holds none.
    expect(await dlq.amountOfSequences("dlq-proc")).toBe(1)
    expect(await dlq.size("dlq-proc")).toBe(3)
    const seqA = await dlq.deadLetterSequence("dlq-proc", "A")
    expect(seqA.map((l) => (l.message.payload as { v: number }).v)).toEqual([1, 2, 4])
    expect(await dlq.contains("dlq-proc", "B")).toBe(false)
  })

  it("does not dead-letter anything when every handler succeeds", async () => {
    const events = [
      makeEvent({ v: 1 }, 0n, tag("A")),
      makeEvent({ v: 2 }, 1n, tag("A")),
    ]
    const dlq = inMemoryDeadLetterQueue()
    const delivered: number[] = []
    const handler: EventHandler<any> = {
      kind: "event-handler",
      descriptor: { kind: "event", name: EVENT_NAME, version: "1.0", payload: {} as any },
      handler: ({ payload }: { payload: { v: number } }) => { delivered.push(payload.v) },
    }

    const processor = runEventProcessor({
      processor: polling(
        deadLettering(eventProcessor("dlq-proc-clean", createInMemoryEventSource(events) as unknown as EventStore, inMemoryTokenStore(), unitOfWork), dlq, byId),
        10,
      ),
      handlers: [{ definition: handler }],
    })

    await processor.start()
    await waitForPosition(processor, 2n)
    processor.stop()

    expect(delivered).toEqual([1, 2])
    expect(await dlq.size("dlq-proc-clean")).toBe(0)
  })
})

// A log may match a query against an event's tags and hand the event back
// without them. The handler's descriptor gives the same tags.
describe("a processor over a log that hands events back without tags", () => {
  const descriptor = {
    kind: "event" as const,
    name: EVENT_NAME,
    version: "1.0",
    payload: {} as any,
    tags: { id: (p: { id: string }) => p.id },
  }

  it("sequences an event by the tags its descriptor gives it", async () => {
    const events = [
      makeEvent({ id: "A", v: 1, poison: true }, 0n),
      makeEvent({ id: "A", v: 2 }, 1n),
      makeEvent({ id: "B", v: 3 }, 2n),
    ]
    const dlq = inMemoryDeadLetterQueue()
    const delivered: number[] = []
    const handler: EventHandler<any> = {
      kind: "event-handler",
      descriptor,
      handler: ({ payload }: { payload: { v: number; poison?: boolean } }) => {
        if (payload.poison) throw new Error("poison pill")
        delivered.push(payload.v)
      },
    }

    const processor = runEventProcessor({
      processor: polling(
        deadLettering(eventProcessor("dlq-proc-untagged", createInMemoryEventSource(events) as unknown as EventStore, inMemoryTokenStore(), unitOfWork), dlq, byId),
        10,
      ),
      handlers: [{ definition: handler }],
    })

    await processor.start()
    await waitForPosition(processor, 3n)
    processor.stop()

    // The second event waits behind the first, in sequence A.
    expect(delivered).toEqual([3])
    expect((await dlq.deadLetterSequence("dlq-proc-untagged", "A")).map((l) => (l.message.payload as { v: number }).v)).toEqual([1, 2])
  })

  it("hands a handler the event with the tags its descriptor gives it", async () => {
    const events = [makeEvent({ id: "A" }, 0n), makeEvent({ id: "B" }, 1n, tag("stored"))]
    const seen: Array<ReadonlyArray<{ key: string; value: string }>> = []
    const handler: EventHandler<any> = {
      kind: "event-handler",
      descriptor,
      handler: (event) => {
        seen.push(event.tags)
      },
    }

    const processor = runEventProcessor({
      processor: polling(
        eventProcessor("proc-untagged", createInMemoryEventSource(events) as unknown as EventStore, inMemoryTokenStore(), unitOfWork),
        10,
      ),
      handlers: [{ definition: handler }],
    })

    await processor.start()
    await waitForPosition(processor, 2n)
    processor.stop()

    // An event read back with tags keeps the ones it was stored with.
    expect(seen).toEqual([[{ key: "id", value: "A" }], [{ key: "id", value: "stored" }]])
  })

  it("picks the descriptor of the event's version when a name has several", async () => {
    const events = [makeEvent({ id: "A", key: "K" }, 0n)]
    const seen: Array<ReadonlyArray<{ key: string; value: string }>> = []
    const older: EventHandler<any> = {
      kind: "event-handler",
      descriptor: { ...descriptor, version: "0.9", tags: { key: (p: { key: string }) => p.key } },
      handler: () => {},
    }
    const current: EventHandler<any> = {
      kind: "event-handler",
      descriptor,
      handler: (event) => {
        seen.push(event.tags)
      },
    }

    const processor = runEventProcessor({
      processor: polling(
        eventProcessor("proc-untagged-versions", createInMemoryEventSource(events) as unknown as EventStore, inMemoryTokenStore(), unitOfWork),
        10,
      ),
      handlers: [{ definition: older }, { definition: current }],
    })

    await processor.start()
    await waitForPosition(processor, 1n)
    processor.stop()

    expect(seen).toEqual([[{ key: "id", value: "A" }]])
  })
})
