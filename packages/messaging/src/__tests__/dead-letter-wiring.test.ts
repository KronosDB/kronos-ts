import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, type QualifiedName } from "@kronos-ts/common"
import { trackingEventProcessor } from "../tracking-event-processor.js"
import type { StreamableEventSource, SequencedEvent } from "../event-source.js"
import type { EventHandlerDefinition } from "../event-handler.js"
import { inMemoryDeadLetterQueue } from "../dead-letter-queue.js"
import { sequentialPerTag } from "../sequencing-policy.js"

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

describe("DLQ wiring (tracking processor, Option A)", () => {
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
    const handler: EventHandlerDefinition<any> = {
      kind: "event-handler",
      descriptor: { kind: "event", name: EVENT_NAME, version: "1.0", payload: {} as any },
      handler: ({ payload }: { payload: { v: number; poison?: boolean } }) => {
        if (payload.poison) throw new Error("poison pill")
        delivered.push(payload.v)
      },
    }

    const processor = trackingEventProcessor({
      name: "dlq-proc",
      eventSource,
      eventHandlers: [handler],
      deadLetterQueue: dlq,
      sequencingPolicy: sequentialPerTag("id"),
      pollingIntervalMs: 10,
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
    expect(await dlq.amountOfSequences()).toBe(1)
    expect(await dlq.size()).toBe(3)
    const seqA = await dlq.deadLetterSequence("A")
    expect(seqA.map((l) => (l.message.payload as { v: number }).v)).toEqual([1, 2, 4])
    expect(await dlq.contains("B")).toBe(false)
  })

  it("does not dead-letter anything when every handler succeeds", async () => {
    const events = [
      makeEvent({ v: 1 }, 0n, tag("A")),
      makeEvent({ v: 2 }, 1n, tag("A")),
    ]
    const dlq = inMemoryDeadLetterQueue()
    const delivered: number[] = []
    const handler: EventHandlerDefinition<any> = {
      kind: "event-handler",
      descriptor: { kind: "event", name: EVENT_NAME, version: "1.0", payload: {} as any },
      handler: ({ payload }: { payload: { v: number } }) => { delivered.push(payload.v) },
    }

    const processor = trackingEventProcessor({
      name: "dlq-proc-clean",
      eventSource: createInMemoryEventSource(events),
      eventHandlers: [handler],
      deadLetterQueue: dlq,
      sequencingPolicy: sequentialPerTag("id"),
      pollingIntervalMs: 10,
    })

    await processor.start()
    await waitForPosition(processor, 2n)
    processor.stop()

    expect(delivered).toEqual([1, 2])
    expect(await dlq.size()).toBe(0)
  })
})
