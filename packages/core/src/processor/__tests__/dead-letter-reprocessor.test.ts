import { describe, expect, it } from "bun:test"
import { qn } from "../../primitives/qualified-name.js"
import { emptyMetadata } from "../../primitives/metadata.js"
import { eventProcessor } from "../event-processor.js"
import { runEventProcessor } from "../running-processor.js"
import type { EventStore } from "../../stores/event-store.js"
import { inMemoryTokenStore } from "../../stores/token-store.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import type { StreamableEventSource, SequencedEvent } from "../event-source.js"
import type { EventHandlerDefinition } from "../../handlers/event-handler.js"
import { inMemoryDeadLetterQueue } from "../../stores/dead-letter-queue.js"
import { sequentialPerTag } from "../sequence.js"

const EVENT_NAME = qn("test", "SomethingHappened")

function makeEvent(payload: unknown, sequence: bigint, value: string): SequencedEvent {
  return {
    sequence,
    event: {
      identifier: `evt-${sequence}`,
      name: EVENT_NAME,
      version: "1.0",
      payload,
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [{ key: "id", value }],
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
      return events.length === 0 ? 0n : events[events.length - 1]!.sequence + 1n
    },
  }
}

async function waitForPosition(proc: { position: bigint }, target: bigint, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs
  while (proc.position < target && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 5))
  }
  if (proc.position < target) throw new Error(`did not reach ${target} (at ${proc.position})`)
}

describe("dead-letter reprocessing", () => {
  it("drains a sequence once the underlying failure is fixed, in order, unblocking it", async () => {
    const events = [
      makeEvent({ v: 1 }, 0n, "A"),
      makeEvent({ v: 2 }, 1n, "A"),
      makeEvent({ v: 3 }, 2n, "B"),
    ]
    const dlq = inMemoryDeadLetterQueue()
    const delivered: number[] = []
    let broken = true // sequence A's handler fails while broken

    const handler: EventHandlerDefinition<any> = {
      kind: "event-handler",
      descriptor: { kind: "event", name: EVENT_NAME, version: "1.0", payload: {} as any },
      handler: ({ payload, tags }: { payload: { v: number }; tags: ReadonlyArray<{ key: string; value: string }> }) => {
        const id = tags.find((t) => t.key === "id")!.value
        if (id === "A" && broken) throw new Error("downstream down")
        delivered.push(payload.v)
      },
    }

    const processor = runEventProcessor({
      processor: eventProcessor({
        name: "reproc",
        eventStore: createInMemoryEventSource(events) as unknown as EventStore,
        tokenStore: inMemoryTokenStore(),
        unitOfWork,
        deadLetterQueue: dlq,
        sequence: sequentialPerTag("id"),
      }),
      handlers: [{ definition: handler }],
      pollingIntervalMs: 10,
    })

    await processor.start()
    await waitForPosition(processor, 3n)

    // B processed live; A parked (poison + blocked).
    expect(delivered).toEqual([3])
    expect(await dlq.size("reproc")).toBe(2)

    // Fix the downstream and drain.
    broken = false
    const processed = await processor.reprocessDeadLetters()
    processor.stop()

    expect(processed).toBe(true)
    expect(await dlq.size("reproc")).toBe(0)
    expect(await dlq.contains("reproc", "A")).toBe(false)
    // A's events replayed in order.
    expect(delivered).toEqual([3, 1, 2])
  })

  it("keeps a still-failing letter parked rather than giving up on it", async () => {
    // There is no retry budget any more. A budget configured before the failure
    // exists turns "we kept the evidence" into "we deleted it on the fifth
    // try" — giving up is an operator decision taken against the parked letter,
    // so a failed replay requeues and the letter stays.
    const events = [makeEvent({ v: 1 }, 0n, "A")]
    const dlq = inMemoryDeadLetterQueue()

    const handler: EventHandlerDefinition<any> = {
      kind: "event-handler",
      descriptor: { kind: "event", name: EVENT_NAME, version: "1.0", payload: {} as any },
      handler: () => { throw new Error("always fails") },
    }

    const processor = runEventProcessor({
      processor: eventProcessor({
        name: "reproc-cap",
        eventStore: createInMemoryEventSource(events) as unknown as EventStore,
        tokenStore: inMemoryTokenStore(),
        unitOfWork,
        deadLetterQueue: dlq,
        sequence: sequentialPerTag("id"),
      }),
      handlers: [{ definition: handler }],
      pollingIntervalMs: 10,
    })

    await processor.start()
    await waitForPosition(processor, 1n)
    expect(await dlq.size("reproc-cap")).toBe(1)

    // The replay fails again — requeued, not evicted.
    expect(await processor.reprocessDeadLetters()).toBe(true)
    processor.stop()

    expect(await dlq.size("reproc-cap")).toBe(1)
    expect(await dlq.contains("reproc-cap", "A")).toBe(true)
  })
})
