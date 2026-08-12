import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, type QualifiedName } from "@kronos-ts/common"
import { streamingEventProcessor } from "../streaming-event-processor.js"
import { propagatingErrorHandler } from "../tracking-event-processor.js"
import type { StreamableEventSource, SequencedEvent } from "../event-source.js"
import type { EventHandlerDefinition } from "../event-handler.js"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeEvent(name: QualifiedName, payload: unknown, sequence: bigint): SequencedEvent {
  return {
    sequence,
    event: {
      identifier: `evt-${sequence}`,
      name,
      version: "1.0",
      payload,
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [],
    },
  }
}

function createInMemoryEventSource(events: SequencedEvent[]): StreamableEventSource {
  const listeners = new Set<() => void>()

  return {
    open(condition) {
      let cursor = condition.position
      let callback: (() => void) | null = null

      const listener = () => {
        if (callback) callback()
      }
      listeners.add(listener)

      return {
        next() {
          const found = events.find((e) => e.sequence >= cursor)
          if (!found) return undefined
          cursor = found.sequence + 1n
          return found
        },
        peek() {
          return events.find((e) => e.sequence >= cursor)
        },
        hasNextAvailable() {
          return events.some((e) => e.sequence >= cursor)
        },
        isCompleted() { return false },
        error() { return undefined },
        setCallback(cb) { callback = cb },
        close() {
          callback = null
          listeners.delete(listener)
        },
      }
    },
    async getHeadPosition() {
      if (events.length === 0) return 0n
      return events[events.length - 1]!.sequence + 1n
    },
  }
}

const TEST_EVENT_NAME = qn("test", "SomethingHappened")

async function waitForPosition(
  proc: { position: bigint; running: boolean },
  target: bigint,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (proc.position < target && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 10))
  }
  if (proc.position < target) {
    throw new Error(`Processor did not reach position ${target} within ${timeoutMs}ms (at ${proc.position})`)
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("StreamingEventProcessor", () => {
  it("reads events from the event source and delivers to handlers", async () => {
    // given
    const delivered: unknown[] = []
    const events = [
      makeEvent(TEST_EVENT_NAME, { value: 1 }, 0n),
      makeEvent(TEST_EVENT_NAME, { value: 2 }, 1n),
    ]
    const eventSource = createInMemoryEventSource(events)

    const handler: EventHandlerDefinition<any> = {
      kind: "event-handler",
      descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
      handler: ({ payload }: any) => { delivered.push(payload) },
    }

    const processor = streamingEventProcessor({
      name: "test-processor",
      eventSource,
      eventHandlers: [handler],
    })

    // when
    await processor.start()
    await waitForPosition(processor, 2n)
    processor.stop()

    // then
    expect(delivered).toEqual([{ value: 1 }, { value: 2 }])
  })

  it("redelivers a failed batch on the next cycle without a restart", async () => {
    // given -- a handler that throws the first time it sees sequence 1n, then
    // succeeds. Before the fix, the live stream cursor advanced past the failed
    // batch during accumulation while the token did not, so the event was
    // skipped until a restart and the position never reached 2n.
    const attempts: bigint[] = []
    const events = [
      makeEvent(TEST_EVENT_NAME, { value: 1 }, 0n),
      makeEvent(TEST_EVENT_NAME, { value: 2 }, 1n),
    ]
    const eventSource = createInMemoryEventSource(events)

    let thrown = false
    const handler: EventHandlerDefinition<any> = {
      kind: "event-handler",
      descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
      handler: ({ sequence }: any) => {
        attempts.push(sequence)
        if (sequence === 1n && !thrown) {
          thrown = true
          throw new Error("transient handler failure")
        }
      },
    }

    const processor = streamingEventProcessor({
      name: "test-processor",
      eventSource,
      eventHandlers: [handler],

      errorHandler: propagatingErrorHandler(),
      errorBackoffMs: 10,
    })

    // when -- never restarted; reaching 2n requires redelivery of the failed event
    await processor.start()
    await waitForPosition(processor, 2n)
    processor.stop()

    // then -- the failed event was retried and the checkpoint advanced past it
    expect(processor.position).toBe(2n)
    expect(attempts.filter((s) => s === 1n).length).toBeGreaterThanOrEqual(2)
  })
})
