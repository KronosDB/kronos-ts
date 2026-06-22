import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { createTrackingEventProcessor } from "../tracking-event-processor.js"
import { createDeadLetteringDelivery } from "../dead-lettering-handler.js"
import { createInMemoryDeadLetterQueue, DeadLetterQueueOverflowError } from "../dead-letter-queue.js"
import { sequentialPerTag } from "../sequencing-policy.js"
import type { DeadLetterListener } from "../dead-letter-listener.js"
import type { StreamableEventSource, SequencedEvent } from "../event-source.js"
import type { EventHandlerRegistration } from "../handler.js"

const EVENT_NAME = qn("test", "SomethingHappened")

function makeEvent(sequence: bigint, value: string): SequencedEvent {
  return {
    sequence,
    event: {
      identifier: `evt-${sequence}`,
      name: EVENT_NAME,
      version: "1.0",
      payload: { value },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [{ key: "id", value }],
    },
  }
}

function recordingListener() {
  const calls: string[] = []
  const listener: DeadLetterListener = {
    onEnqueued(_l, info) { calls.push(info.blocked ? "enqueued:blocked" : "enqueued:fresh") },
    onEvicted() { calls.push("evicted") },
    onRequeued() { calls.push("requeued") },
    onReprocessSuccess() { calls.push("reprocess:success") },
    onReprocessFailure() { calls.push("reprocess:failure") },
    onOverflow() { calls.push("overflow") },
  }
  return { calls, listener }
}

const failingHandler: EventHandlerRegistration<any> = {
  kind: "event-handler",
  descriptor: { kind: "event", name: EVENT_NAME, version: "1.0", payload: {} as any },
  handler: () => { throw new Error("boom") },
}

describe("DLQ observability + reset", () => {
  it("notifies the listener on fresh enqueue, blocked enqueue, and overflow backpressure", async () => {
    const { calls, listener } = recordingListener()
    // maxSequenceSize 1 -> the second letter in a sequence overflows.
    const dlq = createInMemoryDeadLetterQueue({ maxSequenceSize: 1 })
    const delivery = createDeadLetteringDelivery({
      queue: dlq,
      sequencingPolicy: sequentialPerTag("id"),
      listener,
    })

    // First failure for sequence "A" -> fresh enqueue.
    await delivery.deliver(makeEvent(0n, "A"), [failingHandler])
    expect(calls).toEqual(["enqueued:fresh"])

    // Second event in "A": sequence present + full -> overflow propagates (backpressure).
    await expect(delivery.deliver(makeEvent(1n, "A"), [failingHandler])).rejects.toBeInstanceOf(
      DeadLetterQueueOverflowError,
    )
    expect(calls).toContain("overflow")
  })

  it("blocks subsequent events in a failed sequence and reports it", async () => {
    const { calls, listener } = recordingListener()
    const dlq = createInMemoryDeadLetterQueue() // default capacity
    const delivery = createDeadLetteringDelivery({
      queue: dlq,
      sequencingPolicy: sequentialPerTag("id"),
      listener,
    })

    await delivery.deliver(makeEvent(0n, "A"), [failingHandler]) // fresh
    await delivery.deliver(makeEvent(1n, "A"), [failingHandler]) // blocked (sequence already has a letter)

    expect(calls).toEqual(["enqueued:fresh", "enqueued:blocked"])
  })

  it("clears the DLQ on reset only when resetClearsDeadLetters is set", async () => {
    const events = [makeEvent(0n, "A")]
    const source: StreamableEventSource = {
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
      async getHeadPosition() { return 1n },
    }
    const dlq = createInMemoryDeadLetterQueue()

    const processor = createTrackingEventProcessor({
      name: "reset-proc",
      eventSource: source,
      eventHandlers: [failingHandler],
      deadLetterQueue: dlq,
      sequencingPolicy: sequentialPerTag("id"),
      resetClearsDeadLetters: true,
      pollingIntervalMs: 10,
    })

    await processor.start()
    // wait for the poison pill to be parked
    const deadline = Date.now() + 1000
    while ((await dlq.size()) === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 5))
    }
    expect(await dlq.size()).toBe(1)
    processor.stop()

    await processor.resetTokens(0n)
    expect(await dlq.size()).toBe(0)
  })
})
