import { describe, expect, it } from "bun:test"
import { qn } from "@kronos-ts/common"
import {
  createInMemoryDeadLetterQueue,
  createDeadLetter,
  type DeadLetter,
  type EnqueueDecision,
} from "../dead-letter-queue.js"
import type { EventMessage } from "../message.js"

function testEvent(name: string, payload: unknown = {}): EventMessage {
  return {
    identifier: `id-${name}`,
    name: qn("test", name),
    version: "1.0",
    payload,
    metadata: {},
    timestamp: Date.now(),
    tags: [],
  }
}

function letter(seqId: string, eventName: string = "TestEvent"): DeadLetter {
  return createDeadLetter(
    testEvent(eventName),
    new Error("test failure"),
    seqId,
  )
}

describe("InMemorySequencedDeadLetterQueue", () => {
  describe("enqueue and contains", () => {
    it("enqueues a dead letter", async () => {
      const dlq = createInMemoryDeadLetterQueue()

      await dlq.enqueue(letter("seq-1"))

      expect(await dlq.contains("seq-1")).toBe(true)
      expect(dlq.size()).toBe(1)
      expect(dlq.amountOfSequences()).toBe(1)
    })

    it("enqueues multiple letters in same sequence", async () => {
      const dlq = createInMemoryDeadLetterQueue()

      await dlq.enqueue(letter("seq-1"))
      await dlq.enqueue(letter("seq-1"))

      expect(dlq.size()).toBe(2)
      expect(dlq.amountOfSequences()).toBe(1)
    })

    it("enqueues letters in different sequences", async () => {
      const dlq = createInMemoryDeadLetterQueue()

      await dlq.enqueue(letter("seq-1"))
      await dlq.enqueue(letter("seq-2"))

      expect(dlq.size()).toBe(2)
      expect(dlq.amountOfSequences()).toBe(2)
    })

    it("returns false for unknown sequence", async () => {
      const dlq = createInMemoryDeadLetterQueue()

      expect(await dlq.contains("unknown")).toBe(false)
    })
  })

  describe("enqueueIfPresent", () => {
    it("enqueues when sequence exists", async () => {
      const dlq = createInMemoryDeadLetterQueue()
      await dlq.enqueue(letter("seq-1"))

      const result = await dlq.enqueueIfPresent("seq-1", () => letter("seq-1"))

      expect(result).toBe(true)
      expect(dlq.size()).toBe(2)
    })

    it("does not enqueue when sequence does not exist", async () => {
      const dlq = createInMemoryDeadLetterQueue()

      let supplierCalled = false
      const result = await dlq.enqueueIfPresent("seq-1", () => {
        supplierCalled = true
        return letter("seq-1")
      })

      expect(result).toBe(false)
      expect(dlq.size()).toBe(0)
      expect(supplierCalled).toBe(false) // Supplier should NOT be called
    })
  })

  describe("evict", () => {
    it("removes a specific letter from the sequence", async () => {
      const dlq = createInMemoryDeadLetterQueue()
      const l1 = letter("seq-1")
      const l2 = letter("seq-1")
      await dlq.enqueue(l1)
      await dlq.enqueue(l2)

      await dlq.evict("seq-1", l1)

      expect(dlq.size()).toBe(1)
      const remaining = await dlq.deadLetterSequence("seq-1")
      expect(remaining[0]).toBe(l2)
    })

    it("removes sequence when last letter is evicted", async () => {
      const dlq = createInMemoryDeadLetterQueue()
      const l1 = letter("seq-1")
      await dlq.enqueue(l1)

      await dlq.evict("seq-1", l1)

      expect(await dlq.contains("seq-1")).toBe(false)
      expect(dlq.amountOfSequences()).toBe(0)
    })
  })

  describe("deadLetterSequence", () => {
    it("returns letters in insertion order", async () => {
      const dlq = createInMemoryDeadLetterQueue()
      const l1 = letter("seq-1")
      const l2 = letter("seq-1")
      const l3 = letter("seq-1")
      await dlq.enqueue(l1)
      await dlq.enqueue(l2)
      await dlq.enqueue(l3)

      const seq = await dlq.deadLetterSequence("seq-1")

      expect(seq).toEqual([l1, l2, l3])
    })

    it("returns empty array for unknown sequence", async () => {
      const dlq = createInMemoryDeadLetterQueue()

      expect(await dlq.deadLetterSequence("unknown")).toEqual([])
    })
  })

  describe("process", () => {
    it("processes oldest sequence first", async () => {
      const dlq = createInMemoryDeadLetterQueue()

      // Create letters with different lastTouched times
      const old = { ...letter("seq-old"), lastTouched: 1000 }
      const recent = { ...letter("seq-recent"), lastTouched: 2000 }
      await dlq.enqueue(old)
      await dlq.enqueue(recent)

      const processed: string[] = []
      await dlq.process(
        () => true,
        async (l) => {
          processed.push(l.sequenceIdentifier)
          return { shouldEnqueue: false }
        },
      )

      expect(processed).toEqual(["seq-old"])
    })

    it("evicts letters when processingTask returns shouldEnqueue=false", async () => {
      const dlq = createInMemoryDeadLetterQueue()
      await dlq.enqueue(letter("seq-1"))
      await dlq.enqueue(letter("seq-1"))

      await dlq.process(
        () => true,
        async () => ({ shouldEnqueue: false }),
      )

      expect(dlq.size()).toBe(0)
    })

    it("requeues and stops when processingTask returns shouldEnqueue=true", async () => {
      const dlq = createInMemoryDeadLetterQueue()
      const l1 = letter("seq-1")
      const l2 = letter("seq-1")
      await dlq.enqueue(l1)
      await dlq.enqueue(l2)

      let processedCount = 0
      await dlq.process(
        () => true,
        async () => {
          processedCount++
          return { shouldEnqueue: true } // Still failing
        },
      )

      // Should have only tried the first letter
      expect(processedCount).toBe(1)
      // Both letters still in queue
      expect(dlq.size()).toBe(2)
    })

    it("returns false when no matching sequences", async () => {
      const dlq = createInMemoryDeadLetterQueue()
      await dlq.enqueue(letter("seq-1"))

      const result = await dlq.process(
        (id) => id === "nonexistent",
        async () => ({ shouldEnqueue: false }),
      )

      expect(result).toBe(false)
    })

    it("respects sequence filter", async () => {
      const dlq = createInMemoryDeadLetterQueue()
      await dlq.enqueue({ ...letter("skip"), lastTouched: 1000 })
      await dlq.enqueue({ ...letter("process"), lastTouched: 2000 })

      const processed: string[] = []
      await dlq.process(
        (id) => id === "process",
        async (l) => {
          processed.push(l.sequenceIdentifier)
          return { shouldEnqueue: false }
        },
      )

      expect(processed).toEqual(["process"])
    })
  })

  describe("overflow protection", () => {
    it("throws on max sequences exceeded", async () => {
      const dlq = createInMemoryDeadLetterQueue({ maxSequences: 2 })
      await dlq.enqueue(letter("seq-1"))
      await dlq.enqueue(letter("seq-2"))

      expect(dlq.enqueue(letter("seq-3"))).rejects.toThrow("Dead letter queue overflow")
    })

    it("throws on max sequence size exceeded", async () => {
      const dlq = createInMemoryDeadLetterQueue({ maxSequenceSize: 2 })
      await dlq.enqueue(letter("seq-1"))
      await dlq.enqueue(letter("seq-1"))

      expect(dlq.enqueue(letter("seq-1"))).rejects.toThrow("Dead letter queue overflow")
    })
  })

  describe("clear", () => {
    it("removes all dead letters", async () => {
      const dlq = createInMemoryDeadLetterQueue()
      await dlq.enqueue(letter("seq-1"))
      await dlq.enqueue(letter("seq-2"))

      await dlq.clear()

      expect(dlq.size()).toBe(0)
      expect(dlq.amountOfSequences()).toBe(0)
    })
  })
})
