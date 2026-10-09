import { describe, expect, it } from "bun:test"
import { unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { qn, type EventMessage } from "../../messaging/messages.js"
import { inMemoryDeadLetterQueue, deadLetter, type DeadLetter, type EnqueueDecision } from "../dead-letter-queue.js"
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

/** Every call names its partition, exactly as a processor does. */
const GROUP = "test-processor"

type Deferred = { promise: Promise<void>; resolve: () => void }
function deferred(): Deferred {
  let resolve!: () => void
  const promise = new Promise<void>((res) => {
    resolve = res
  })
  return { promise, resolve }
}

function letter(seqId: string, eventName: string = "TestEvent"): DeadLetter {
  return deadLetter(
    testEvent(eventName),
    new Error("test failure"),
    seqId,
  )
}

describe("InMemorySequencedDeadLetterQueue", () => {
  describe("enqueue and contains", () => {
    it("enqueues a dead letter", async () => {
      const dlq = inMemoryDeadLetterQueue()

      await dlq.enqueue(GROUP, letter("seq-1"))

      expect(await dlq.contains(GROUP, "seq-1")).toBe(true)
      expect(await dlq.size(GROUP)).toBe(1)
      expect(await dlq.amountOfSequences(GROUP)).toBe(1)
    })

    it("enqueues multiple letters in same sequence", async () => {
      const dlq = inMemoryDeadLetterQueue()

      await dlq.enqueue(GROUP, letter("seq-1"))
      await dlq.enqueue(GROUP, letter("seq-1"))

      expect(await dlq.size(GROUP)).toBe(2)
      expect(await dlq.amountOfSequences(GROUP)).toBe(1)
    })

    it("enqueues letters in different sequences", async () => {
      const dlq = inMemoryDeadLetterQueue()

      await dlq.enqueue(GROUP, letter("seq-1"))
      await dlq.enqueue(GROUP, letter("seq-2"))

      expect(await dlq.size(GROUP)).toBe(2)
      expect(await dlq.amountOfSequences(GROUP)).toBe(2)
    })

    it("returns false for unknown sequence", async () => {
      const dlq = inMemoryDeadLetterQueue()

      expect(await dlq.contains(GROUP, "unknown")).toBe(false)
    })
  })

  describe("enqueueIfPresent", () => {
    it("enqueues when sequence exists", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, letter("seq-1"))

      const result = await dlq.enqueueIfPresent(GROUP, "seq-1", () => letter("seq-1"))

      expect(result).toBe(true)
      expect(await dlq.size(GROUP)).toBe(2)
    })

    it("does not enqueue when sequence does not exist", async () => {
      const dlq = inMemoryDeadLetterQueue()

      let supplierCalled = false
      const result = await dlq.enqueueIfPresent(GROUP, "seq-1", () => {
        supplierCalled = true
        return letter("seq-1")
      })

      expect(result).toBe(false)
      expect(await dlq.size(GROUP)).toBe(0)
      expect(supplierCalled).toBe(false) // Supplier should NOT be called
    })
  })

  describe("evict", () => {
    it("removes a specific letter from the sequence", async () => {
      const dlq = inMemoryDeadLetterQueue()
      const l1 = letter("seq-1")
      const l2 = letter("seq-1")
      await dlq.enqueue(GROUP, l1)
      await dlq.enqueue(GROUP, l2)

      await dlq.evict(GROUP, "seq-1", l1)

      expect(await dlq.size(GROUP)).toBe(1)
      const remaining = await dlq.deadLetterSequence(GROUP, "seq-1")
      expect(remaining[0]).toBe(l2)
    })

    it("removes sequence when last letter is evicted", async () => {
      const dlq = inMemoryDeadLetterQueue()
      const l1 = letter("seq-1")
      await dlq.enqueue(GROUP, l1)

      await dlq.evict(GROUP, "seq-1", l1)

      expect(await dlq.contains(GROUP, "seq-1")).toBe(false)
      expect(await dlq.amountOfSequences(GROUP)).toBe(0)
    })
  })

  describe("deadLetterSequence", () => {
    it("returns letters in insertion order", async () => {
      const dlq = inMemoryDeadLetterQueue()
      const l1 = letter("seq-1")
      const l2 = letter("seq-1")
      const l3 = letter("seq-1")
      await dlq.enqueue(GROUP, l1)
      await dlq.enqueue(GROUP, l2)
      await dlq.enqueue(GROUP, l3)

      const seq = await dlq.deadLetterSequence(GROUP, "seq-1")

      expect(seq).toEqual([l1, l2, l3])
    })

    it("returns empty array for unknown sequence", async () => {
      const dlq = inMemoryDeadLetterQueue()

      expect(await dlq.deadLetterSequence(GROUP, "unknown")).toEqual([])
    })
  })

  describe("process", () => {
    it("processes oldest sequence first", async () => {
      const dlq = inMemoryDeadLetterQueue()

      // Create letters with different lastTouched times
      const old = { ...letter("seq-old"), lastTouched: 1000 }
      const recent = { ...letter("seq-recent"), lastTouched: 2000 }
      await dlq.enqueue(GROUP, old)
      await dlq.enqueue(GROUP, recent)

      const processed: string[] = []
      await dlq.process(GROUP, 
        () => true,
        async (l) => {
          processed.push(l.sequenceIdentifier)
          return { shouldEnqueue: false }
        },
        unitOfWork,
      )

      expect(processed).toEqual(["seq-old"])
    })

    it("evicts letters when processingTask returns shouldEnqueue=false", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, letter("seq-1"))
      await dlq.enqueue(GROUP, letter("seq-1"))

      await dlq.process(GROUP, 
        () => true,
        async () => ({ shouldEnqueue: false }),
        unitOfWork,
      )

      expect(await dlq.size(GROUP)).toBe(0)
    })

    it("requeues and stops when processingTask returns shouldEnqueue=true", async () => {
      const dlq = inMemoryDeadLetterQueue()
      const l1 = letter("seq-1")
      const l2 = letter("seq-1")
      await dlq.enqueue(GROUP, l1)
      await dlq.enqueue(GROUP, l2)

      let processedCount = 0
      await dlq.process(GROUP, 
        () => true,
        async () => {
          processedCount++
          return { shouldEnqueue: true } // Still failing
        },
        unitOfWork,
      )

      // Should have only tried the first letter
      expect(processedCount).toBe(1)
      // Both letters still in queue
      expect(await dlq.size(GROUP)).toBe(2)
    })

    it("returns false when no matching sequences", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, letter("seq-1"))

      const result = await dlq.process(GROUP, 
        (id) => id === "nonexistent",
        async () => ({ shouldEnqueue: false }),
        unitOfWork,
      )

      expect(result).toBe(false)
    })

    it("respects sequence filter", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, { ...letter("skip"), lastTouched: 1000 })
      await dlq.enqueue(GROUP, { ...letter("process"), lastTouched: 2000 })

      const processed: string[] = []
      await dlq.process(GROUP, 
        (id) => id === "process",
        async (l) => {
          processed.push(l.sequenceIdentifier)
          return { shouldEnqueue: false }
        },
        unitOfWork,
      )

      expect(processed).toEqual(["process"])
    })
  })

  describe("process: one unit of work per letter", () => {
    /** Mint units of work that remember themselves, so a test can look at each. */
    function minting() {
      const minted: UnitOfWork[] = []
      const factory = () => {
        const uow = unitOfWork()
        minted.push(uow)
        return uow
      }
      return { minted, factory }
    }

    it("hands each letter a unit of work of its own, minted by the factory", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, letter("seq-1"))
      await dlq.enqueue(GROUP, letter("seq-1"))
      await dlq.enqueue(GROUP, letter("seq-1"))
      const { minted, factory } = minting()

      const handed: UnitOfWork[] = []
      await dlq.process(GROUP, () => true, async (_l, uow) => {
        handed.push(uow)
        return { shouldEnqueue: false }
      }, factory)

      expect(minted.length).toBe(3)
      expect(handed).toEqual(minted)
      expect(new Set(handed).size).toBe(3)
      // Each ran to completion: the lifecycle ran, and nothing is left open.
      expect(handed.every((uow) => uow.closed)).toBe(true)
    })

    it("evicts a letter before the next letter's task starts", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, letter("seq-1", "A"))
      await dlq.enqueue(GROUP, letter("seq-1", "B"))
      const entered = deferred()
      const release = deferred()
      let calls = 0

      const walk = dlq.process(GROUP, () => true, async () => {
        calls++
        if (calls === 2) {
          entered.resolve()
          await release.promise
        }
        return { shouldEnqueue: false }
      }, unitOfWork)

      await entered.promise
      // Letter 1's unit of work has committed while letter 2 is still running.
      expect(await dlq.size(GROUP)).toBe(1)
      expect((await dlq.deadLetterSequence(GROUP, "seq-1"))[0]!.message.identifier).toBe("id-B")
      release.resolve()
      await walk
      expect(await dlq.size(GROUP)).toBe(0)
    })

    it("rolls a failing letter's unit of work back and requeues it with the decision", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, letter("seq-1", "A"))
      await dlq.enqueue(GROUP, letter("seq-1", "B"))
      await dlq.enqueue(GROUP, letter("seq-1", "C"))
      const { minted, factory } = minting()

      const events: string[] = []
      const processed = await dlq.process(GROUP, () => true, async (l, uow) => {
        const id = l.message.identifier
        uow.onPrepareCommit(() => void events.push(`prepare:${id}`))
        uow.onCommit(() => void events.push(`commit:${id}`))
        uow.onError(() => void events.push(`error:${id}`))
        uow.events.buffered.push(l.message)
        return id === "id-B"
          ? { shouldEnqueue: true, cause: new TypeError("still failing"), diagnostics: { note: "kept" } }
          : { shouldEnqueue: false }
      }, factory)

      expect(processed).toBe(true)
      // A committed. B rolled back: its commit hooks never ran, its error hooks did.
      expect(events).toEqual(["prepare:id-A", "commit:id-A", "error:id-B"])
      // The walk stopped at B: C never got a unit of work.
      expect(minted.length).toBe(2)
      const failed = minted[1]!
      expect(failed.closed).toBe(true)
      // What B buffered stays on its own, closed unit of work; nothing flushes it
      // and no later unit of work shares it.
      expect(minted[0]!.events.buffered).not.toBe(failed.events.buffered)

      const lane = await dlq.deadLetterSequence(GROUP, "seq-1")
      expect(lane.map((l) => l.message.identifier)).toEqual(["id-B", "id-C"])
      expect(lane[0]!.cause.name).toBe("TypeError")
      expect(lane[0]!.diagnostics["note"]).toBe("kept")
    })

    it("rethrows a task that throws, keeping its letter and the lane claimable", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, letter("seq-1", "A"))
      await dlq.enqueue(GROUP, letter("seq-1", "B"))

      await expect(
        dlq.process(GROUP, () => true, async (l) => {
          if (l.message.identifier === "id-B") throw new Error("task broke")
          return { shouldEnqueue: false }
        }, unitOfWork),
      ).rejects.toThrow("task broke")

      // A was evicted with its own unit of work; B is neither evicted nor requeued.
      const lane = await dlq.deadLetterSequence(GROUP, "seq-1")
      expect(lane.map((l) => l.message.identifier)).toEqual(["id-B"])
      expect(lane[0]!.cause.message).toBe("test failure")
      // The lane was handed back.
      expect(await dlq.process(GROUP, () => true, async () => ({ shouldEnqueue: false }), unitOfWork)).toBe(true)
      expect(await dlq.size(GROUP)).toBe(0)
    })

    it("keeps a letter whose unit of work fails to commit", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, letter("seq-1", "A"))
      await dlq.enqueue(GROUP, letter("seq-1", "B"))

      let calls = 0
      await expect(
        dlq.process(GROUP, () => true, async (_l, uow) => {
          calls++
          uow.onPrepareCommit(() => {
            throw new Error("flush failed")
          })
          return { shouldEnqueue: false }
        }, unitOfWork),
      ).rejects.toThrow("flush failed")

      // The commit failed, so the eviction did not happen, and the walk stopped.
      expect(calls).toBe(1)
      expect(await dlq.size(GROUP)).toBe(2)
    })
  })

  describe("overflow protection", () => {
    it("throws on max sequences exceeded", async () => {
      const dlq = inMemoryDeadLetterQueue({ maxSequences: 2 })
      await dlq.enqueue(GROUP, letter("seq-1"))
      await dlq.enqueue(GROUP, letter("seq-2"))

      expect(dlq.enqueue(GROUP, letter("seq-3"))).rejects.toThrow("Dead letter queue overflow")
    })

    it("throws on max sequence size exceeded", async () => {
      const dlq = inMemoryDeadLetterQueue({ maxSequenceSize: 2 })
      await dlq.enqueue(GROUP, letter("seq-1"))
      await dlq.enqueue(GROUP, letter("seq-1"))

      expect(dlq.enqueue(GROUP, letter("seq-1"))).rejects.toThrow("Dead letter queue overflow")
    })
  })

  describe("clear", () => {
    it("removes all dead letters", async () => {
      const dlq = inMemoryDeadLetterQueue()
      await dlq.enqueue(GROUP, letter("seq-1"))
      await dlq.enqueue(GROUP, letter("seq-2"))

      await dlq.clear(GROUP)

      expect(await dlq.size(GROUP)).toBe(0)
      expect(await dlq.amountOfSequences(GROUP)).toBe(0)
    })
  })
})
