import { afterEach, describe, expect, it, setSystemTime } from "bun:test"
import { unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { qn, type EventMessage } from "../../messaging/messages.js"
import { deadLetterBackoff, exponentialBackoff } from "../dead-letter-backoff.js"
import {
  deadLetter,
  inMemoryDeadLetterQueue,
  type DeadLetter,
  type EnqueueDecision,
} from "../dead-letter-queue.js"

const GROUP = "test-processor"
const T0 = Date.UTC(2026, 0, 1)

function testEvent(name: string): EventMessage {
  return {
    kind: "event",
    identifier: `id-${name}`,
    name: qn("test", name),
    version: "1.0",
    payload: {},
    metadata: {},
    timestamp: Date.now(),
    tags: [],
  }
}

function letter(seqId: string, name = "TestEvent", cause = new Error("test failure")): DeadLetter {
  return deadLetter(testEvent(name), cause, seqId, { position: 1 })
}

const succeed = async (): Promise<EnqueueDecision> => ({ shouldEnqueue: false })
const fail = async (): Promise<EnqueueDecision> => ({
  shouldEnqueue: true,
  cause: new Error("still failing"),
})

type Backoff = { attempts: number; retryAt: number | "hold" | null }
const backoffOf = (l: DeadLetter | undefined) => l?.diagnostics["backoff"] as Backoff | undefined

afterEach(() => setSystemTime())

describe("deadLetterBackoff", () => {
  it("stamps attempts and the first retry time on enqueue, without mutating the letter", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 5_000)
    const original = letter("s1")

    await queue.enqueue(GROUP, original)

    expect(backoffOf((await queue.deadLetterSequence(GROUP, "s1"))[0])).toEqual({
      attempts: 1,
      retryAt: T0 + 5_000,
    })
    // The caller's letter is untouched, and its other diagnostics survive.
    expect(original.diagnostics["backoff"]).toBeUndefined()
    expect((await queue.deadLetterSequence(GROUP, "s1"))[0]!.diagnostics["position"]).toBe(1)
  })

  it("tells the policy the attempt number and the failure's own cause", async () => {
    const seen: Array<{ attempts: number; cause: string }> = []
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), ({ attempts, letter }) => {
      seen.push({ attempts, cause: letter.cause.message })
      return 0
    })
    await queue.enqueue(GROUP, letter("s1", "E", new Error("first")))
    await queue.process(GROUP, () => true, async () => ({
      shouldEnqueue: true,
      cause: new Error("second"),
    }), unitOfWork)

    expect(seen).toEqual([
      { attempts: 1, cause: "first" },
      { attempts: 2, cause: "second" },
    ])
  })

  it("passes each letter's unit of work through, and its diagnostics to the requeue", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 5_000)
    await queue.enqueue(GROUP, letter("s1", "A"))
    await queue.enqueueIfPresent(GROUP, "s1", () => letter("s1", "B"))
    const minted: UnitOfWork[] = []
    const handed: UnitOfWork[] = []

    setSystemTime(new Date(T0 + 5_000))
    await queue.process(
      GROUP,
      () => true,
      async (l, uow) => {
        handed.push(uow)
        return l.message.identifier === "id-A"
          ? { shouldEnqueue: false }
          : { shouldEnqueue: true, diagnostics: { note: "kept" } }
      },
      () => {
        const uow = unitOfWork()
        minted.push(uow)
        return uow
      },
    )

    expect(handed).toEqual(minted)
    expect(minted.length).toBe(2)
    const [b] = await queue.deadLetterSequence(GROUP, "s1")
    expect(b!.diagnostics["note"]).toBe("kept")
    // B was never attempted before, so this is its first failure.
    expect(backoffOf(b)).toEqual({ attempts: 1, retryAt: T0 + 5_000 + 5_000 })
  })

  it("skips a lane whose head is not due", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 5_000)
    await queue.enqueue(GROUP, letter("s1"))
    let calls = 0

    const processed = await queue.process(GROUP, () => true, async () => {
      calls++
      return { shouldEnqueue: false }
    }, unitOfWork)

    expect(processed).toBe(false)
    expect(calls).toBe(0)
    expect(await queue.contains(GROUP, "s1")).toBe(true)
  })

  it("passes the head letter to the caller's filter, and applies both filters", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 0)
    await queue.enqueue(GROUP, letter("s1", "First"))
    await queue.enqueue(GROUP, letter("s2", "Second"))
    const heads: string[] = []

    const processed = await queue.process(
      GROUP,
      (id, head) => {
        heads.push(`${id}:${head.message.identifier}`)
        return id === "s2"
      },
      succeed,
      unitOfWork,
    )

    expect(processed).toBe(true)
    expect(heads).toContain("s1:id-First")
    expect(await queue.contains(GROUP, "s1")).toBe(true)
    expect(await queue.contains(GROUP, "s2")).toBe(false)
  })

  it("picks a lane up once it is due", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 5_000)
    await queue.enqueue(GROUP, letter("s1"))

    setSystemTime(new Date(T0 + 4_999))
    expect(await queue.process(GROUP, () => true, succeed, unitOfWork)).toBe(false)

    setSystemTime(new Date(T0 + 5_000))
    expect(await queue.process(GROUP, () => true, succeed, unitOfWork)).toBe(true)
    expect(await queue.contains(GROUP, "s1")).toBe(false)
  })

  it("increments attempts through a failing process and pushes the retry time out", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), ({ attempts }) => attempts * 1_000)
    await queue.enqueue(GROUP, letter("s1"))

    setSystemTime(new Date(T0 + 1_000))
    expect(await queue.process(GROUP, () => true, fail, unitOfWork)).toBe(true)
    expect(backoffOf((await queue.deadLetterSequence(GROUP, "s1"))[0])).toEqual({
      attempts: 2,
      retryAt: T0 + 1_000 + 2_000,
    })

    setSystemTime(new Date(T0 + 3_000))
    expect(await queue.process(GROUP, () => true, fail, unitOfWork)).toBe(true)
    expect(backoffOf((await queue.deadLetterSequence(GROUP, "s1"))[0])).toEqual({
      attempts: 3,
      retryAt: T0 + 3_000 + 3_000,
    })
    // Still parked, never evicted, whatever the count.
    expect(await queue.size(GROUP)).toBe(1)
  })

  it("keeps the failure's new cause when it stamps the next attempt", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 0)
    await queue.enqueue(GROUP, letter("s1"))

    await queue.process(GROUP, () => true, async () => ({
      shouldEnqueue: true,
      cause: new TypeError("different"),
      diagnostics: { note: "kept" },
    }), unitOfWork)

    const [head] = await queue.deadLetterSequence(GROUP, "s1")
    expect(head!.cause.name).toBe("TypeError")
    expect(head!.diagnostics["note"]).toBe("kept")
    expect(backoffOf(head)?.attempts).toBe(2)
  })

  it("evicts on success, and the next letter in the lane starts with no backoff", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 1_000)
    await queue.enqueue(GROUP, letter("s1", "A"))
    await queue.enqueueIfPresent(GROUP, "s1", () => letter("s1", "B"))
    // Fail A once so it carries attempts 2.
    setSystemTime(new Date(T0 + 1_000))
    await queue.process(GROUP, () => true, fail, unitOfWork)
    expect(backoffOf((await queue.deadLetterSequence(GROUP, "s1"))[0])?.attempts).toBe(2)

    // A succeeds and is evicted. B is replayed in the same walk and fails: its
    // count starts from nothing, not from A's.
    setSystemTime(new Date(T0 + 10_000))
    let calls = 0
    await queue.process(GROUP, () => true, async (l) => {
      calls++
      return l.message.identifier === "id-A" ? { shouldEnqueue: false } : fail()
    }, unitOfWork)

    expect(calls).toBe(2)
    const lane = await queue.deadLetterSequence(GROUP, "s1")
    expect(lane.map((l) => l.message.identifier)).toEqual(["id-B"])
    expect(backoffOf(lane[0])).toEqual({ attempts: 1, retryAt: T0 + 10_000 + 1_000 })
  })

  it("leaves a letter that follows an evicted head with no state until it fails", async () => {
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 60_000)
    await queue.enqueue(GROUP, letter("s1", "A"))
    await queue.enqueueIfPresent(GROUP, "s1", () => letter("s1", "B"))
    // Evict the head directly: B becomes the head, never attempted.
    const [a] = await queue.deadLetterSequence(GROUP, "s1")
    await queue.evict(GROUP, "s1", a!)

    const [b] = await queue.deadLetterSequence(GROUP, "s1")
    expect(backoffOf(b)).toBeUndefined()
    expect(await queue.process(GROUP, () => true, succeed, unitOfWork)).toBe(true)
  })

  it('"hold" is never due until retryNow', async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => "hold")
    await queue.enqueue(GROUP, letter("s1"))
    expect(backoffOf((await queue.deadLetterSequence(GROUP, "s1"))[0])).toEqual({
      attempts: 1,
      retryAt: "hold",
    })

    setSystemTime(new Date(T0 + 10 * 365 * 24 * 3_600_000))
    expect(await queue.process(GROUP, () => true, succeed, unitOfWork)).toBe(false)

    expect(await queue.retryNow(GROUP, "s1")).toBe(true)
    expect(await queue.process(GROUP, () => true, succeed, unitOfWork)).toBe(true)
    expect(await queue.contains(GROUP, "s1")).toBe(false)
  })

  it('a replay that fails again under a "hold" policy goes back on hold', async () => {
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => "hold")
    await queue.enqueue(GROUP, letter("s1"))
    await queue.retryNow(GROUP, "s1")

    expect(await queue.process(GROUP, () => true, fail, unitOfWork)).toBe(true)

    expect(backoffOf((await queue.deadLetterSequence(GROUP, "s1"))[0])).toEqual({
      attempts: 2,
      retryAt: "hold",
    })
    expect(await queue.process(GROUP, () => true, succeed, unitOfWork)).toBe(false)
  })

  it("retryNow keeps attempts, makes the head due, and reports an empty lane", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 60_000)
    await queue.enqueue(GROUP, letter("s1"))
    setSystemTime(new Date(T0 + 60_000))
    await queue.process(GROUP, () => true, fail, unitOfWork)
    expect(backoffOf((await queue.deadLetterSequence(GROUP, "s1"))[0])?.attempts).toBe(2)

    expect(await queue.retryNow(GROUP, "s1")).toBe(true)

    expect(backoffOf((await queue.deadLetterSequence(GROUP, "s1"))[0])).toEqual({
      attempts: 2,
      retryAt: null,
    })
    expect(await queue.process(GROUP, () => true, succeed, unitOfWork)).toBe(true)
    expect(await queue.retryNow(GROUP, "s1")).toBe(false)
    expect(await queue.retryNow(GROUP, "never-parked")).toBe(false)
  })

  it("retryNow moves the lane to the back of the oldest-first order", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 0)
    await queue.enqueue(GROUP, letter("older"))
    setSystemTime(new Date(T0 + 1))
    await queue.enqueue(GROUP, letter("newer"))

    setSystemTime(new Date(T0 + 2))
    await queue.retryNow(GROUP, "older")

    await queue.process(GROUP, () => true, succeed, unitOfWork)
    expect(await queue.contains(GROUP, "newer")).toBe(false)
    expect(await queue.contains(GROUP, "older")).toBe(true)
  })

  it("enqueueIfPresent is passed through untouched", async () => {
    setSystemTime(new Date(T0))
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue(), () => 5_000)
    await queue.enqueue(GROUP, letter("s1", "A"))

    const parked = await queue.enqueueIfPresent(GROUP, "s1", () => letter("s1", "B"))

    expect(parked).toBe(true)
    const lane = await queue.deadLetterSequence(GROUP, "s1")
    expect(lane).toHaveLength(2)
    expect(backoffOf(lane[1])).toBeUndefined()
    expect(await queue.enqueueIfPresent(GROUP, "absent", () => letter("absent"))).toBe(false)
  })

  it("treats a missing or null backoff as due", async () => {
    const inner = inMemoryDeadLetterQueue()
    const queue = deadLetterBackoff(inner, () => 60_000)
    await inner.enqueue(GROUP, letter("bare"))
    await inner.enqueue(
      GROUP,
      deadLetter(testEvent("N"), new Error("x"), "nulled", {
        backoff: { attempts: 3, retryAt: null },
      }),
    )

    expect(await queue.process(GROUP, () => true, succeed, unitOfWork)).toBe(true)
    expect(await queue.process(GROUP, () => true, succeed, unitOfWork)).toBe(true)
    expect(await queue.size(GROUP)).toBe(0)
  })

  it("passes every other method through", async () => {
    const queue = deadLetterBackoff(inMemoryDeadLetterQueue({ maxSequenceSize: 1 }), () => 0)
    await queue.enqueue(GROUP, letter("s1"))

    expect(await queue.contains(GROUP, "s1")).toBe(true)
    expect(await queue.sequenceIdentifiers(GROUP)).toEqual(["s1"])
    expect(await queue.size(GROUP)).toBe(1)
    expect(await queue.amountOfSequences(GROUP)).toBe(1)
    expect(await queue.isFull(GROUP, "s1")).toBe(true)
    await queue.clear(GROUP)
    expect(await queue.size(GROUP)).toBe(0)
  })
})

describe("exponentialBackoff", () => {
  const failure = (attempts: number) => ({ attempts, letter: letter("s1") })
  /** The policy's delay at the extremes of its jitter. */
  const at = (random: number, policy: ReturnType<typeof exponentialBackoff>, attempts: number) => {
    const real = Math.random
    Math.random = () => random
    try {
      return policy(failure(attempts)) as number
    } finally {
      Math.random = real
    }
  }

  it("grows by the multiplier from the initial delay", () => {
    const policy = exponentialBackoff({ initialDelayMs: 1_000, multiplier: 3 })
    // Math.random() = 0.5 is the centre of the jitter: exactly the base delay.
    expect(at(0.5, policy, 1)).toBe(1_000)
    expect(at(0.5, policy, 2)).toBe(3_000)
    expect(at(0.5, policy, 3)).toBe(9_000)
  })

  it("defaults to 10s initial, doubling, capped at an hour", () => {
    const policy = exponentialBackoff()
    expect(at(0.5, policy, 1)).toBe(10_000)
    expect(at(0.5, policy, 2)).toBe(20_000)
    expect(at(0.5, policy, 3)).toBe(40_000)
    expect(at(0.5, policy, 20)).toBe(3_600_000)
  })

  it("caps the delay before jitter", () => {
    const policy = exponentialBackoff({ initialDelayMs: 1_000, maxDelayMs: 5_000 })
    expect(at(0.5, policy, 3)).toBe(4_000)
    expect(at(0.5, policy, 4)).toBe(5_000)
    expect(at(0.5, policy, 50)).toBe(5_000)
  })

  it("spreads each delay by 25% either side", () => {
    const policy = exponentialBackoff({ initialDelayMs: 1_000, maxDelayMs: 5_000 })
    expect(at(0, policy, 1)).toBe(750)
    expect(at(0.999999, policy, 1)).toBeCloseTo(1_250, 1)
    expect(at(0, policy, 10)).toBe(3_750)
    expect(at(0.999999, policy, 10)).toBeCloseTo(6_250, 1)

    for (let i = 0; i < 200; i++) {
      const delay = policy(failure(2)) as number
      expect(delay).toBeGreaterThanOrEqual(1_500)
      expect(delay).toBeLessThanOrEqual(2_500)
    }
  })
})
