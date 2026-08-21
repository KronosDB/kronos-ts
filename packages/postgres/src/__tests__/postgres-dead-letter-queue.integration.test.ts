/**
 * postgresDeadLetterQueue against a live postgres — the same proof set the ORM
 * families' dead-letter tests run, plus the two things specific to this family:
 * the processing GROUP travels per call (never in the constructor), and a
 * parked letter joins the unit of work's transaction.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import {
  DeadLetterQueueOverflowError,
  deadLetter,
  emptyMetadata,
  qn,
  unitOfWork,
} from "@kronos-ts/core"
import type { DeadLetter, SequencedDeadLetterQueue } from "@kronos-ts/core"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresDeadLetterQueue } from "../postgres-dead-letter-queue.js"
import { postgresTransaction, postgresUnitOfWork } from "../postgres-transaction.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"

const EVENT_NAME = qn("pg-dlq", "SomethingHappened")
const GROUP = "orders"

function makeLetter(seqId: string, value: string, cause = new Error("boom")): DeadLetter {
  return deadLetter(
    {
      kind: "event",
      identifier: `evt-${seqId}-${value}`,
      name: EVENT_NAME,
      version: "1.0",
      payload: { value },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [{ key: "id", value: seqId }],
    },
    cause,
    seqId,
    { position: 0 },
  )
}

const valueOf = (letter: DeadLetter): string => (letter.message.payload as { value: string }).value

let pg: RunningPostgres
let pool: PostgresResource
let queue: SequencedDeadLetterQueue

beforeAll(async () => {
  pg = await startPostgresContainer()
  pool = postgresPool(pg.connectionString)
  await pool.start()
  queue = postgresDeadLetterQueue(pool)
}, 60_000)

afterAll(async () => {
  await pool.close()
  await pg.stop()
}, 30_000)

beforeEach(async () => {
  await pool.query(`TRUNCATE TABLE ${pool.tables.deadLetters}`)
})

describe("postgresDeadLetterQueue", () => {
  it("enqueues and reads back a sequence in insertion order", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    await queue.enqueue(GROUP, makeLetter("s1", "b"))

    const letters = await queue.deadLetterSequence(GROUP, "s1")
    expect(letters.map(valueOf)).toEqual(["a", "b"])
    expect(await queue.size(GROUP)).toBe(2)
    expect(await queue.amountOfSequences(GROUP)).toBe(1)
    expect(await queue.contains(GROUP, "s1")).toBe(true)
    expect(await queue.contains(GROUP, "nope")).toBe(false)
  })

  it("carries the processing GROUP per call — one table, many partitions", async () => {
    // The group is not a constructor parameter: which partition a call touches
    // is a property of the caller, exactly as processorName is on a token store.
    await queue.enqueue("orders", makeLetter("s1", "o"))
    await queue.enqueue("shipping", makeLetter("s1", "s"))

    expect((await queue.deadLetterSequence("orders", "s1")).map(valueOf)).toEqual(["o"])
    expect((await queue.deadLetterSequence("shipping", "s1")).map(valueOf)).toEqual(["s"])
    expect(await queue.sequenceIdentifiers("orders")).toEqual(["s1"])
    await queue.clear("orders")
    expect(await queue.size("orders")).toBe(0)
    expect(await queue.size("shipping")).toBe(1)
  })

  it("enqueueIfPresent only enqueues when the sequence already exists", async () => {
    expect(await queue.enqueueIfPresent(GROUP, "s1", () => makeLetter("s1", "a"))).toBe(false)
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    expect(await queue.enqueueIfPresent(GROUP, "s1", () => makeLetter("s1", "b"))).toBe(true)
    expect((await queue.deadLetterSequence(GROUP, "s1")).map(valueOf)).toEqual(["a", "b"])
  })

  it("evicts a specific letter via the round-tripped identity", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    await queue.enqueue(GROUP, makeLetter("s1", "b"))
    const [first] = await queue.deadLetterSequence(GROUP, "s1")

    await queue.evict(GROUP, "s1", first!)

    expect((await queue.deadLetterSequence(GROUP, "s1")).map(valueOf)).toEqual(["b"])
  })

  it("requeue updates the cause", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    const [letter] = await queue.deadLetterSequence(GROUP, "s1")

    await queue.requeue(GROUP, letter!, { cause: new TypeError("second failure") })

    const [updated] = await queue.deadLetterSequence(GROUP, "s1")
    expect(updated!.cause.name).toBe("TypeError")
    expect(updated!.cause.message).toBe("second failure")
  })

  it("process() drains a sequence when the task evicts each letter", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    await queue.enqueue(GROUP, makeLetter("s1", "b"))

    const seen: string[] = []
    const handled = await queue.process(
      GROUP,
      () => true,
      async (letter) => {
        seen.push(valueOf(letter))
        return { shouldEnqueue: false }
      },
    )

    expect(handled).toBe(true)
    expect(seen).toEqual(["a", "b"])
    expect(await queue.size(GROUP)).toBe(0)
  })

  it("process() requeues and stops at the first letter the task keeps", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    await queue.enqueue(GROUP, makeLetter("s1", "b"))

    const seen: string[] = []
    await queue.process(
      GROUP,
      () => true,
      async (letter) => {
        seen.push(valueOf(letter))
        return { shouldEnqueue: true, cause: new Error("still failing") }
      },
    )

    // FIFO: the head is retried, the tail is untouched.
    expect(seen).toEqual(["a"])
    expect((await queue.deadLetterSequence(GROUP, "s1")).map(valueOf)).toEqual(["a", "b"])
  })

  it("throws DeadLetterQueueOverflowError when a sequence is full (backpressure)", async () => {
    const small = postgresDeadLetterQueue(pool, { maxSequenceSize: 1 })
    await small.enqueue(GROUP, makeLetter("s1", "a"))
    expect(await small.isFull(GROUP, "s1")).toBe(true)
    await expect(small.enqueue(GROUP, makeLetter("s1", "b"))).rejects.toBeInstanceOf(
      DeadLetterQueueOverflowError,
    )
  })

  it("throws DeadLetterQueueOverflowError when the group is out of sequences", async () => {
    const small = postgresDeadLetterQueue(pool, { maxSequences: 1 })
    await small.enqueue(GROUP, makeLetter("s1", "a"))
    await expect(small.enqueue(GROUP, makeLetter("s2", "a"))).rejects.toBeInstanceOf(
      DeadLetterQueueOverflowError,
    )
  })

  it("commits the enqueue in the active unit of work's transaction — and rolls it back on failure", async () => {
    // Same premise as the token store: a parked letter and the token that
    // skipped past it are one transaction, or neither happened.
    const make = postgresUnitOfWork(unitOfWork, pool)

    await expect(
      make().execute(async (uow) => {
        await postgresTransaction(uow)
        await queue.enqueue(GROUP, makeLetter("s1", "a"), uow)
        expect(await queue.size(GROUP, uow)).toBe(1)
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    expect(await queue.size(GROUP)).toBe(0)

    await make().execute(async (uow) => {
      await postgresTransaction(uow)
      await queue.enqueue(GROUP, makeLetter("s1", "a"), uow)
    })

    expect(await queue.size(GROUP)).toBe(1)
  })
})
