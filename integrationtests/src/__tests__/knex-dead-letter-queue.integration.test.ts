import { describe, expect, it, beforeAll, afterAll, beforeEach } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import Knex from "knex"
import {
  DeadLetterQueueOverflowError,
  unitOfWork,
} from "@kronos-ts/core"
import { knexDeadLetterQueue, knexUnitOfWork } from "@kronos-ts/knex"
import { DEAD_LETTER_TABLE_DDL, makeDeadLetter, valueOf } from "./shared-dead-letter-table.js"

const GROUP = "test-processor"

describe("Knex SequencedDeadLetterQueue (PostgreSQL)", () => {
  let container: StartedTestContainer
  let knex: ReturnType<typeof Knex>

  const makeQueue = (opts?: { maxSequenceSize?: number; maxSequences?: number }) =>
    knexDeadLetterQueue(knex as any, { ...opts })

  beforeAll(async () => {
    container = await new GenericContainer("postgres:16-alpine")
      .withExposedPorts(5432)
      .withEnvironment({ POSTGRES_PASSWORD: "test", POSTGRES_USER: "test", POSTGRES_DB: "test" })
      .withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2))
      .start()

    const port = container.getMappedPort(5432)
    const host = container.getHost()
    knex = Knex({ client: "pg", connection: `postgresql://test:test@${host}:${port}/test` })
    await knex.raw(DEAD_LETTER_TABLE_DDL)
  }, 120_000)

  afterAll(async () => {
    await knex?.destroy()
    await container?.stop()
  })

  beforeEach(async () => {
    await knex.raw("DELETE FROM kronos_dead_letters")
  })

  it("enqueues and reads back a sequence in insertion order", async () => {
    const dlq = makeQueue()
    await dlq.enqueue(GROUP, makeDeadLetter("A", "1"))
    await dlq.enqueue(GROUP, makeDeadLetter("A", "2"))
    await dlq.enqueue(GROUP, makeDeadLetter("B", "3"))

    expect(await dlq.size(GROUP)).toBe(3)
    expect(await dlq.amountOfSequences(GROUP)).toBe(2)
    expect((await dlq.deadLetterSequence(GROUP, "A")).map(valueOf)).toEqual(["1", "2"])
    expect(await dlq.contains(GROUP, "A")).toBe(true)
    expect(await dlq.contains(GROUP, "missing")).toBe(false)
  })

  it("enqueueIfPresent only enqueues when the sequence already exists", async () => {
    const dlq = makeQueue()
    expect(await dlq.enqueueIfPresent(GROUP, "A", () => makeDeadLetter("A", "x"))).toBe(false)
    await dlq.enqueue(GROUP, makeDeadLetter("A", "1"))
    expect(await dlq.enqueueIfPresent(GROUP, "A", () => makeDeadLetter("A", "2"))).toBe(true)
    expect((await dlq.deadLetterSequence(GROUP, "A")).map(valueOf)).toEqual(["1", "2"])
  })

  it("evicts a specific letter via the round-tripped identity", async () => {
    const dlq = makeQueue()
    await dlq.enqueue(GROUP, makeDeadLetter("A", "1"))
    await dlq.enqueue(GROUP, makeDeadLetter("A", "2"))
    const [first] = await dlq.deadLetterSequence(GROUP, "A")
    await dlq.evict(GROUP, "A", first!)
    expect((await dlq.deadLetterSequence(GROUP, "A")).map(valueOf)).toEqual(["2"])
  })

  it("process() drains a sequence when the task evicts each letter", async () => {
    const dlq = makeQueue()
    await dlq.enqueue(GROUP, makeDeadLetter("A", "1"))
    await dlq.enqueue(GROUP, makeDeadLetter("A", "2"))

    const seen: string[] = []
    const processed = await dlq.process(
      GROUP,
      () => true,
      async (letter) => {
        seen.push(valueOf(letter))
        return { shouldEnqueue: false }
      },
    )

    expect(processed).toBe(true)
    expect(seen).toEqual(["1", "2"])
    expect(await dlq.size(GROUP)).toBe(0)
  })

  it("process() requeues and stops at the first letter the task keeps", async () => {
    const dlq = makeQueue()
    await dlq.enqueue(GROUP, makeDeadLetter("A", "1"))
    await dlq.enqueue(GROUP, makeDeadLetter("A", "2"))

    const processed = await dlq.process(
      GROUP,
      () => true,
      async () => ({ shouldEnqueue: true, cause: new Error("still failing") }),
    )

    expect(processed).toBe(true)
    expect(await dlq.size(GROUP)).toBe(2)
    expect((await dlq.deadLetterSequence(GROUP, "A"))[0]!.cause.message).toBe("still failing")
  })

  it("throws DeadLetterQueueOverflowError when a sequence is full (backpressure)", async () => {
    const dlq = makeQueue({ maxSequenceSize: 1 })
    await dlq.enqueue(GROUP, makeDeadLetter("A", "1"))
    expect(dlq.enqueue(GROUP, makeDeadLetter("A", "2"))).rejects.toBeInstanceOf(DeadLetterQueueOverflowError)
    expect(await dlq.isFull(GROUP, "A")).toBe(true)
  })

  it("commits the enqueue in the active UnitOfWork transaction — and rolls it back on failure", async () => {
    const dlq = makeQueue()
    const runUoW = knexUnitOfWork(unitOfWork, knex as any)

    await runUoW().execute(async (uow) => {
      await dlq.enqueue(GROUP, makeDeadLetter("A", "committed"), uow)
    })
    expect(await dlq.size(GROUP)).toBe(1)

    await expect(
      runUoW().execute(async (uow) => {
        await dlq.enqueue(GROUP, makeDeadLetter("A", "rolled-back"), uow)
        throw new Error("boom — force rollback")
      }),
    ).rejects.toThrow("boom — force rollback")

    expect(await dlq.size(GROUP)).toBe(1)
    expect((await dlq.deadLetterSequence(GROUP, "A")).map(valueOf)).toEqual(["committed"])
  })
})
