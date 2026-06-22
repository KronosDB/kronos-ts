import { describe, expect, it, beforeAll, afterAll, beforeEach } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import Knex from "knex"
import { emptyMetadata } from "@kronos-ts/common"
import {
  Decisions,
  DeadLetterQueueOverflowError,
  transactionalUnitOfWorkFactory,
  runInNewUoW,
} from "@kronos-ts/messaging"
import { knexDeadLetterQueue, knexTransactionManager } from "@kronos-ts/knex"
import { DEAD_LETTER_TABLE_DDL, makeDeadLetter, valueOf } from "./shared-dead-letter-table.js"

const GROUP = "test-processor"

describe("Knex SequencedDeadLetterQueue (PostgreSQL)", () => {
  let container: StartedTestContainer
  let knex: ReturnType<typeof Knex>

  const makeQueue = (opts?: { maxSequenceSize?: number; maxSequences?: number }) =>
    knexDeadLetterQueue(knex as any, { processingGroup: GROUP, ...opts })

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
    await dlq.enqueue(makeDeadLetter("A", "1"))
    await dlq.enqueue(makeDeadLetter("A", "2"))
    await dlq.enqueue(makeDeadLetter("B", "3"))

    expect(await dlq.size()).toBe(3)
    expect(await dlq.amountOfSequences()).toBe(2)
    expect((await dlq.deadLetterSequence("A")).map(valueOf)).toEqual(["1", "2"])
    expect(await dlq.contains("A")).toBe(true)
    expect(await dlq.contains("missing")).toBe(false)
  })

  it("enqueueIfPresent only enqueues when the sequence already exists", async () => {
    const dlq = makeQueue()
    expect(await dlq.enqueueIfPresent("A", () => makeDeadLetter("A", "x"))).toBe(false)
    await dlq.enqueue(makeDeadLetter("A", "1"))
    expect(await dlq.enqueueIfPresent("A", () => makeDeadLetter("A", "2"))).toBe(true)
    expect((await dlq.deadLetterSequence("A")).map(valueOf)).toEqual(["1", "2"])
  })

  it("evicts a specific letter via the round-tripped identity", async () => {
    const dlq = makeQueue()
    await dlq.enqueue(makeDeadLetter("A", "1"))
    await dlq.enqueue(makeDeadLetter("A", "2"))
    const [first] = await dlq.deadLetterSequence("A")
    await dlq.evict("A", first!)
    expect((await dlq.deadLetterSequence("A")).map(valueOf)).toEqual(["2"])
  })

  it("process() drains a sequence when the task evicts each letter", async () => {
    const dlq = makeQueue()
    await dlq.enqueue(makeDeadLetter("A", "1"))
    await dlq.enqueue(makeDeadLetter("A", "2"))

    const seen: string[] = []
    const processed = await dlq.process(
      () => true,
      async (letter) => {
        seen.push(valueOf(letter))
        return Decisions.evict()
      },
    )

    expect(processed).toBe(true)
    expect(seen).toEqual(["1", "2"])
    expect(await dlq.size()).toBe(0)
  })

  it("process() requeues and stops at the first letter the task keeps", async () => {
    const dlq = makeQueue()
    await dlq.enqueue(makeDeadLetter("A", "1"))
    await dlq.enqueue(makeDeadLetter("A", "2"))

    const processed = await dlq.process(
      () => true,
      async () => Decisions.requeue(new Error("still failing")),
    )

    expect(processed).toBe(true)
    expect(await dlq.size()).toBe(2)
    expect((await dlq.deadLetterSequence("A"))[0]!.cause.message).toBe("still failing")
  })

  it("throws DeadLetterQueueOverflowError when a sequence is full (backpressure)", async () => {
    const dlq = makeQueue({ maxSequenceSize: 1 })
    await dlq.enqueue(makeDeadLetter("A", "1"))
    expect(dlq.enqueue(makeDeadLetter("A", "2"))).rejects.toBeInstanceOf(DeadLetterQueueOverflowError)
    expect(await dlq.isFull("A")).toBe(true)
  })

  it("commits the enqueue in the active UnitOfWork transaction — and rolls it back on failure", async () => {
    const dlq = makeQueue()
    const runUoW = transactionalUnitOfWorkFactory(runInNewUoW, knexTransactionManager(knex as any))

    await runUoW(emptyMetadata(), async () => {
      await dlq.enqueue(makeDeadLetter("A", "committed"))
    })
    expect(await dlq.size()).toBe(1)

    await expect(
      runUoW(emptyMetadata(), async () => {
        await dlq.enqueue(makeDeadLetter("A", "rolled-back"))
        throw new Error("boom — force rollback")
      }),
    ).rejects.toThrow("boom — force rollback")

    expect(await dlq.size()).toBe(1)
    expect((await dlq.deadLetterSequence("A")).map(valueOf)).toEqual(["committed"])
  })
})
