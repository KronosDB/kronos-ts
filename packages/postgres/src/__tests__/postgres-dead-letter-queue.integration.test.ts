/**
 * postgresDeadLetterQueue against a live postgres — the same proof set the ORM
 * families' dead-letter tests run, plus the two things specific to this family:
 * the processing GROUP travels per call (never in the constructor), and a
 * parked letter joins the unit of work's transaction.
 */
import assert from "node:assert/strict"
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import {
  DeadLetterQueueOverflowError,
  deadLetter,
  emptyMetadata,
  generateIdentifier,
  qn,
  unitOfWork,
} from "@kronos-ts/core"
import type {
  DeadLetter,
  EnqueueDecision,
  EventMessage,
  SequencedDeadLetterQueue,
  SequencedEvent,
} from "@kronos-ts/core"
import type { PostgresAdapterTransaction } from "../adapter.js"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresDeadLetterQueue } from "../postgres-dead-letter-queue.js"
import { postgresEventStore } from "../postgres-event-store.js"
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
  it("opens the lazy postgres transaction itself when the batch wrote nothing before the letter", async () => {
    // The dead-letter write can be the FIRST writer in a batch (a handler that
    // failed before touching the database). Observing the transaction instead
    // of opening it threw here.
    const uow = postgresUnitOfWork(unitOfWork, pool)()
    await uow.execute(async () => {
      await queue.enqueue(GROUP, makeLetter("lazy", "a"), uow)
    })
    expect(await queue.size(GROUP)).toBe(1)
  })

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

    await assert.rejects(
      make().execute(async (uow) => {
        await postgresTransaction(uow)
        await queue.enqueue(GROUP, makeLetter("s1", "a"), uow)
        expect(await queue.size(GROUP, uow)).toBe(1)
        throw new Error("boom")
      }),
      /boom/,
    )

    expect(await queue.size(GROUP)).toBe(0)

    await make().execute(async (uow) => {
      await postgresTransaction(uow)
      await queue.enqueue(GROUP, makeLetter("s1", "a"), uow)
    })

    expect(await queue.size(GROUP)).toBe(1)
  })
})

// ── process(): the lease and the replay transaction ──────────────────────────

type Deferred = { promise: Promise<void>; resolve: () => void }

function deferred(): Deferred {
  let resolve!: () => void
  const promise = new Promise<void>((res) => {
    resolve = res
  })
  return { promise, resolve }
}

const keep = async (): Promise<EnqueueDecision> => ({ shouldEnqueue: false })

/** The xid the transaction has been given, or null while it has written nothing. */
async function assignedXid(tx: PostgresAdapterTransaction): Promise<string | null> {
  const rows = await tx.query<{ xid: string | null }>(
    "SELECT pg_current_xact_id_if_assigned()::text AS xid",
  )
  return rows[0]!.xid
}

/** The lease on the head letter of a lane, as committed. */
async function leaseOf(seqId: string): Promise<string | null> {
  const rows = await pool.query<{ processing_started: string | null }>(
    `SELECT processing_started FROM ${pool.tables.deadLetters}
      WHERE processing_group = $1 AND sequence_identifier = $2
      ORDER BY sequence_index ASC LIMIT 1`,
    [GROUP, seqId],
  )
  return rows[0]!.processing_started
}

/** The next event a tracking stream delivers, awaited through its callback. */
function nextEvent(stream: {
  next(): SequencedEvent | undefined
  setCallback(callback: () => void): void
}): Promise<SequencedEvent> {
  return new Promise((resolve) => {
    const take = () => {
      const event = stream.next()
      if (event !== undefined) resolve(event)
    }
    stream.setCallback(take)
    take()
  })
}

describe("postgresDeadLetterQueue.process() lease", () => {
  // `pool` exists only once beforeAll has run, so the factory is built per use.
  const makeUow = () => postgresUnitOfWork(unitOfWork, pool)()

  it("gives the replay transaction no xid before the handler writes", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))

    const uow = makeUow()
    let xid: string | null | undefined
    await uow.execute(() =>
      queue.process(
        GROUP,
        () => true,
        async () => {
          xid = await assignedXid(await postgresTransaction(uow))
          return { shouldEnqueue: false }
        },
        uow,
      ),
    )

    expect(xid).toBeNull()
    expect(await queue.size(GROUP)).toBe(0)
  })

  it("lets an event appended during a replay reach a tracking stream", async () => {
    // The replay's transaction takes no xid while a handler runs, so it cannot
    // hold back the event store's gap-free tail.
    await pool.query(`TRUNCATE TABLE ${pool.tables.events} RESTART IDENTITY`)
    const store = postgresEventStore(pool)
    const stream = store.open({ position: 0n })
    await queue.enqueue(GROUP, makeLetter("s1", "a"))

    const entered = deferred()
    const hold = deferred()
    const uow = makeUow()
    const replay = uow.execute(() =>
      queue.process(
        GROUP,
        () => true,
        async () => {
          entered.resolve()
          await hold.promise
          return { shouldEnqueue: false }
        },
        uow,
      ),
    )
    try {
      await entered.promise
      await store.append([
        {
          identifier: generateIdentifier(),
          name: EVENT_NAME,
          version: "1.0",
          payload: {},
          metadata: emptyMetadata(),
          timestamp: Date.now(),
          tags: [],
        } as unknown as EventMessage,
      ])
      const event = await nextEvent(stream)
      expect(event.sequence).toBe(1n)
    } finally {
      hold.resolve()
      stream.close()
      await replay
    }
  }, 15_000)

  it("lets one replayer take a lane and sends a concurrent one away without waiting", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))

    const entered = deferred()
    const hold = deferred()
    const seen: string[] = []
    const first = makeUow()
    const firstPass = first.execute(() =>
      queue.process(
        GROUP,
        () => true,
        async (letter) => {
          seen.push(`first:${valueOf(letter)}`)
          entered.resolve()
          await hold.promise
          return { shouldEnqueue: false }
        },
        first,
      ),
    )
    try {
      await entered.promise
      // The claim is committed: visible to everyone while the replay is open.
      expect(await leaseOf("s1")).not.toBeNull()

      const second = makeUow()
      const handled = await second.execute(() =>
        queue.process(
          GROUP,
          () => true,
          async (letter) => {
            seen.push(`second:${valueOf(letter)}`)
            return { shouldEnqueue: false }
          },
          second,
        ),
      )
      expect(handled).toBe(false)
    } finally {
      hold.resolve()
    }

    expect(await firstPass).toBe(true)
    expect(seen).toEqual(["first:a"])
    expect(await queue.size(GROUP)).toBe(0)
  }, 15_000)

  it("skips a lane under a live lease and claims it once the lease is older than claimDurationMs", async () => {
    const leased = postgresDeadLetterQueue(pool, { claimDurationMs: 60_000 })
    await leased.enqueue(GROUP, makeLetter("s1", "a"))
    const setLease = (value: string) =>
      pool.query(`UPDATE ${pool.tables.deadLetters} SET processing_started = $1`, [value])
    const seen: string[] = []
    const task = async (letter: DeadLetter): Promise<EnqueueDecision> => {
      seen.push(valueOf(letter))
      return { shouldEnqueue: false }
    }

    const recent = String(Date.now() - 1_000)
    await setLease(recent)
    expect(await leased.process(GROUP, () => true, task)).toBe(false)
    expect(seen).toEqual([])
    expect(await leaseOf("s1")).toBe(recent)

    await setLease(String(Date.now() - 120_000))
    expect(await leased.process(GROUP, () => true, task)).toBe(true)
    expect(seen).toEqual(["a"])
    expect(await leased.size(GROUP)).toBe(0)
  })

  it("moves on to the next lane when the oldest one is under a live lease", async () => {
    await queue.enqueue(GROUP, makeLetter("old", "a"))
    await queue.enqueue(GROUP, makeLetter("new", "b"))
    await pool.query(
      `UPDATE ${pool.tables.deadLetters} SET processing_started = $1 WHERE sequence_identifier = 'old'`,
      [String(Date.now())],
    )

    const seen: string[] = []
    await queue.process(GROUP, () => true, async (letter) => {
      seen.push(valueOf(letter))
      return { shouldEnqueue: false }
    })

    expect(seen).toEqual(["b"])
    expect(await queue.contains(GROUP, "old")).toBe(true)
  })

  it("applies evictions only after every handler in the walk has run", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    await queue.enqueue(GROUP, makeLetter("s1", "b"))
    await queue.enqueue(GROUP, makeLetter("s1", "c"))

    const uow = makeUow()
    const xids: Array<string | null> = []
    const seen: string[] = []
    await uow.execute(() =>
      queue.process(
        GROUP,
        () => true,
        async (letter) => {
          // Letters 2 and 3 run after letter 1 succeeded; if its eviction had
          // been written already the transaction would hold an xid by now.
          xids.push(await assignedXid(await postgresTransaction(uow)))
          seen.push(valueOf(letter))
          return { shouldEnqueue: false }
        },
        uow,
      ),
    )

    expect(seen).toEqual(["a", "b", "c"])
    expect(xids).toEqual([null, null, null])
    expect(await queue.size(GROUP)).toBe(0)
  })

  it("evicts the letters that succeeded, requeues the one that failed and releases the lease", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    await queue.enqueue(GROUP, makeLetter("s1", "b"))
    await queue.enqueue(GROUP, makeLetter("s1", "c"))

    const uow = makeUow()
    await uow.execute(() =>
      queue.process(
        GROUP,
        () => true,
        async (letter) =>
          valueOf(letter) === "b"
            ? { shouldEnqueue: true, cause: new TypeError("still failing") }
            : { shouldEnqueue: false },
        uow,
      ),
    )

    const rest = await queue.deadLetterSequence(GROUP, "s1")
    expect(rest.map(valueOf)).toEqual(["b", "c"])
    expect(rest[0]!.cause.message).toBe("still failing")
    expect(await leaseOf("s1")).toBeNull()
  })

  it("releases the lease when the first letter fails", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))

    const uow = makeUow()
    await uow.execute(() =>
      queue.process(
        GROUP,
        () => true,
        async () => ({ shouldEnqueue: true, cause: new Error("nope") }),
        uow,
      ),
    )

    expect(await leaseOf("s1")).toBeNull()
    expect((await queue.deadLetterSequence(GROUP, "s1")).map(valueOf)).toEqual(["a"])
  })

  it("releases the lease when the replay's unit of work rejects, and rolls the walk back", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    await queue.enqueue(GROUP, makeLetter("s1", "b"))

    const uow = makeUow()
    await assert.rejects(
      uow.execute(() =>
        queue.process(
          GROUP,
          () => true,
          async (letter) => {
            if (valueOf(letter) === "b") throw new Error("handler broke")
            return { shouldEnqueue: false }
          },
          uow,
        ),
      ),
      /handler broke/,
    )

    expect(await queue.size(GROUP)).toBe(2)
    expect(await leaseOf("s1")).toBeNull()
    // Free to be claimed again straight away, not after claimDurationMs.
    const seen: string[] = []
    await queue.process(GROUP, () => true, async (letter) => {
      seen.push(valueOf(letter))
      return { shouldEnqueue: false }
    })
    expect(seen).toEqual(["a", "b"])
  })

  it("releases the lease when the unit of work fails after the walk, at commit", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))

    const uow = makeUow()
    uow.onPrepareCommit(() => {
      throw new Error("flush failed")
    })
    await assert.rejects(
      uow.execute(() => queue.process(GROUP, () => true, keep, uow)),
      /flush failed/,
    )

    expect(await queue.size(GROUP)).toBe(1)
    expect(await leaseOf("s1")).toBeNull()
  })

  it("leaves a lease another replayer took alone when releasing", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))

    const uow = makeUow()
    const theirs = String(Date.now() + 5_000)
    await assert.rejects(
      uow.execute(() =>
        queue.process(
          GROUP,
          () => true,
          async () => {
            // Ours expired mid-replay and another replayer claimed the lane.
            await pool.query(`UPDATE ${pool.tables.deadLetters} SET processing_started = $1`, [theirs])
            throw new Error("handler broke")
          },
          uow,
        ),
      ),
      /handler broke/,
    )

    expect(await leaseOf("s1")).toBe(theirs)
  })
})
