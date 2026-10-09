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
  deadLetterBackoff,
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

describe("postgresDeadLetterQueue.process() filter", () => {
  const makeUow = () => postgresUnitOfWork(unitOfWork, pool)()

  /**
   * A pool whose first claim statement is preceded by `beforeClaim` — the moment
   * between a replayer's candidate selection and its claim, when another
   * replayer can change a lane.
   */
  function poolWithRaceBeforeClaim(beforeClaim: () => Promise<unknown>): PostgresResource {
    let raced = false
    return new Proxy(pool, {
      get(target, prop) {
        if (prop === "query") {
          return async (sql: string, params?: unknown[]) => {
            if (!raced && sql.includes("SKIP LOCKED")) {
              raced = true
              await beforeClaim()
            }
            return target.query(sql, params)
          }
        }
        const value = Reflect.get(target, prop, target)
        return typeof value === "function" ? value.bind(target) : value
      },
    })
  }

  /** Overwrite a lane's head diagnostics, as another replayer's requeue would. */
  const setHeadDiagnostics = (seqId: string, diagnostics: Record<string, unknown>) =>
    pool.query(
      `UPDATE ${pool.tables.deadLetters} SET diagnostics = $3
        WHERE processing_group = $1 AND sequence_identifier = $2 AND sequence_index = 0`,
      [GROUP, seqId, JSON.stringify(diagnostics)],
    )

  it("hands the filter each lane's head letter, with its stored diagnostics and identity", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    await queue.enqueue(GROUP, makeLetter("s1", "b"))
    await queue.enqueue(GROUP, makeLetter("s2", "c", new TypeError("typed")))

    const seen = new Map<string, DeadLetter>()
    await queue.process(
      GROUP,
      (id, head) => {
        seen.set(id, head)
        return false
      },
      keep,
    )

    expect([...seen.keys()].sort()).toEqual(["s1", "s2"])
    const head = seen.get("s1")!
    expect(valueOf(head)).toBe("a")
    expect(head.sequenceIdentifier).toBe("s1")
    expect(head.diagnostics["position"]).toBe(0)
    expect(typeof head.diagnostics["__dlqId"]).toBe("string")
    expect(seen.get("s2")!.cause.name).toBe("TypeError")
    // Looking is not claiming.
    expect(await leaseOf("s1")).toBeNull()
  })

  it("re-checks the filter on the head as it is after the claim, and takes the next lane", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))
    await new Promise((resolve) => setTimeout(resolve, 5))
    await queue.enqueue(GROUP, makeLetter("s2", "b"))

    // Between selection and claim another replayer requeues s1's head, and the
    // requeue makes it not eligible.
    const racing = postgresDeadLetterQueue(
      poolWithRaceBeforeClaim(() => setHeadDiagnostics("s1", { position: 0, skip: true })),
    )
    const checks: Array<[string, boolean]> = []
    const seen: string[] = []
    const handled = await racing.process(
      GROUP,
      (id, head) => {
        const eligible = head.diagnostics["skip"] !== true
        checks.push([id, eligible])
        return eligible
      },
      async (letter) => {
        seen.push(valueOf(letter))
        return { shouldEnqueue: false }
      },
    )

    expect(handled).toBe(true)
    expect(seen).toEqual(["b"])
    // Selection saw s1 as eligible; the check after the claim saw it was not.
    expect(checks).toContainEqual(["s1", true])
    expect(checks).toContainEqual(["s1", false])
    // s1's lease went back, and its letter is still parked.
    expect(await leaseOf("s1")).toBeNull()
    expect(await queue.contains(GROUP, "s1")).toBe(true)
    expect(await queue.contains(GROUP, "s2")).toBe(false)
  })

  it("returns false, with the lease released, when no lane passes the re-check", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))

    const racing = postgresDeadLetterQueue(
      poolWithRaceBeforeClaim(() => setHeadDiagnostics("s1", { position: 0, skip: true })),
    )
    let called = 0
    const handled = await racing.process(
      GROUP,
      (_id, head) => head.diagnostics["skip"] !== true,
      async () => {
        called++
        return { shouldEnqueue: false }
      },
    )

    expect(handled).toBe(false)
    expect(called).toBe(0)
    expect(await leaseOf("s1")).toBeNull()
    expect(await queue.size(GROUP)).toBe(1)
  })

  it("releases the claim when the filter throws on the re-check", async () => {
    await queue.enqueue(GROUP, makeLetter("s1", "a"))

    let calls = 0
    await assert.rejects(
      queue.process(
        GROUP,
        () => {
          if (++calls === 2) throw new Error("filter broke")
          return true
        },
        keep,
      ),
      /filter broke/,
    )

    expect(await leaseOf("s1")).toBeNull()
  })

  describe("under deadLetterBackoff", () => {
    it("round-trips backoff through a failing process, then skips the lane until retryNow", async () => {
      const backoff = deadLetterBackoff(queue, () => 60_000)
      const before = Date.now()
      await backoff.enqueue(GROUP, makeLetter("s1", "a"))

      const parked = (await backoff.deadLetterSequence(GROUP, "s1"))[0]!
      const stamped = parked.diagnostics["backoff"] as { attempts: number; retryAt: number }
      expect(stamped.attempts).toBe(1)
      expect(stamped.retryAt).toBeGreaterThanOrEqual(before + 60_000)
      // The letter's own diagnostics survive the stamp.
      expect(parked.diagnostics["position"]).toBe(0)

      // Not due: skipped without a lease or a handler call.
      let calls = 0
      const task = async (): Promise<EnqueueDecision> => {
        calls++
        return { shouldEnqueue: true, cause: new TypeError("still failing") }
      }
      expect(await backoff.process(GROUP, () => true, task)).toBe(false)
      expect(calls).toBe(0)
      expect(await leaseOf("s1")).toBeNull()

      // retryNow makes it due and keeps the count; a failing replay in the
      // replay's unit of work then stamps attempt 2 and pushes it out again.
      expect(await backoff.retryNow(GROUP, "s1")).toBe(true)
      expect((await backoff.deadLetterSequence(GROUP, "s1"))[0]!.diagnostics["backoff"]).toEqual({
        attempts: 1,
        retryAt: null,
      })
      const uow = makeUow()
      expect(await uow.execute(() => backoff.process(GROUP, () => true, task, uow))).toBe(true)
      expect(calls).toBe(1)

      const failed = (await backoff.deadLetterSequence(GROUP, "s1"))[0]!
      const next = failed.diagnostics["backoff"] as { attempts: number; retryAt: number }
      expect(next.attempts).toBe(2)
      expect(next.retryAt).toBeGreaterThanOrEqual(before + 60_000)
      expect(failed.cause.name).toBe("TypeError")
      expect(await leaseOf("s1")).toBeNull()

      // And the lane is skipped again.
      expect(await backoff.process(GROUP, () => true, task)).toBe(false)
      expect(calls).toBe(1)
    })

    it('round-trips "hold" and lets a due lane be replayed ahead of a held one', async () => {
      const policy = (failure: { letter: DeadLetter }) =>
        failure.letter.sequenceIdentifier === "held" ? ("hold" as const) : 0
      const backoff = deadLetterBackoff(queue, policy)
      await backoff.enqueue(GROUP, makeLetter("held", "a"))
      await new Promise((resolve) => setTimeout(resolve, 5))
      await backoff.enqueue(GROUP, makeLetter("due", "b"))

      expect((await backoff.deadLetterSequence(GROUP, "held"))[0]!.diagnostics["backoff"]).toEqual({
        attempts: 1,
        retryAt: "hold",
      })

      const seen: string[] = []
      expect(
        await backoff.process(GROUP, () => true, async (letter) => {
          seen.push(valueOf(letter))
          return { shouldEnqueue: false }
        }),
      ).toBe(true)
      expect(seen).toEqual(["b"])
      expect(await backoff.contains(GROUP, "held")).toBe(true)
    })
  })
})
