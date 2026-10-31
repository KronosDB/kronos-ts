import { describe, expect, it } from "bun:test"
import { emptyMetadata, qn, qualifiedNameToString, type QualifiedName } from "../../messaging/messages.js"
import { compileQuery, type EventQuery } from "../../event-sourcing/dcb-query.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { batched, deadLettering, eventProcessor, polling, type EventProcessor } from "../processor.js"
import { runEventProcessor } from "../running-processor.js"
import { inMemoryTokenStore, type TokenStore } from "../token-store.js"
import type { SequencedEvent, StreamingCondition } from "../source.js"
import type { EventHandler } from "../handler.js"
import { inMemoryDeadLetterQueue } from "../dead-letter-queue.js"
import { deserializeToken, globalSequenceToken, serializeToken, type ReplayToken, type TrackingToken } from "../tracking-token.js"

// ---------------------------------------------------------------------------
// What a processor asks of its log, and how it moves through a quiet stretch.
//
// The log here is a fake that FILTERS by the query it is opened with (as
// KronosDB does), counts how many events it was asked to read ahead, and — when
// `reportsProgress` — says how far it has looked, like a Progress frame does.
// ---------------------------------------------------------------------------

const Ordered = qn("shop", "Ordered")
const Shipped = qn("shop", "Shipped")
const Noise = qn("shop", "Noise")

function event(name: QualifiedName, sequence: bigint, tags: Array<{ key: string; value: string }> = []): SequencedEvent {
  return {
    sequence,
    event: {
      kind: "event",
      identifier: `e-${sequence}`,
      name,
      version: "1.0",
      payload: { n: Number(sequence) },
      metadata: emptyMetadata(),
      timestamp: 0,
      tags,
    },
  }
}

type FakeLog = EventStore & {
  readonly opened: StreamingCondition[]
  /** Add events to the log, waking every open stream. */
  push(...more: SequencedEvent[]): void
  /** Close every open stream with an error, as a dropped connection does. */
  readonly head: () => bigint
}

function fakeLog(initial: SequencedEvent[], options: { reportsProgress?: boolean } = {}): FakeLog {
  const log = [...initial]
  const waiting = new Set<() => void>()
  const opened: StreamingCondition[] = []
  const head = () => (log.length === 0 ? 0n : log[log.length - 1]!.sequence + 1n)

  const store = {
    opened,
    head,
    push(...more: SequencedEvent[]) {
      log.push(...more)
      for (const wake of waiting) wake()
    },
    open(condition: StreamingCondition) {
      opened.push(condition)
      let cursor = condition.position
      let callback: (() => void) | null = null
      const wanted = condition.query ? compileQuery(condition.query) : undefined
      const names = wanted && wanted.kind === "type-restricted" ? new Set(wanted.types) : undefined
      const matches = (e: SequencedEvent) => e.sequence >= cursor && (!names || names.has(qualifiedNameToString(e.event.name)))
      const wake = () => callback?.()
      waiting.add(wake)
      return {
        next() {
          const found = log.find(matches)
          if (found) cursor = found.sequence + 1n
          return found
        },
        peek: () => log.find(matches),
        hasNextAvailable: () => log.some(matches),
        isCompleted: () => false,
        error: () => undefined,
        setCallback(cb: () => void) {
          callback = cb
        },
        close() {
          callback = null
          waiting.delete(wake)
        },
        ...(options.reportsProgress && condition.query
          ? {
              progressToken: (): TrackingToken | undefined =>
                log.some(matches) ? undefined : globalSequenceToken(head()),
            }
          : {}),
      }
    },
    async getHeadPosition() {
      return head()
    },
  }
  return store as unknown as FakeLog
}

function handlerFor(name: QualifiedName, into: bigint[] = []): EventHandler<any> {
  return {
    kind: "event-handler",
    descriptor: { kind: "event", name, version: "1.0", payload: {} as any },
    handler: ({ sequence }) => {
      into.push(sequence)
    },
  }
}

/** A token store that remembers every write. */
function recordingTokens(): TokenStore & { readonly writes: bigint[] } {
  const inner = inMemoryTokenStore()
  const writes: bigint[] = []
  return Object.assign(Object.create(inner) as TokenStore, {
    writes,
    store: async (...args: Parameters<TokenStore["store"]>) => {
      writes.push(args[2].position())
      return inner.store(...args)
    },
  }) as TokenStore & { readonly writes: bigint[] }
}

function run(config: {
  log: EventStore
  handlers: EventHandler<any>[]
  tokens?: TokenStore
  batch?: number
  withQueue?: boolean
}) {
  let processor: EventProcessor = eventProcessor("p", config.log, config.tokens ?? inMemoryTokenStore(), unitOfWork)
  if (config.batch !== undefined) processor = batched(processor, config.batch)
  if (config.withQueue) processor = deadLettering(processor, inMemoryDeadLetterQueue(), (e) => e.tags.find((t) => t.key === "order")?.value ?? qualifiedNameToString(e.name))
  return runEventProcessor({
    processor: polling(processor, 5),
    handlers: config.handlers.map((definition) => ({ definition })),
  })
}

async function until(done: () => boolean, what: string, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!done() && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5))
  if (!done()) throw new Error(`timed out waiting for ${what}`)
}

const types = (query: EventQuery | undefined) => (query === undefined ? undefined : (query as { types: unknown[] }).types)

// ---------------------------------------------------------------------------

describe("the query a processor opens its stream with", () => {
  it("is the types its handlers handle, once each", async () => {
    const log = fakeLog([])
    const processor = run({ log, handlers: [handlerFor(Ordered), handlerFor(Shipped), handlerFor(Ordered)] })
    await processor.start()
    await until(() => log.opened.length > 0, "the stream to open")
    processor.stop()

    expect(types(log.opened[0]!.query)).toEqual(["shop.Ordered", "shop.Shipped"])
  })

  it("is how a store that filters never sends the rest", async () => {
    const seen: bigint[] = []
    const log = fakeLog([event(Noise, 0n), event(Ordered, 1n), event(Noise, 2n), event(Ordered, 3n)])
    const processor = run({ log, handlers: [handlerFor(Ordered, seen)] })
    await processor.start()
    await until(() => seen.length === 2, "both events")
    processor.stop()

    expect(seen).toEqual([1n, 3n])
  })

  it("is a request, not a promise: a store that ignores it still gets only the handled events delivered", async () => {
    const seen: bigint[] = []
    const log = fakeLog([event(Noise, 0n), event(Ordered, 1n), event(Noise, 2n)])
    const ignoring: EventStore = { ...log, open: (condition) => log.open({ position: condition.position }) } as EventStore
    const processor = run({ log: ignoring, handlers: [handlerFor(Ordered, seen)] })
    await processor.start()
    await until(() => processor.position === 3n, "the position to reach the end")
    processor.stop()

    expect(seen).toEqual([1n])
  })
})

describe("the token through a quiet stretch", () => {
  it("moves to where the stream says it has looked, and is stored", async () => {
    const tokens = recordingTokens()
    const log = fakeLog([event(Ordered, 0n), ...Array.from({ length: 50 }, (_, i) => event(Noise, BigInt(i + 1)))], {
      reportsProgress: true,
    })
    const processor = run({ log, handlers: [handlerFor(Ordered)], tokens })
    await processor.start()
    await until(() => processor.position === 51n, "the position to pass the quiet stretch")
    processor.stop()

    // The one event moved it to 1; the quiet stretch moved it the rest of the way.
    expect(tokens.writes.at(-1)).toBe(51n)
    expect(tokens.writes).toContain(1n)
    expect(processor.status().caughtUp).toBe(true)
  })

  it("moves while LIVE too: events nobody handles arrive and the position follows", async () => {
    const log = fakeLog([event(Ordered, 0n)], { reportsProgress: true })
    const processor = run({ log, handlers: [handlerFor(Ordered)] })
    await processor.start()
    await until(() => processor.position === 1n, "the first event")

    log.push(event(Noise, 1n), event(Noise, 2n))
    await until(() => processor.position === 3n, "the position to follow the noise")
    processor.stop()
  })

  it("does not move on a store that cannot say, and that is not a fault", async () => {
    const log = fakeLog([event(Ordered, 0n), event(Noise, 1n), event(Noise, 2n)])
    const processor = run({ log, handlers: [handlerFor(Ordered)] })
    await processor.start()
    await until(() => processor.position === 1n, "the matching event")
    await new Promise((r) => setTimeout(r, 40))
    processor.stop()

    expect(processor.position).toBe(1n)
    expect(processor.status().error).toBeUndefined()
  })

  it("writes nothing when the stream has not looked past the token", async () => {
    const tokens = recordingTokens()
    const log = fakeLog([event(Ordered, 0n)], { reportsProgress: true })
    const processor = run({ log, handlers: [handlerFor(Ordered)], tokens })
    await processor.start()
    await until(() => processor.position === 1n, "the event")
    const before = tokens.writes.length
    await new Promise((r) => setTimeout(r, 50))
    processor.stop()

    expect(tokens.writes.length).toBe(before)
  })

  it("ends a replay when the stream has looked past where the reset began", async () => {
    // The last event of interest is long before the head, so only the stream's
    // own account of the quiet tail can finish the replay.
    const log = fakeLog([event(Ordered, 0n), ...Array.from({ length: 20 }, (_, i) => event(Noise, BigInt(i + 1)))], {
      reportsProgress: true,
    })
    const seen: boolean[] = []
    const handler: EventHandler<any> = {
      ...handlerFor(Ordered),
      handler: (_m, ctx: any) => {
        seen.push(ctx.isReplay())
      },
    }
    const processor = run({ log, handlers: [handler] })
    await processor.start()
    await until(() => processor.position === 21n, "the first pass to reach the head")
    processor.stop()
    seen.length = 0
    await processor.resetTokens(0n)
    expect(processor.replaying).toBe(true)

    await processor.start()
    await until(() => processor.position === 21n, "the replay to reach the head")
    processor.stop()

    expect(seen).toEqual([true])
    expect(processor.replaying).toBe(false)
  })

  it("does not call a replay finished just because the stream ran dry for a moment", async () => {
    // A stream that reports progress can be empty with events still on the wire.
    // Without a progress report that covers the reset point, the replay goes on.
    const log = fakeLog([event(Ordered, 0n), event(Noise, 1n)], { reportsProgress: true })
    const stalled: EventStore = {
      ...log,
      open: (condition) => ({ ...log.open(condition), progressToken: () => undefined }),
    } as EventStore
    // The first pass, on the stream that DOES report, gets the token past the
    // noise — so the replay runs up to 2, beyond the one event it will see.
    const tokens = inMemoryTokenStore()
    const first = run({ log, handlers: [handlerFor(Ordered)], tokens })
    await first.start()
    await until(() => first.position === 2n, "the first pass past the noise")
    first.stop()
    const processor = run({ log: stalled, handlers: [handlerFor(Ordered)], tokens })
    await processor.resetTokens(0n)
    await processor.start()
    await until(() => processor.position === 1n, "the matching event")
    await new Promise((r) => setTimeout(r, 40))
    processor.stop()

    expect(processor.replaying).toBe(true)
  })

  it("still ends a replay on an empty stream when the store reports no progress at all", async () => {
    const log = fakeLog([event(Ordered, 0n), event(Noise, 1n)])
    const processor = run({ log, handlers: [handlerFor(Ordered)] })
    await processor.start()
    await until(() => processor.position === 1n, "the first pass")
    processor.stop()
    await processor.resetTokens(0n)
    expect(processor.replaying).toBe(true)
    await processor.start()
    await until(() => !processor.replaying, "the replay to end")
    processor.stop()
  })
})

describe("parked events and a filtered stream", () => {
  it("park the same events they always did: the ones nobody handles never reach the queue", async () => {
    const order = (n: string) => [{ key: "order", value: n }]
    const failing = new Set([1n])
    const delivered: bigint[] = []
    const handler: EventHandler<any> = {
      ...handlerFor(Ordered),
      handler: ({ sequence }) => {
        if (failing.has(sequence)) throw new Error("poison")
        delivered.push(sequence)
      },
    }
    // Lane "a": 1 fails, so 3 is parked behind it. Lane "b" is untouched. The
    // noise in between is in lane "a" too, and must not be parked.
    const log = fakeLog(
      [
        event(Ordered, 0n, order("b")),
        event(Ordered, 1n, order("a")),
        event(Noise, 2n, order("a")),
        event(Ordered, 3n, order("a")),
        event(Ordered, 4n, order("b")),
      ],
      { reportsProgress: true },
    )
    const processor = run({ log, handlers: [handler], withQueue: true })
    await processor.start()
    await until(() => processor.position === 5n, "the end of the log")
    processor.stop()

    expect(delivered).toEqual([0n, 4n])
  })
})

describe("the batch size", () => {
  const backlog = (n: number) => Array.from({ length: n }, (_, i) => event(Ordered, BigInt(i)))

  it("unwrapped, is 1000 while far behind the head", async () => {
    const tokens = recordingTokens()
    const log = fakeLog(backlog(2500))
    const processor = run({ log, handlers: [handlerFor(Ordered)], tokens })
    await processor.start()
    await until(() => processor.position === 2500n, "the backlog")
    processor.stop()

    // 1000 + 1000 + 500: three batches, not 2500.
    expect(tokens.writes).toEqual([1000n, 2000n, 2500n])
  })

  it("asks the stream to read ahead as far as a batch", async () => {
    const log = fakeLog([])
    const processor = run({ log, handlers: [handlerFor(Ordered)], batch: 4000 })
    await processor.start()
    await until(() => log.opened.length > 0, "the stream")
    processor.stop()

    expect(log.opened[0]!.readAhead).toBe(4000)
  })

  it("unwrapped, takes one event at the head", async () => {
    const tokens = recordingTokens()
    const log = fakeLog(backlog(10))
    const processor = run({ log, handlers: [handlerFor(Ordered)], tokens })
    await processor.start()
    await until(() => processor.position === 10n && processor.status().caughtUp, "the head")
    expect(tokens.writes).toEqual([10n])

    // Live now: five events arrive together.
    log.push(...[10n, 11n, 12n, 13n, 14n].map((n) => event(Ordered, n)))
    await until(() => processor.position === 15n, "the live events")
    processor.stop()

    // The first live batch is one event. (What is left behind it is a backlog
    // again, and is taken in one go.)
    expect(tokens.writes.slice(0, 2)).toEqual([10n, 11n])
    expect(tokens.writes.at(-1)).toBe(15n)
  })

  it("batched, is the same size at the head and behind it", async () => {
    const tokens = recordingTokens()
    const log = fakeLog(backlog(6))
    const processor = run({ log, handlers: [handlerFor(Ordered)], tokens, batch: 3 })
    await processor.start()
    await until(() => processor.position === 6n && processor.status().caughtUp, "the backlog")
    expect(tokens.writes).toEqual([3n, 6n])

    log.push(...[6n, 7n, 8n, 9n, 10n].map((n) => event(Ordered, n)))
    await until(() => processor.position === 11n, "the live events")
    processor.stop()

    expect(tokens.writes.slice(2)).toEqual([9n, 11n])
  })

  it("retries a failed batch one event at a time, so the bad event ends up alone", async () => {
    const tokens = recordingTokens()
    const log = fakeLog(backlog(8))
    let failedOnce = false
    const handler: EventHandler<any> = {
      ...handlerFor(Ordered),
      handler: ({ sequence }) => {
        if (sequence === 5n && !failedOnce) {
          failedOnce = true
          throw new Error("transient")
        }
      },
    }
    const processor = run({ log, handlers: [handler], tokens, batch: 100 })
    const quiet = console.error
    console.error = () => {}
    try {
      await processor.start()
      await until(() => processor.position === 8n, "the retry to get through", 3000)
    } finally {
      console.error = quiet
      processor.stop()
    }

    // The whole 8-event batch failed and wrote nothing; every batch after it,
    // up to the event that failed, is one event.
    expect(tokens.writes.slice(0, 6)).toEqual([1n, 2n, 3n, 4n, 5n, 6n])
    expect(tokens.writes.at(-1)).toBe(8n)
  })
})

describe("resetting a processor", () => {
  /** A handler that records, per event, whether it was told it is a replay. */
  function replayRecorder(): { handler: EventHandler<any>; seen: Array<[bigint, boolean]> } {
    const seen: Array<[bigint, boolean]> = []
    return {
      seen,
      handler: { ...handlerFor(Ordered), handler: ({ sequence }, ctx: any) => void seen.push([sequence, ctx.isReplay()]) },
    }
  }

  /** What a persistent store does: every token goes through its stored form. */
  function serializingTokens(): TokenStore {
    const inner = inMemoryTokenStore()
    return {
      ...inner,
      store: (name, segment, token, uow) => {
        const { type, data } = serializeToken(token)
        return inner.store(name, segment, deserializeToken(type, data)!, uow)
      },
    }
  }

  it("replays up to where the processor had got to, not the head of the log", async () => {
    const log = fakeLog([event(Ordered, 0n), event(Ordered, 1n)])
    const { handler, seen } = replayRecorder()
    const processor = run({ log, handlers: [handler] })
    await processor.start()
    await until(() => processor.position === 2n, "the first pass")
    processor.stop()
    // Three more arrive while it is stopped: it was never handed them.
    log.push(event(Ordered, 2n), event(Ordered, 3n), event(Ordered, 4n))

    seen.length = 0
    await processor.resetTokens(0n)
    await processor.start()
    await until(() => processor.position === 5n, "the replay and what came after")
    processor.stop()

    expect(seen).toEqual([[0n, true], [1n, true], [2n, false], [3n, false], [4n, false]])
  })

  it("tells each event by its own position: one past the reset point is new, however the stream got there", async () => {
    // Filtered: the noise at 1–3 never reaches the processor, so the event at 4
    // comes straight after the one at 0, past a reset point of 2.
    const log = fakeLog([event(Ordered, 0n), event(Noise, 1n)], { reportsProgress: true })
    const { handler, seen } = replayRecorder()
    const processor = run({ log, handlers: [handler], batch: 10 })
    await processor.start()
    await until(() => processor.position === 2n, "the first pass past the noise")
    processor.stop()
    log.push(event(Noise, 2n), event(Noise, 3n), event(Ordered, 4n))

    seen.length = 0
    await processor.resetTokens(0n)
    await processor.start()
    await until(() => processor.position === 5n, "the replay and the new event")
    processor.stop()

    expect(seen).toEqual([[0n, true], [4n, false]])
  })

  it("is still a replay after a restart through a store that keeps only the stored form", async () => {
    const tokens = serializingTokens()
    const log = fakeLog([event(Ordered, 0n), event(Ordered, 1n), event(Ordered, 2n)])
    const first = run({ log, handlers: [handlerFor(Ordered)], tokens })
    await first.start()
    await until(() => first.position === 3n, "the first pass")
    first.stop()
    await first.resetTokens(0n)

    // A new process: nothing but what the store kept.
    const { handler, seen } = replayRecorder()
    const second = run({ log, handlers: [handler], tokens })
    await second.start()
    await until(() => second.position === 3n, "the replay")
    second.stop()

    expect(seen).toEqual([[0n, true], [1n, true], [2n, true]])
    expect(second.replaying).toBe(false)
  })

  it("reset again mid-replay, still replays up to the first reset's point", async () => {
    const tokens = serializingTokens()
    const log = fakeLog([event(Ordered, 0n), event(Ordered, 1n), event(Ordered, 2n)])
    const processor = run({ log, handlers: [handlerFor(Ordered)], tokens })
    await processor.start()
    await until(() => processor.position === 3n, "the first pass")
    processor.stop()
    await processor.resetTokens(2n)
    await processor.resetTokens(0n)

    const stored = (await tokens.get("p", 0)) as ReplayToken
    expect(stored.kind).toBe("replay")
    expect(stored.tokenAtReset.position()).toBe(3n)
    expect(stored.position()).toBe(0n)
  })

  it("replays nothing when reset to where the processor already is, or past it, or before it ever ran", async () => {
    const log = fakeLog([event(Ordered, 0n), event(Ordered, 1n)])
    const processor = run({ log, handlers: [handlerFor(Ordered)] })
    await processor.resetTokens(0n)
    expect(processor.replaying).toBe(false)

    await processor.start()
    await until(() => processor.position === 2n, "the first pass")
    processor.stop()
    await processor.resetTokens(2n)
    expect(processor.replaying).toBe(false)
    await processor.resetTokens(5n)
    expect(processor.replaying).toBe(false)
    expect(processor.position).toBe(5n)
  })

  it("runs the reset function in the unit of work that stores the token", async () => {
    const stores: unknown[] = []
    const inner = inMemoryTokenStore()
    const tokens: TokenStore = {
      ...inner,
      store: (name, segment, token, uow) => {
        stores.push(uow)
        return inner.store(name, segment, token, uow)
      },
    }
    const log = fakeLog([event(Ordered, 0n)])
    const processor = run({ log, handlers: [handlerFor(Ordered)], tokens })
    await processor.start()
    await until(() => processor.position === 1n, "the first pass")
    processor.stop()

    let resetIn: unknown
    await processor.resetTokens(0n, (uow) => {
      resetIn = uow
    })

    expect(resetIn).toBeDefined()
    expect(stores.at(-1)).toBe(resetIn)
  })

  it("leaves the token where it was when the reset function throws", async () => {
    const log = fakeLog([event(Ordered, 0n)])
    const tokens = inMemoryTokenStore()
    const processor = run({ log, handlers: [handlerFor(Ordered)], tokens })
    await processor.start()
    await until(() => processor.position === 1n, "the first pass")
    processor.stop()

    await expect(
      processor.resetTokens(0n, () => {
        throw new Error("not today")
      }),
    ).rejects.toThrow("not today")

    expect(processor.replaying).toBe(false)
    expect(processor.position).toBe(1n)
    expect((await tokens.get("p", 0))!.kind).toBe("global-sequence")
  })
})
