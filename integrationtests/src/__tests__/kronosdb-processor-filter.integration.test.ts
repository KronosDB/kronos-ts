/**
 * An event processor over a real KronosDB: the server filters, and the token
 * still moves.
 *
 * A processor asks for the types its handlers handle. What this checks, which
 * the unit tests can only fake: that the server sends nothing else, that the
 * Progress frames it sends through a quiet stretch reach the token store, that
 * a restart resumes from there, and that the filter survives an upcaster which
 * renames a type.
 *
 * Needs KRONOSDB_SERVER_BIN or KRONOSDB_IMAGE (see ../kronosdb-server.ts).
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import {
  emptyMetadata,
  globalSequenceToken,
  batched,
  eventProcessor,
  generateIdentifier,
  inMemoryTokenStore,
  jsonSerializer,
  kronos,
  localCommandBus,
  localQueryBus,
  qn,
  unitOfWork,
  upcastingEventStore,
  type EventHandler,
  type EventMessage,
  type EventStore,
  type RunningProcessor,
  type TokenStore,
  type Upcast,
} from "@kronos-ts/core"
import { kronosDbConnection, kronosDbEventStore, type KronosDbConnectionHandle } from "@kronos-ts/kronosdb"
import { startKronosDb, type KronosDbServer } from "../kronosdb-server.js"

const Ordered = qn("filter-it", "Ordered")
const Noise = qn("filter-it", "Noise")
const OrderPlaced = qn("filter-it", "OrderPlaced") // the name the log has for what code now calls Ordered

function fact(name: { namespace: string; name: string }, n: number): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name,
    version: "1",
    payload: { n },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags: [{ key: "run", value: "x" }],
  }
}

async function until(done: () => boolean, what: string, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!done() && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10))
  if (!done()) throw new Error(`timed out waiting for ${what}`)
}

function handling(name: { namespace: string; name: string }, into: number[]): EventHandler<any> {
  return {
    kind: "event-handler",
    descriptor: { kind: "event", name, version: "1", payload: {} as any },
    handler: ({ payload }) => {
      into.push((payload as { n: number }).n)
    },
  }
}

describe("a processor over KronosDB, with the filter on the server", () => {
  let server: KronosDbServer
  let kdb: KronosDbConnectionHandle
  let store: EventStore

  beforeAll(async () => {
    server = await startKronosDb()
    kdb = await kronosDbConnection({ componentName: "filter-it", host: server.host, port: server.port, serializer: jsonSerializer() })
    // The progress interval is how long the token waits to hear it may move on.
    store = kronosDbEventStore(kdb, "default", { progressIntervalMs: 50 })
  }, 120_000)

  afterAll(async () => {
    for (const app of apps) await app.stop()
    await kdb?.close()
    await server?.stop()
  })

  /**
   * A processor assembled the way a host does it. `kronos` starts it at once, so
   * a test that wants a replay stops it, resets it and starts it again.
   */
  const apps: Array<{ stop(): Promise<void> }> = []
  function run(config: { name: string; eventStore: EventStore; tokenStore: TokenStore; handlers: EventHandler<any>[] }): RunningProcessor {
    const processor = batched(eventProcessor(config.name, config.eventStore, config.tokenStore, unitOfWork), 10)
    const commandBus = localCommandBus(unitOfWork)
    const queryBus = localQueryBus(unitOfWork)
    const app = kronos({ eventHandlers: config.handlers.map((h) => ({ ...h, commandBus, queryBus, processor })) })
    apps.push(app)
    return app.processors.get(config.name)!
  }

  /** A token store whose processor has already read to `position`, as one that has been running does. */
  async function readTo(name: string, position: bigint): Promise<TokenStore> {
    const tokens = inMemoryTokenStore()
    await tokens.initializeSegments(name, 1)
    await tokens.store(name, 0, globalSequenceToken(position))
    return tokens
  }

  async function replayFrom(processor: RunningProcessor, position: bigint): Promise<void> {
    // `kronos` has already started it; let that finish before taking it over.
    await until(() => processor.running, "the processor to start")
    processor.stop()
    await processor.resetTokens(position)
    await processor.start()
  }

  it("receives only what it handles, moves its token to the head through the noise, and resumes from there", async () => {
    // 10 events of interest among 3000 others, the last of interest early.
    const batch: EventMessage[] = []
    for (let i = 0; i < 3000; i++) batch.push(i % 300 === 0 ? fact(Ordered, i / 300) : fact(Noise, i))
    await store.append(batch)

    const seen: number[] = []
    const writes: bigint[] = []
    const tokens = inMemoryTokenStore()
    const recording: TokenStore = {
      ...tokens,
      store: async (...args) => {
        writes.push(args[2].position())
        return tokens.store(...args)
      },
    }

    const first = run({ name: "orders", eventStore: store, tokenStore: recording, handlers: [handling(Ordered, seen)] })
    await until(() => seen.length === 10, "the ten events")
    expect(seen).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    // The last match is at position 2700, so the token is behind the head by
    // 299 events nobody handles.
    expect(first.position).toBe(2701n)

    // The log goes on, and the server reports how far it has looked: the token
    // moves past the quiet stretch — a stretch that holds no event for it.
    await store.append([fact(Noise, 0)])
    const newHead = await store.getHeadPosition()
    await until(() => first.position === newHead, "the token to reach the head through the noise")
    first.stop()
    expect(writes.at(-1)).toBe(newHead)
    expect(writes).toContain(2701n)

    // A restart resumes from the head: nothing is read again.
    const again: number[] = []
    const second = run({ name: "orders", eventStore: store, tokenStore: tokens, handlers: [handling(Ordered, again)] })
    await new Promise((r) => setTimeout(r, 200))
    expect(second.position).toBe(newHead)
    await store.append([fact(Ordered, 99)])
    await until(() => again.length === 1, "a new event")
    second.stop()
    expect(again).toEqual([99])
  }, 60_000)

  it("ends a replay through a quiet tail", async () => {
    const before = await store.getHeadPosition()
    await store.append([fact(Ordered, 500), ...Array.from({ length: 500 }, (_, i) => fact(Noise, i))])
    const head = await store.getHeadPosition()
    expect(head).toBeGreaterThan(before)

    const seen: number[] = []
    const processor = run({ name: "replayed", eventStore: store, tokenStore: await readTo("replayed", head), handlers: [handling(Ordered, seen)] })
    await replayFrom(processor, before)
    expect(processor.replaying).toBe(true)
    await until(() => seen.length === 1, "the one event")
    // The tail of the log is quiet: only the server's account of how far it has
    // looked can end the replay, and it speaks when the log moves on.
    await store.append([fact(Noise, 0)])
    await until(() => !processor.replaying && processor.position > head, "the replay to end through the quiet tail")
    processor.stop()

    expect(seen).toEqual([500])
  }, 60_000)

  it("finds an event stored under a name the upcaster has since renamed", async () => {
    await store.append([fact(OrderPlaced, 700), fact(Noise, 1), fact(Ordered, 701)])
    const head = await store.getHeadPosition()
    const rename: Upcast = (e) => (e.name.name === OrderPlaced.name ? { ...e, name: Ordered, version: "2" } : e)

    // Told which stored names become Ordered: the server is asked for both.
    const told: number[] = []
    const aware = run({
      name: "renamed-told",
      eventStore: upcastingEventStore(store, rename, {
        storedTypes: (type) => (type === "filter-it.Ordered" ? [type, "filter-it.OrderPlaced"] : [type]),
      }),
      tokenStore: await readTo("renamed-told", head),
      handlers: [handling(Ordered, told)],
    })
    await replayFrom(aware, head - 3n)
    await until(() => told.length === 2, "both events")
    aware.stop()
    expect(told.sort()).toEqual([700, 701])

    // Not told: the type restriction is widened, so nothing is lost either.
    const widened: number[] = []
    const unaware = run({
      name: "renamed-widened",
      eventStore: upcastingEventStore(store, rename),
      tokenStore: await readTo("renamed-widened", head),
      handlers: [handling(Ordered, widened)],
    })
    await replayFrom(unaware, head - 3n)
    await until(() => widened.length === 2, "both events")
    unaware.stop()
    expect(widened.sort()).toEqual([700, 701])
  }, 60_000)
})
