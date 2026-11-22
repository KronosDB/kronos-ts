import { describe, expect, it } from "bun:test"
import { jsonSerializer, type SequencedEvent, type Tag } from "@kronos-ts/core"
import { axonServerEventStore } from "../axon-server-event-store.js"
import type { AxonServerStoreSource } from "../connection.js"

const encoder = new TextEncoder()
const serializer = jsonSerializer()

/** The event Axon Server holds at `sequence`: no tags travel with it. */
function stored(sequence: bigint) {
  return {
    sequence,
    event: {
      identifier: `event-${sequence}`,
      timestamp: 1n,
      name: "store.Thing",
      version: "",
      payload: serializer.serialize({ n: Number(sequence) }, "store.Thing", "").data,
      metadata: {},
    },
  }
}

const tagsAt = (sequence: bigint): Tag[] => [
  { key: "thing", value: `thing-${sequence}` },
  { key: "all", value: "things" },
]

/**
 * A store over a connection whose event store holds `count` events, and
 * answers `GetTags` when `answerTags` says so. The `Stream` call stays open
 * after its events, or fails when `streamFails` is set.
 */
function storeOver(
  count: number,
  answerTags: (sequence: bigint) => Promise<void> = async () => {},
  streamFails = false,
) {
  let lookups = 0
  let mostLookupsAtOnce = 0
  let streamCancelled = false
  const events = Array.from({ length: count }, (_, index) => stored(BigInt(index)))
  const source: AxonServerStoreSource = {
    serializer,
    connection: {
      config: { token: "", readTimeoutMs: 1000 },
      eventStore: {
        async *source() {
          for (const event of events) yield { event }
          yield { consistencyMarker: BigInt(count) }
        },
        async *stream(_request: unknown, options: { signal: AbortSignal }) {
          options.signal.addEventListener("abort", () => (streamCancelled = true), { once: true })
          for (const event of events) yield { event }
          if (streamFails) throw new Error("stream broke")
          await new Promise<void>((_, reject) => {
            options.signal.addEventListener("abort", () => reject(new Error("cancelled")), { once: true })
          })
        },
        async getTags({ sequence }: { sequence: bigint }) {
          lookups++
          mostLookupsAtOnce = Math.max(mostLookupsAtOnce, lookups)
          try {
            await answerTags(sequence)
          } finally {
            lookups--
          }
          return {
            tag: tagsAt(sequence).map((tag) => ({ key: encoder.encode(tag.key), value: encoder.encode(tag.value) })),
          }
        },
      },
    } as any,
  }
  return {
    store: axonServerEventStore(source, "default"),
    get mostLookupsAtOnce() {
      return mostLookupsAtOnce
    },
    get streamCancelled() {
      return streamCancelled
    },
  }
}

/** Take `count` events off a stream as they become available. */
function take(
  stream: {
    next(): SequencedEvent | undefined
    error(): Error | undefined
    setCallback(callback: () => void): void
  },
  count: number,
): Promise<SequencedEvent[]> {
  return new Promise((resolve, reject) => {
    const taken: SequencedEvent[] = []
    const drain = () => {
      for (let event = stream.next(); event; event = stream.next()) taken.push(event)
      const error = stream.error()
      if (error) reject(error)
      else if (taken.length >= count) resolve(taken)
    }
    stream.setCallback(drain)
    drain()
  })
}

describe("axonServerEventStore and an event's tags", () => {
  it("source() returns each event with the tags stored for its sequence", async () => {
    const { store } = storeOver(3)
    const { events } = await store.source({ query: { tags: { all: "things" } } })
    expect(events.map((event) => event.tags)).toEqual([tagsAt(0n), tagsAt(1n), tagsAt(2n)])
    expect(events.map((event) => event.identifier)).toEqual(["event-0", "event-1", "event-2"])
  })

  it("open() delivers each event with the tags stored for its sequence", async () => {
    const { store } = storeOver(3)
    const stream = store.open({ position: 0n })
    try {
      const events = await take(stream, 3)
      expect(events.map((delivered) => delivered.event.tags)).toEqual([tagsAt(0n), tagsAt(1n), tagsAt(2n)])
    } finally {
      stream.close()
    }
  })

  it("open() keeps the log's order when a later event's tags arrive first", async () => {
    // The first event's tags arrive last.
    const { store } = storeOver(5, (sequence) => new Promise((r) => setTimeout(r, sequence === 0n ? 30 : 0)))
    const stream = store.open({ position: 0n })
    try {
      const events = await take(stream, 5)
      expect(events.map((delivered) => delivered.sequence)).toEqual([0n, 1n, 2n, 3n, 4n])
      expect(events.map((delivered) => delivered.event.tags[0]!.value)).toEqual([
        "thing-0",
        "thing-1",
        "thing-2",
        "thing-3",
        "thing-4",
      ])
    } finally {
      stream.close()
    }
  })

  it("asks for at most 64 events' tags at once", async () => {
    const slow = () => new Promise<void>((r) => setTimeout(r, 1))
    const sourced = storeOver(200, slow)
    expect((await sourced.store.source({ query: { tags: { all: "things" } } })).events).toHaveLength(200)
    expect(sourced.mostLookupsAtOnce).toBe(64)

    const opened = storeOver(200, slow)
    const stream = opened.store.open({ position: 0n })
    try {
      expect(await take(stream, 200)).toHaveLength(200)
      expect(opened.mostLookupsAtOnce).toBe(64)
    } finally {
      stream.close()
    }
  })

  it("source() rejects when an event's tags cannot be read", async () => {
    const { store } = storeOver(3, async (sequence) => {
      if (sequence === 1n) throw new Error("tags unavailable")
    })
    const failure = await store.source({ query: { tags: { all: "things" } } }).then(
      () => undefined,
      (error: unknown) => error,
    )
    expect((failure as Error).message).toContain("tags unavailable")
  })

  it("open() fails the stream and ends the call when an event's tags cannot be read", async () => {
    const over = storeOver(3, async (sequence) => {
      if (sequence === 1n) throw new Error("tags unavailable")
    })
    const stream = over.store.open({ position: 0n })
    try {
      const taken: SequencedEvent[] = []
      const failure = await new Promise<Error>((resolve) => {
        const drain = () => {
          for (let event = stream.next(); event; event = stream.next()) taken.push(event)
          const error = stream.error()
          if (error) resolve(error)
        }
        stream.setCallback(drain)
        drain()
      })
      expect(failure.message).toContain("tags unavailable")
      // The event before the one that failed was delivered, the ones after were not.
      expect(taken.map((delivered) => delivered.sequence)).toEqual([0n])
      expect(stream.isCompleted()).toBe(true)
      expect(over.streamCancelled).toBe(true)
    } finally {
      stream.close()
    }
  })

  it("open() delivers the events read before the call failed, with their tags", async () => {
    const { store } = storeOver(3, () => new Promise((r) => setTimeout(r, 5)), true)
    const stream = store.open({ position: 0n })
    try {
      const taken: SequencedEvent[] = []
      const failure = await new Promise<Error>((resolve) => {
        const drain = () => {
          for (let event = stream.next(); event; event = stream.next()) taken.push(event)
          const error = stream.error()
          if (error) resolve(error)
        }
        stream.setCallback(drain)
        drain()
      })
      expect(failure.message).toContain("stream broke")
      expect(taken.map((delivered) => delivered.event.tags)).toEqual([tagsAt(0n), tagsAt(1n), tagsAt(2n)])
    } finally {
      stream.close()
    }
  })

  it("close() ends the call and reports no error", async () => {
    const over = storeOver(3)
    const stream = over.store.open({ position: 0n })
    await take(stream, 3)
    stream.close()
    await new Promise((r) => setTimeout(r, 10))
    expect(over.streamCancelled).toBe(true)
    expect(stream.error()).toBeUndefined()
  })
})
