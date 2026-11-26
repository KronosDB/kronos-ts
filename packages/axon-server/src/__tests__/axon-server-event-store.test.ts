import { describe, expect, it } from "bun:test"
import { jsonSerializer, upcastingEventStore, type SequencedEvent } from "@kronos-ts/core"
import { axonServerEventStore } from "../axon-server-event-store.js"
import type { AxonServerStoreSource } from "../connection.js"

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

/**
 * A store over a connection whose event store holds `count` events. The
 * `Stream` call stays open after its events, or fails when `streamFails` is
 * set.
 */
function storeOver(count: number, streamFails = false) {
  let tagLookups = 0
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
        async getTags() {
          tagLookups++
          return { tag: [] }
        },
      },
    } as any,
  }
  return {
    store: axonServerEventStore(source, "default"),
    get tagLookups() {
      return tagLookups
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
  it("source() returns each event without tags and asks the server for none", async () => {
    const over = storeOver(3)
    const { events } = await over.store.source({ query: { tags: { all: "things" } } })
    expect(events.map((event) => event.identifier)).toEqual(["event-0", "event-1", "event-2"])
    expect(events.map((event) => event.tags)).toEqual([[], [], []])
    expect(over.tagLookups).toBe(0)
  })

  it("open() delivers each event without tags and asks the server for none", async () => {
    const over = storeOver(3)
    const stream = over.store.open({ position: 0n })
    try {
      const events = await take(stream, 3)
      expect(events.map((delivered) => delivered.sequence)).toEqual([0n, 1n, 2n])
      expect(events.map((delivered) => delivered.event.tags)).toEqual([[], [], []])
      expect(over.tagLookups).toBe(0)
    } finally {
      stream.close()
    }
  })

  it("upcastingEventStore over it hands back the events of a read with tags", async () => {
    const store = upcastingEventStore(storeOver(3).store, [])
    const { events } = await store.source({ query: { tags: { all: "things" }, types: ["store.Thing"] } })
    expect(events.map((event) => event.identifier)).toEqual(["event-0", "event-1", "event-2"])
  })

  it("open() delivers the events read before the call failed", async () => {
    const { store } = storeOver(3, true)
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
      expect(taken.map((delivered) => delivered.sequence)).toEqual([0n, 1n, 2n])
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
