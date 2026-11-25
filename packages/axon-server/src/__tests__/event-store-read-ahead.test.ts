import { describe, expect, it } from "bun:test"
import { jsonSerializer } from "@kronos-ts/core"
import { axonServerEventStore } from "../axon-server-event-store.js"
import type { AxonServerStoreSource } from "../connection.js"

const serializer = jsonSerializer()

function stored(sequence: number) {
  return {
    sequence: BigInt(sequence),
    event: {
      identifier: `event-${sequence}`,
      timestamp: 1n,
      name: "store.Thing",
      version: "",
      payload: serializer.serialize({ n: sequence }, "store.Thing", "").data,
      metadata: {},
    },
  }
}

/**
 * A store over a `Stream` call that has `count` events to send and then does
 * what `after` says: stays open, ends, or fails. `sent` is how many events the
 * store has taken off the call. A tag lookup is answered with no tags.
 */
function storeOver(count: number, after: "open" | "end" | "fail" = "open") {
  const state = { sent: 0, cancelled: false }
  const source: AxonServerStoreSource = {
    serializer,
    connection: {
      config: { token: "", readTimeoutMs: 1000 },
      eventStore: {
        async *stream(_request: unknown, options: { signal: AbortSignal }) {
          options.signal.addEventListener("abort", () => (state.cancelled = true), { once: true })
          for (let sequence = 0; sequence < count; sequence++) {
            state.sent++
            yield { event: stored(sequence) }
          }
          if (after === "fail") throw new Error("stream broke")
          if (after === "end") return
          await new Promise<void>((_, reject) => {
            options.signal.addEventListener("abort", () => reject(new Error("cancelled")), { once: true })
          })
        },
      },
    } as any,
  }
  return { store: axonServerEventStore(source, "default"), state }
}

const settle = () => new Promise((r) => setTimeout(r, 10))

/**
 * How many events the store may have taken off the call beyond the ones it
 * holds ready, for work it does on an event before it is ready.
 */
const IN_HAND = 80

describe("axonServerEventStore open() and a slow reader", () => {
  it("stops taking events off the call once it holds readAhead of them, until its reader takes some", async () => {
    const { store, state } = storeOver(10_000)
    const stream = store.open({ position: 0n, readAhead: 5 })
    try {
      await settle()
      const held = state.sent
      expect(held).toBeGreaterThanOrEqual(5)
      expect(held).toBeLessThanOrEqual(5 + IN_HAND)
      await settle()
      expect(state.sent).toBe(held)

      expect(stream.next()!.sequence).toBe(0n)
      expect(stream.next()!.sequence).toBe(1n)
      await settle()
      expect(state.sent).toBeGreaterThan(held)
      expect(state.sent).toBeLessThanOrEqual(7 + IN_HAND)
    } finally {
      stream.close()
    }
  })

  it("holds 1000 events ready when the reader names no readAhead", async () => {
    const { store, state } = storeOver(10_000)
    const stream = store.open({ position: 0n })
    try {
      await settle()
      expect(state.sent).toBeGreaterThanOrEqual(1000)
      expect(state.sent).toBeLessThanOrEqual(1000 + IN_HAND)
      let ready = 0
      while (stream.next()) ready++
      expect(ready).toBe(1000)
    } finally {
      stream.close()
    }
  })

  it("delivers every event in order to a reader that takes them a few at a time", async () => {
    const { store, state } = storeOver(500, "end")
    const stream = store.open({ position: 0n, readAhead: 7 })
    const taken: bigint[] = []
    let mostAhead = 0
    while (!stream.isCompleted()) {
      for (let n = 0; n < 3; n++) {
        const next = stream.next()
        if (next) taken.push(next.sequence)
      }
      await Promise.resolve()
      await Promise.resolve()
      mostAhead = Math.max(mostAhead, state.sent - taken.length)
    }
    expect(taken).toEqual(Array.from({ length: 500 }, (_, sequence) => BigInt(sequence)))
    expect(mostAhead).toBeLessThanOrEqual(7 + IN_HAND)
    expect(stream.error()).toBeUndefined()
  })

  it("wakes its reader for each event and when the call ends", async () => {
    const { store } = storeOver(3, "end")
    const stream = store.open({ position: 0n, readAhead: 2 })
    let wakes = 0
    stream.setCallback(() => wakes++)
    await settle()
    expect(wakes).toBe(2)
    expect(stream.isCompleted()).toBe(false)
    stream.next()
    stream.next()
    await settle()
    // The third event, then the end of the call.
    expect(wakes).toBe(4)
    expect(stream.next()!.sequence).toBe(2n)
    expect(stream.isCompleted()).toBe(true)
  })

  it("reports a failed call after the events read before it", async () => {
    const { store } = storeOver(2, "fail")
    const stream = store.open({ position: 0n, readAhead: 10 })
    await settle()
    expect(stream.error()?.message).toContain("stream broke")
    expect(stream.isCompleted()).toBe(false)
    expect(stream.next()!.sequence).toBe(0n)
    expect(stream.next()!.sequence).toBe(1n)
    expect(stream.isCompleted()).toBe(true)
  })

  it("close() cancels the call while the buffer is full, and reports no error", async () => {
    const { store, state } = storeOver(10_000)
    const stream = store.open({ position: 0n, readAhead: 5 })
    await settle()
    const held = state.sent
    stream.close()
    await settle()
    expect(state.cancelled).toBe(true)
    expect(state.sent).toBe(held)
    expect(stream.error()).toBeUndefined()
  })

  it("refuses a readAhead that is not a positive integer", () => {
    const { store } = storeOver(1)
    expect(() => store.open({ position: 0n, readAhead: 0 })).toThrow(RangeError)
    expect(() => store.open({ position: 0n, readAhead: 1.5 })).toThrow(RangeError)
  })
})
