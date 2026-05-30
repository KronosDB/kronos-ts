import { describe, expect, it, afterEach } from "bun:test"
import { qn, generateIdentifier, emptyMetadata } from "@kronos-ts/common"
import type { EventMessage } from "../message.js"
import type { EventHandlerRegistration } from "../handler.js"
import type { SubscribableEventSource } from "../subscribing-event-processor.js"
import { createSubscribingEventProcessor } from "../subscribing-event-processor.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function eventMsg(name: string, payload: unknown = {}): EventMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    version: "1.0",
    tags: [],
  }
}

function createInMemorySubscribableSource(): SubscribableEventSource & {
  publish(events: ReadonlyArray<EventMessage>): Promise<void>
} {
  const subscribers = new Set<(events: ReadonlyArray<EventMessage>) => Promise<void>>()

  return {
    subscribe(handler) {
      subscribers.add(handler)
      return () => { subscribers.delete(handler) }
    },
    async publish(events) {
      for (const subscriber of subscribers) {
        await subscriber(events)
      }
    },
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SubscribingEventProcessor", () => {
  let processor: ReturnType<typeof createSubscribingEventProcessor>

  afterEach(() => {
    if (processor?.running) processor.stop()
  })

  describe("lifecycle", () => {
    it("starts and stops", () => {
      // given
      const source = createInMemorySubscribableSource()
      processor = createSubscribingEventProcessor({
        name: "test-sub",
        eventSource: source,
        eventHandlers: [],

      })

      // when / then
      expect(processor.running).toBe(false)
      processor.start()
      expect(processor.running).toBe(true)
      processor.stop()
      expect(processor.running).toBe(false)
    })

    it("has the configured name", () => {
      // given
      const source = createInMemorySubscribableSource()
      processor = createSubscribingEventProcessor({
        name: "my-projection",
        eventSource: source,
        eventHandlers: [],

      })

      // then
      expect(processor.name).toBe("my-projection")
    })

    it("does not support reset", () => {
      // given
      const source = createInMemorySubscribableSource()
      processor = createSubscribingEventProcessor({
        name: "test-sub",
        eventSource: source,
        eventHandlers: [],

      })

      // then
      expect(processor.supportsReset()).toBe(false)
    })

    it("start is idempotent", () => {
      // given
      const source = createInMemorySubscribableSource()
      processor = createSubscribingEventProcessor({
        name: "test-sub",
        eventSource: source,
        eventHandlers: [],

      })

      // when
      processor.start()
      processor.start() // no error

      // then
      expect(processor.running).toBe(true)
    })
  })

  describe("event delivery", () => {
    it("delivers events to matching handlers", async () => {
      // given
      const source = createInMemorySubscribableSource()
      const received: unknown[] = []

      const handler: EventHandlerRegistration<any> = {
        descriptor: { kind: "event", name: qn("test", "SomethingHappened"), version: "1.0", payload: {} as any },
        handler: async ({ payload }) => { received.push(payload) },
      }

      processor = createSubscribingEventProcessor({
        name: "test-sub",
        eventSource: source,
        eventHandlers: [handler],

      })
      processor.start()

      // when
      await source.publish([eventMsg("SomethingHappened", { data: "hello" })])

      // then
      expect(received).toHaveLength(1)
      expect(received[0]).toEqual({ data: "hello" })
    })

    it("ignores events with no matching handler", async () => {
      // given
      const source = createInMemorySubscribableSource()
      const received: unknown[] = []

      const handler: EventHandlerRegistration<any> = {
        descriptor: { kind: "event", name: qn("test", "SomethingHappened"), version: "1.0", payload: {} as any },
        handler: async ({ payload }) => { received.push(payload) },
      }

      processor = createSubscribingEventProcessor({
        name: "test-sub",
        eventSource: source,
        eventHandlers: [handler],

      })
      processor.start()

      // when
      await source.publish([eventMsg("UnrelatedEvent", { data: "ignored" })])

      // then
      expect(received).toHaveLength(0)
    })

    it("does not deliver events when stopped", async () => {
      // given
      const source = createInMemorySubscribableSource()
      const received: unknown[] = []

      const handler: EventHandlerRegistration<any> = {
        descriptor: { kind: "event", name: qn("test", "SomethingHappened"), version: "1.0", payload: {} as any },
        handler: async ({ payload }) => { received.push(payload) },
      }

      processor = createSubscribingEventProcessor({
        name: "test-sub",
        eventSource: source,
        eventHandlers: [handler],

      })
      processor.start()
      processor.stop()

      // when
      await source.publish([eventMsg("SomethingHappened", { data: "missed" })])

      // then
      expect(received).toHaveLength(0)
    })

    it("delivers batch of events in order", async () => {
      // given
      const source = createInMemorySubscribableSource()
      const received: string[] = []

      const handler: EventHandlerRegistration<any> = {
        descriptor: { kind: "event", name: qn("test", "ItemAdded"), version: "1.0", payload: {} as any },
        handler: async ({ payload }: any) => { received.push(payload.item) },
      }

      processor = createSubscribingEventProcessor({
        name: "test-sub",
        eventSource: source,
        eventHandlers: [handler],

      })
      processor.start()

      // when
      await source.publish([
        eventMsg("ItemAdded", { item: "first" }),
        eventMsg("ItemAdded", { item: "second" }),
        eventMsg("ItemAdded", { item: "third" }),
      ])

      // then
      expect(received).toEqual(["first", "second", "third"])
    })
  })

  describe("error handling", () => {
    it("uses error handler when handler throws", async () => {
      // given
      const source = createInMemorySubscribableSource()
      const errors: unknown[] = []

      const handler: EventHandlerRegistration<any> = {
        descriptor: { kind: "event", name: qn("test", "BadEvent"), version: "1.0", payload: {} as any },
        handler: async () => { throw new Error("handler failed") },
      }

      processor = createSubscribingEventProcessor({
        name: "test-sub",
        eventSource: source,
        eventHandlers: [handler],

        errorHandler: {
          handleError(error) { errors.push(error) },
        },
      })
      processor.start()

      // when
      await source.publish([eventMsg("BadEvent")])

      // then
      expect(errors).toHaveLength(1)
      expect((errors[0] as Error).message).toBe("handler failed")
    })
  })
})
