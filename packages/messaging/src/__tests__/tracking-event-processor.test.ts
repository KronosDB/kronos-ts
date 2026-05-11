import { describe, expect, it, beforeEach } from "bun:test"
import { qn, emptyMetadata, type QualifiedName } from "@kronos-ts/common"
import {
  createTrackingEventProcessor,
  loggingErrorHandler,
  propagatingErrorHandler,
  type TrackingEventProcessorOptions,
} from "../tracking-event-processor.js"
import type { StreamableEventSource, SequencedEvent, MessageStream } from "../event-source.js"
import type { EventMessage } from "../message.js"
import type { EventHandlerRegistration } from "../handler.js"
import type { TokenStore } from "../token-store.js"
import type { TrackingToken } from "../tracking-token.js"
import { globalSequenceToken } from "../tracking-token.js"
import { isReplay, REPLAY_STATE_KEY } from "../replay-token.js"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeEvent(name: QualifiedName, payload: unknown, sequence: bigint): SequencedEvent {
  return {
    sequence,
    event: {
      identifier: `evt-${sequence}`,
      name,
      version: "1.0",
      payload,
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [],
    },
  }
}

function createInMemoryEventSource(events: SequencedEvent[]): StreamableEventSource {
  const listeners = new Set<() => void>()

  return {
    open(condition) {
      let cursor = condition.position
      let callback: (() => void) | null = null

      const listener = () => {
        if (callback) callback()
      }
      listeners.add(listener)

      return {
        next() {
          const found = events.find((e) => e.sequence >= cursor)
          if (!found) return undefined
          cursor = found.sequence + 1n
          return found
        },
        peek() {
          return events.find((e) => e.sequence >= cursor)
        },
        hasNextAvailable() {
          return events.some((e) => e.sequence >= cursor)
        },
        isCompleted() { return false },
        error() { return undefined },
        setCallback(cb) { callback = cb },
        close() {
          callback = null
          listeners.delete(listener)
        },
      }
    },
    async getHeadPosition() {
      if (events.length === 0) return 0n
      return events[events.length - 1]!.sequence + 1n
    },
  }
}

function createRecordingTokenStore(): TokenStore & { stored: Array<{ name: string; segment: number; token: TrackingToken }> } {
  const tokens = new Map<string, TrackingToken>()
  const stored: Array<{ name: string; segment: number; token: TrackingToken }> = []

  return {
    stored,
    async store(processorName, segment, token) {
      tokens.set(`${processorName}:${segment}`, token)
      stored.push({ name: processorName, segment, token })
    },
    async get(processorName, segment) {
      return tokens.get(`${processorName}:${segment}`)
    },
    async initializeSegments() {},
    async claimToken() { return undefined },
    async extendClaim() {},
    async releaseClaim() {},
    async fetchSegments() { return [0] },
    async fetchAvailableSegments() { return [0] },
    async deleteToken() {},
  }
}

const TEST_EVENT_NAME = qn("test", "SomethingHappened")

/** Wait for the processor to advance past a position, with a timeout. */
async function waitForPosition(
  proc: { position: bigint; running: boolean },
  target: bigint,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (proc.position < target && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 10))
  }
  if (proc.position < target) {
    throw new Error(`Processor did not reach position ${target} within ${timeoutMs}ms (at ${proc.position})`)
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("TrackingEventProcessor", () => {
  describe("event delivery", () => {
    it("reads events from the event source and delivers to handlers", async () => {
      // given
      const delivered: unknown[] = []
      const events = [
        makeEvent(TEST_EVENT_NAME, { value: 1 }, 0n),
        makeEvent(TEST_EVENT_NAME, { value: 2 }, 1n),
        makeEvent(TEST_EVENT_NAME, { value: 3 }, 2n),
      ]
      const eventSource = createInMemoryEventSource(events)

      const handler: EventHandlerRegistration<any> = {
        kind: "event-handler",
        descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
        handler: (payload) => { delivered.push(payload) },
      }

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [handler],

        pollingIntervalMs: 10,
      })

      // when
      await processor.start()
      await waitForPosition(processor, 3n)
      processor.stop()

      // then
      expect(delivered).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }])
    })

    it("ignores events with no matching handler", async () => {
      // given
      const delivered: unknown[] = []
      const otherName = qn("test", "UnhandledEvent")
      const events = [
        makeEvent(otherName, { value: 1 }, 0n),
        makeEvent(TEST_EVENT_NAME, { value: 2 }, 1n),
      ]
      const eventSource = createInMemoryEventSource(events)

      const handler: EventHandlerRegistration<any> = {
        kind: "event-handler",
        descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
        handler: (payload) => { delivered.push(payload) },
      }

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [handler],

        pollingIntervalMs: 10,
      })

      // when
      await processor.start()
      await waitForPosition(processor, 2n)
      processor.stop()

      // then
      expect(delivered).toEqual([{ value: 2 }])
    })
  })

  describe("position tracking", () => {
    it("advances position after each batch", async () => {
      // given
      const events = [
        makeEvent(TEST_EVENT_NAME, {}, 0n),
        makeEvent(TEST_EVENT_NAME, {}, 1n),
        makeEvent(TEST_EVENT_NAME, {}, 2n),
      ]
      const eventSource = createInMemoryEventSource(events)

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [{
          kind: "event-handler",
          descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
          handler: () => {},
        }],

        pollingIntervalMs: 10,
      })

      // when
      expect(processor.position).toBe(0n) // starts at 0
      await processor.start()
      await waitForPosition(processor, 3n)
      processor.stop()

      // then
      expect(processor.position).toBe(3n)
    })
  })

  describe("token store integration", () => {
    it("stores token at PREPARE_COMMIT when token store is configured", async () => {
      // given
      const tokenStore = createRecordingTokenStore()
      const events = [
        makeEvent(TEST_EVENT_NAME, {}, 0n),
        makeEvent(TEST_EVENT_NAME, {}, 1n),
      ]
      const eventSource = createInMemoryEventSource(events)

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [{
          kind: "event-handler",
          descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
          handler: () => {},
        }],

        tokenStore,
        pollingIntervalMs: 10,
      })

      // when
      await processor.start()
      await waitForPosition(processor, 2n)
      processor.stop()

      // then
      expect(tokenStore.stored.length).toBeGreaterThanOrEqual(1)
      const lastStored = tokenStore.stored[tokenStore.stored.length - 1]!
      expect(lastStored.token.position()).toBe(2n)
      expect(lastStored.name).toBe("test-processor")
    })

    it("resumes from stored position on restart", async () => {
      // given
      const tokenStore = createRecordingTokenStore()
      // Pre-seed token at position 2
      await tokenStore.store("test-processor", 0, globalSequenceToken(2n))

      const delivered: bigint[] = []
      const events = [
        makeEvent(TEST_EVENT_NAME, {}, 0n),
        makeEvent(TEST_EVENT_NAME, {}, 1n),
        makeEvent(TEST_EVENT_NAME, {}, 2n),
        makeEvent(TEST_EVENT_NAME, {}, 3n),
      ]
      const eventSource = createInMemoryEventSource(events)

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [{
          kind: "event-handler",
          descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
          handler: (_payload, _metadata) => {
            delivered.push(BigInt(delivered.length))
          },
        }],

        tokenStore,
        pollingIntervalMs: 10,
      })

      // when
      await processor.start()
      await waitForPosition(processor, 4n)
      processor.stop()

      // then -- should only deliver events at position 2 and 3 (skipped 0, 1)
      expect(delivered).toHaveLength(2)
      expect(processor.position).toBe(4n)
    })
  })

  describe("error handling", () => {
    it("logging error handler logs and continues processing", async () => {
      // given
      const delivered: unknown[] = []
      const events = [
        makeEvent(TEST_EVENT_NAME, { value: 1 }, 0n),
        makeEvent(TEST_EVENT_NAME, { value: 2 }, 1n),
        makeEvent(TEST_EVENT_NAME, { value: 3 }, 2n),
      ]
      const eventSource = createInMemoryEventSource(events)

      let callCount = 0
      const handler: EventHandlerRegistration<any> = {
        kind: "event-handler",
        descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
        handler: (payload) => {
          callCount++
          if (callCount === 2) throw new Error("handler failed")
          delivered.push(payload)
        },
      }

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [handler],

        errorHandler: loggingErrorHandler("test-processor"),
        pollingIntervalMs: 10,
      })

      // when
      await processor.start()
      await waitForPosition(processor, 3n)
      processor.stop()

      // then -- first and third events delivered, second failed but continued
      expect(delivered).toEqual([{ value: 1 }, { value: 3 }])
      expect(processor.position).toBe(3n)
    })

    it("propagating error handler aborts the batch", async () => {
      // given
      const delivered: unknown[] = []
      const events = [
        makeEvent(TEST_EVENT_NAME, { value: 1 }, 0n),
        makeEvent(TEST_EVENT_NAME, { value: 2 }, 1n),
      ]
      const eventSource = createInMemoryEventSource(events)

      const handler: EventHandlerRegistration<any> = {
        kind: "event-handler",
        descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
        handler: (payload) => {
          delivered.push(payload)
          throw new Error("handler failed")
        },
      }

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [handler],

        errorHandler: propagatingErrorHandler(),
        pollingIntervalMs: 10,
      })

      // when
      await processor.start()
      await new Promise((r) => setTimeout(r, 200))
      processor.stop()

      // then -- position should NOT advance past the failing event
      expect(processor.position).toBe(0n)
    })
  })

  describe("stop and restart", () => {
    it("can be stopped and restarted", async () => {
      // given
      const delivered: unknown[] = []
      const events = [
        makeEvent(TEST_EVENT_NAME, { value: 1 }, 0n),
        makeEvent(TEST_EVENT_NAME, { value: 2 }, 1n),
      ]
      const mutableEvents = [...events]

      const listeners = new Set<() => void>()
      const eventSource: StreamableEventSource = {
        open(condition) {
          let cursor = condition.position
          let cb: (() => void) | null = null
          const listener = () => { if (cb) cb() }
          listeners.add(listener)
          return {
            next() {
              const found = mutableEvents.find((e) => e.sequence >= cursor)
              if (!found) return undefined
              cursor = found.sequence + 1n
              return found
            },
            peek() { return mutableEvents.find((e) => e.sequence >= cursor) },
            hasNextAvailable() { return mutableEvents.some((e) => e.sequence >= cursor) },
            isCompleted() { return false },
            error() { return undefined },
            setCallback(callback) { cb = callback },
            close() { cb = null; listeners.delete(listener) },
          }
        },
        async getHeadPosition() {
          if (mutableEvents.length === 0) return 0n
          return mutableEvents[mutableEvents.length - 1]!.sequence + 1n
        },
      }

      const handler: EventHandlerRegistration<any> = {
        kind: "event-handler",
        descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
        handler: (payload) => { delivered.push(payload) },
      }

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [handler],

        pollingIntervalMs: 10,
      })

      // when -- first run
      await processor.start()
      await waitForPosition(processor, 2n)
      processor.stop()
      expect(processor.running).toBe(false)

      // Add more events
      mutableEvents.push(makeEvent(TEST_EVENT_NAME, { value: 3 }, 2n))

      // when -- restart
      await processor.start()
      expect(processor.running).toBe(true)
      await waitForPosition(processor, 3n)
      processor.stop()

      // then
      expect(delivered).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }])
    })
  })

  describe("reset and replay", () => {
    it("reset creates a ReplayToken and calls onReset handlers", async () => {
      // given
      let resetCalled = false
      const events = [
        makeEvent(TEST_EVENT_NAME, {}, 0n),
        makeEvent(TEST_EVENT_NAME, {}, 1n),
        makeEvent(TEST_EVENT_NAME, {}, 2n),
      ]
      const tokenStore = createRecordingTokenStore()
      const eventSource = createInMemoryEventSource(events)

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [{
          kind: "event-handler",
          descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
          handler: () => {},
        }],
        onReset: () => { resetCalled = true },

        tokenStore,
        pollingIntervalMs: 10,
      })

      // when -- process all events, then stop and reset
      await processor.start()
      await waitForPosition(processor, 3n)
      processor.stop()

      await processor.resetTokens()

      // then
      expect(resetCalled).toBe(true)
      expect(processor.replaying).toBe(true)
      expect(processor.position).toBe(0n)
      const lastStored = tokenStore.stored[tokenStore.stored.length - 1]!
      expect(lastStored.token.kind).toBe("replay")
    })

    it("throws when resetting while running", async () => {
      // given
      const eventSource = createInMemoryEventSource([])
      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [],

        pollingIntervalMs: 10,
      })

      await processor.start()

      // when / then
      await expect(processor.resetTokens()).rejects.toThrow("must be stopped before resetting")

      processor.stop()
    })

    it("handlers see isReplaying=true during replay", async () => {
      // given
      const replayStates: boolean[] = []
      const events = [
        makeEvent(TEST_EVENT_NAME, { value: 1 }, 0n),
        makeEvent(TEST_EVENT_NAME, { value: 2 }, 1n),
        makeEvent(TEST_EVENT_NAME, { value: 3 }, 2n),
      ]
      const eventSource = createInMemoryEventSource(events)

      const handler: EventHandlerRegistration<any> = {
        kind: "event-handler",
        descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
        handler: (_payload) => {
          replayStates.push(isReplay())
        },
      }

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [handler],

        pollingIntervalMs: 10,
      })

      // First pass
      await processor.start()
      await waitForPosition(processor, 3n)
      processor.stop()

      // Reset and replay
      replayStates.length = 0
      await processor.resetTokens()
      await processor.start()
      await waitForPosition(processor, 3n)
      processor.stop()

      // then
      expect(replayStates).toHaveLength(3)
      expect(replayStates.every((s) => s === true)).toBe(true)
    })

    it("replay completes when position passes the reset point", async () => {
      // given
      const events = [
        makeEvent(TEST_EVENT_NAME, {}, 0n),
        makeEvent(TEST_EVENT_NAME, {}, 1n),
        makeEvent(TEST_EVENT_NAME, {}, 2n),
      ]
      const eventSource = createInMemoryEventSource(events)

      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [{
          kind: "event-handler",
          descriptor: { kind: "event", name: TEST_EVENT_NAME, version: "1.0", payload: {} as any },
          handler: () => {},
        }],

        pollingIntervalMs: 10,
      })

      // Process all events
      await processor.start()
      await waitForPosition(processor, 3n)
      processor.stop()

      // Reset to beginning
      await processor.resetTokens()
      expect(processor.replaying).toBe(true)

      // Replay
      await processor.start()
      await waitForPosition(processor, 3n)
      await new Promise((r) => setTimeout(r, 100))
      processor.stop()

      // then
      expect(processor.replaying).toBe(false)
      expect(processor.position).toBe(3n)
    })
  })

  describe("processor properties", () => {
    it("exposes name and running state", async () => {
      // given
      const eventSource = createInMemoryEventSource([])
      const processor = createTrackingEventProcessor({
        name: "my-processor",
        eventSource,
        eventHandlers: [],

      })

      // then
      expect(processor.name).toBe("my-processor")
      expect(processor.running).toBe(false)

      await processor.start()
      expect(processor.running).toBe(true)

      processor.stop()
      expect(processor.running).toBe(false)
    })

    it("is not replaying when freshly created", () => {
      // given
      const eventSource = createInMemoryEventSource([])
      const processor = createTrackingEventProcessor({
        name: "test-processor",
        eventSource,
        eventHandlers: [],

      })

      // then
      expect(processor.replaying).toBe(false)
    })
  })
})
