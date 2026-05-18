import type { EventMessage } from "./message.js"
import type { EventSink } from "./event-sink.js"

/**
 * Subscribable source of events — push-based delivery.
 */
export interface SubscribableEventSource {
  /**
   * Subscribe to events as they are published.
   * Returns an unsubscribe function.
   *
   * @param handler Called with each batch of events as they are appended.
   */
  subscribe(handler: (events: ReadonlyArray<EventMessage>) => Promise<void>): () => void
}

/**
 * The event bus — combines event publication (EventSink) with
 * push-based subscription (SubscribableEventSource).
 *
 * In event sourcing setups, the EventStore serves as the EventBus.
 * In non-event-sourcing setups, SimpleEventBus provides in-memory distribution.
 */
export interface EventBus extends SubscribableEventSource, EventSink {}

/**
 * In-memory event bus for non-event-sourcing scenarios.
 * Publishes events directly to all subscribers.
 */
export function createSimpleEventBus(): EventBus {
  const subscribers = new Set<(events: ReadonlyArray<EventMessage>) => Promise<void>>()

  return {
    async publish(events) {
      for (const subscriber of subscribers) {
        try { await subscriber(events) } catch { /* ignore subscriber errors */ }
      }
    },

    subscribe(handler) {
      subscribers.add(handler)
      return () => { subscribers.delete(handler) }
    },
  }
}
