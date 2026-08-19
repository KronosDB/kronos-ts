import type { EventMessage } from "../messages/message.js"
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
 * Event publication (EventSink) plus push-based subscription
 * (SubscribableEventSource).
 *
 * INTERNAL, and deliberately not on the public surface. There is no event bus
 * in this framework: delivery is tracked-only, through an `eventProcessor` over
 * an `EventStore`, so there is no on-commit lane for an in-memory bus to serve
 * and `simpleEventBus` had no honest caller. What survives is the two-method
 * SHAPE, because `EventStore` genuinely is both — it persists events and it
 * notifies subscribers — and writing `EventStore extends EventStorageEngine,
 * EventBus` is how that gets said once.
 *
 * A host never names this type. It names `EventStore`.
 */
export interface EventBus extends SubscribableEventSource, EventSink {}
