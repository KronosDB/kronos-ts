import type { EventMessage } from "./message.js"

/**
 * Publish-only abstraction for event publication.
 *
 * This is the messaging-level contract for publishing events. The EventGateway
 * and command handlers use this to emit events without needing to know about
 * event storage.
 *
 * Implementations include:
 * - The EventStore (which persists AND publishes)
 * - A simple EventBus (which only distributes to subscribers)
 *
 * Aligned with AF5's `EventSink`.
 */
export interface EventSink {
  /**
   * Publish events. The events are distributed to any subscribed handlers.
   *
   * In an event sourcing context, this also persists the events to the
   * underlying storage engine.
   */
  publish(events: ReadonlyArray<EventMessage>): Promise<void>
}
