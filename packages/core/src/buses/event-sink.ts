import type { EventMessage } from "../messages/message.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * Publish-only abstraction for event publication.
 *
 * This is the messaging-level contract for publishing events. A publisher
 * and command handlers use this to emit events without needing to know about
 * event storage.
 *
 * Implementations include:
 * - The EventStore (which persists AND publishes)
 * - A simple EventBus (which only distributes to subscribers)
 *
 */
export interface EventSink {
  /**
   * Publish events. The events are distributed to any subscribed handlers.
   *
   * In an event sourcing context, this also persists the events to the
   * underlying storage engine — pass the unit of work so the write joins its
   * transaction and subscriber fan-out is deferred to AFTER_COMMIT. Without
   * one the implementation opens its own short-lived transaction.
   */
  publish(events: ReadonlyArray<EventMessage>, uow?: UnitOfWork): Promise<void>
}
