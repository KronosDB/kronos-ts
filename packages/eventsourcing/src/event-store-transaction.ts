import type { EventMessage } from "@kronos-ts/messaging"
import type { ProcessingContext } from "@kronos-ts/messaging"

/**
 * A transaction scope for event store operations within a UnitOfWork.
 *
 * Events are buffered during the transaction and only persisted when
 * the UnitOfWork commits. The `onAppend` hook enables entity cache
 * updates as events are buffered (before persistence).
 *
 * This is the TypeScript equivalent of AF5's EventStoreTransaction.
 */
export interface EventStoreTransaction {
  /**
   * Buffer an event for append. The event is not yet persisted —
   * it will be written to the store at PREPARE_COMMIT.
   */
  appendEvent(event: EventMessage): void

  /**
   * Register a callback invoked each time an event is buffered via `appendEvent`.
   * Used by the entity cache to apply events to cached entities immediately,
   * keeping the cache consistent within the same UnitOfWork.
   */
  onAppend(callback: (event: EventMessage) => void): void

  /**
   * Get all buffered events (not yet committed).
   */
  readonly bufferedEvents: ReadonlyArray<EventMessage>
}

/**
 * Creates an EventStoreTransaction that buffers events and notifies
 * registered callbacks.
 */
export function createEventStoreTransaction(): EventStoreTransaction {
  const events: EventMessage[] = []
  const appendCallbacks: Array<(event: EventMessage) => void> = []

  return {
    appendEvent(event: EventMessage): void {
      events.push(event)
      for (const callback of appendCallbacks) {
        try {
          callback(event)
        } catch (e) {
          console.warn("EventStoreTransaction: onAppend callback threw an exception:", e)
        }
      }
    },

    onAppend(callback: (event: EventMessage) => void): void {
      appendCallbacks.push(callback)
    },

    get bufferedEvents(): ReadonlyArray<EventMessage> {
      return events
    },
  }
}
