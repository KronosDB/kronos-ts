import type { EventMessage } from "../messages/message.js"
import type { EventCriteria, TagCriteria, TypeRestrictedCriteria, EitherCriteria } from "../query/event-query.js"
import type { MessageStream, SequencedEvent, StreamingCondition } from "../processor/event-source.js"
import type { TrackingToken } from "../processor/tracking-token.js"
import { messageStream } from "../processor/event-source.js"
import { globalSequenceToken, FIRST_TOKEN } from "../processor/tracking-token.js"
import { compileQuery } from "../query/event-query.js"
import { qualifiedNameToString } from "../primitives/qualified-name.js"
import type { EventStore, SourcingResult } from "./event-store.js"
import type { AppendTransaction } from "./event-storage-engine.js"
import type { AppendCondition } from "./append-condition.js"
import type { SourcingCondition } from "./sourcing-condition.js"
import { markerAt } from "./consistency-marker.js"
import type { ConsistencyMarker } from "./consistency-marker.js"

/**
 * In-memory event store for testing and standalone usage.
 * Events are stored in an ordered array with a global sequence position.
 * Supports push-based streaming via open().
 */
export function inMemoryEventStore(): EventStore {
  const events: Array<{ position: bigint; event: EventMessage }> = []
  let nextPosition = 0n

  // Registered stream listeners — notified when events are appended
  const streamListeners = new Set<() => void>()

  // Push-based subscribers — notified with actual events on append
  const eventSubscribers = new Set<(events: ReadonlyArray<EventMessage>) => Promise<void>>()

  function matchesCriteria(event: EventMessage, criteria: EventCriteria): boolean {
    switch (criteria.kind) {
      case "tags":
        return matchesTags(event, criteria)
      case "any-tag":
        return event.tags.length > 0
      case "type-restricted":
        return matchesTypeRestricted(event, criteria)
      case "either":
        return matchesEither(event, criteria)
    }
  }

  function matchesTags(event: EventMessage, criteria: TagCriteria): boolean {
    return criteria.tags.every((requiredTag) =>
      event.tags.some(
        (eventTag) =>
          eventTag.key === requiredTag.key && eventTag.value === requiredTag.value,
      ),
    )
  }

  function matchesTypeRestricted(event: EventMessage, criteria: TypeRestrictedCriteria): boolean {
    if (!matchesCriteria(event, criteria.inner)) return false
    const eventType = qualifiedNameToString(event.name)
    return criteria.types.includes(eventType)
  }

  function matchesEither(event: EventMessage, criteria: EitherCriteria): boolean {
    return criteria.criteria.some((c) => matchesCriteria(event, c))
  }

  function notifyListeners() {
    for (const listener of streamListeners) {
      try { listener() } catch { /* ignore */ }
    }
  }

  return {
    async source(condition: SourcingCondition): Promise<SourcingResult> {
      const start = condition.start ?? 0n
      const criteria = compileQuery(condition.query)
      const matching = events
        .filter((entry) => entry.position >= start)
        .filter((entry) => matchesCriteria(entry.event, criteria))

      const lastPosition = matching.length > 0
        ? matching[matching.length - 1]!.position
        : start > 0n ? start - 1n : -1n

      const globalMarker = events.length > 0
        ? events[events.length - 1]!.position
        : -1n

      return {
        events: matching.map((e) => e.event),
        marker: markerAt(globalMarker),
      }
    },

    async appendEvents(
      newEvents: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<AppendTransaction> {
      if (condition) {
        const criteria = compileQuery(condition.query)
        const conflicting = events
          .filter((entry) => entry.position > condition.marker.position)
          .filter((entry) => matchesCriteria(entry.event, criteria))

        if (conflicting.length > 0) {
          throw new AppendConditionError(
            `Append condition violated: ${conflicting.length} conflicting event(s) ` +
            `found after position ${condition.marker.position}`,
          )
        }
      }

      // Stage events — they're added to the store but we track the range
      const startPosition = nextPosition
      for (const event of newEvents) {
        events.push({ position: nextPosition, event })
        nextPosition++
      }
      const endPosition = nextPosition - 1n
      let committed = false

      return {
        async commit() {
          committed = true
          // Events are already in the array — notify listeners
          notifyListeners()
          for (const subscriber of eventSubscribers) {
            try { await subscriber(newEvents) } catch { /* ignore */ }
          }
        },
        async afterCommit() {
          return markerAt(endPosition)
        },
        rollback() {
          if (!committed) {
            // Remove staged events
            while (events.length > 0 && events[events.length - 1]!.position >= startPosition) {
              events.pop()
            }
            nextPosition = startPosition
          }
        },
      }
    },

    async append(
      newEvents: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<ConsistencyMarker> {
      if (condition) {
        const criteria = compileQuery(condition.query)
        const conflicting = events
          .filter((entry) => entry.position > condition.marker.position)
          .filter((entry) => matchesCriteria(entry.event, criteria))

        if (conflicting.length > 0) {
          throw new AppendConditionError(
            `Append condition violated: ${conflicting.length} conflicting event(s) ` +
            `found after position ${condition.marker.position}`,
          )
        }
      }

      for (const event of newEvents) {
        events.push({ position: nextPosition, event })
        nextPosition++
      }

      // Notify open streams that new events are available
      notifyListeners()

      // Notify push-based subscribers with the actual events
      for (const subscriber of eventSubscribers) {
        try { await subscriber(newEvents) } catch { /* ignore subscriber errors */ }
      }

      return markerAt(nextPosition - 1n)
    },

    open(condition: StreamingCondition): MessageStream<SequencedEvent> {
      let cursor = condition.position
      const criteria = condition.query ? compileQuery(condition.query) : undefined
      let availableCallback: (() => void) | null = null
      let closed = false

      const listener = () => {
        if (!closed && availableCallback) {
          availableCallback()
        }
      }

      streamListeners.add(listener)

      function findNext(): SequencedEvent | undefined {
        if (closed) return undefined
        for (const entry of events) {
          if (entry.position < cursor) continue
          if (criteria && !matchesCriteria(entry.event, criteria)) continue
          return { sequence: entry.position, event: entry.event }
        }
        return undefined
      }

      return messageStream<SequencedEvent>({
        next() {
          const item = findNext()
          if (item) {
            cursor = item.sequence + 1n
          }
          return item
        },

        peek() {
          return findNext()
        },

        hasNextAvailable() {
          return findNext() !== undefined
        },

        setCallback(callback: () => void) {
          availableCallback = callback
        },

        isCompleted() {
          return closed
        },

        error() {
          return undefined // In-memory store never errors
        },

        close() {
          closed = true
          availableCallback = null
          streamListeners.delete(listener)
        },
      })
    },

    async getHeadPosition(): Promise<bigint> {
      return nextPosition
    },

    async firstToken(): Promise<TrackingToken> {
      return FIRST_TOKEN
    },

    async latestToken(): Promise<TrackingToken> {
      return globalSequenceToken(nextPosition)
    },

    async publish(publishedEvents: ReadonlyArray<EventMessage>): Promise<void> {
      // In the in-memory store, publish = append without condition
      for (const event of publishedEvents) {
        events.push({ position: nextPosition, event })
        nextPosition++
      }
      notifyListeners()
      for (const subscriber of eventSubscribers) {
        try { await subscriber(publishedEvents) } catch { /* ignore */ }
      }
    },

    subscribe(handler: (events: ReadonlyArray<EventMessage>) => Promise<void>): () => void {
      eventSubscribers.add(handler)
      return () => { eventSubscribers.delete(handler) }
    },
  }
}

/**
 * Thrown when an append condition is violated — optimistic concurrency failure.
 */
export class AppendConditionError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "AppendConditionError"
  }
}
