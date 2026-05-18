import type { EventMessage, StreamableEventSource } from "@kronos-ts/messaging"
import type { SourcingCondition } from "./sourcing-condition.js"
import type { SourcingResult } from "./event-store.js"
import type { AppendCondition } from "./append-condition.js"
import type { ConsistencyMarker } from "./consistency-marker.js"

/**
 * A transactional handle for an append operation.
 *
 * The two-phase pattern allows the processing lifecycle to control when
 * events become visible:
 *
 * 1. {@link appendEvents} stages events and returns an AppendTransaction
 * 2. {@link commit} makes events visible to consumers
 * 3. {@link afterCommit} returns the consistency marker
 * 4. {@link rollback} discards staged events on failure
 */
export interface AppendTransaction {
  /** Make appended events visible to consumers. */
  commit(): Promise<void>
  /** Get the consistency marker after a successful commit. */
  afterCommit(): Promise<ConsistencyMarker>
  /** Discard staged events. */
  rollback(): void
}

/**
 * Infrastructure-level abstraction for event persistence.
 *
 * This is the raw storage mechanism — append, source, and stream events.
 * Database extensions (drizzle, knex, prisma, etc.) implement this interface
 * to provide persistent event storage.
 *
 * Not intended for direct use by application code. The {@link EventStore}
 * composes an EventStorageEngine with event distribution (EventSink) and
 * tag resolution (TagResolver).
 */
export interface EventStorageEngine extends StreamableEventSource {
  /**
   * Source events matching the given condition (criteria-based, for state sourcing).
   * Returns the matching events and a consistency marker.
   */
  source(condition: SourcingCondition): Promise<SourcingResult>

  /**
   * Append events to the store.
   * If an append condition is provided, the engine verifies that no conflicting
   * events were written since the marker before appending.
   *
   * Returns an {@link AppendTransaction} for two-phase commit control.
   * For simple cases, use the convenience form that auto-commits:
   * ```typescript
   * const marker = await store.append(events, condition)
   * ```
   */
  appendEvents(
    events: ReadonlyArray<EventMessage>,
    condition?: AppendCondition,
  ): Promise<AppendTransaction>

  /**
   * Convenience method that appends events and auto-commits in one step.
   * Equivalent to calling appendEvents() followed by commit() and afterCommit().
   */
  append(
    events: ReadonlyArray<EventMessage>,
    condition?: AppendCondition,
  ): Promise<ConsistencyMarker>
}
