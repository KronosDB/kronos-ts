import type { EventMessage } from "../messages/message.js"
import type { EventBus } from "../buses/event-bus.js"
import type { EventStorageEngine } from "./event-storage-engine.js"
import type { ConsistencyMarker } from "./consistency-marker.js"

/**
 * Result of sourcing events — the events plus a consistency marker
 * representing the position up to which events were read.
 */
export interface SourcingResult {
  readonly events: ReadonlyArray<EventMessage>
  readonly marker: ConsistencyMarker
}

/**
 * The event store — dual-role component that combines event storage
 * with event distribution.
 *
 * Extends:
 * - `EventStorageEngine` — raw storage (source, append, stream)
 * - `EventBus` — event publication + push-based subscription
 *
 * In an event sourcing context, the EventStore persists events durably while
 * simultaneously distributing them to subscribed event handlers, eliminating
 * the need for a separate EventBus component.
 */
export interface EventStore extends EventStorageEngine, EventBus {}
