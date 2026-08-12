export {
  type ConsistencyMarker,
  ORIGIN,
  INFINITY,
  MARKER_RESOURCE_KEY,
  noMarker,
  markerAt,
  markerLowerBound,
  markerUpperBound,
} from "./consistency-marker.js"

export {
  type SourcingCondition,
  sourcingCondition,
} from "./sourcing-condition.js"

export {
  type AppendCondition,
  appendCondition,
} from "./append-condition.js"

export {
  type EventStore,
  type SourcingResult,
} from "./event-store.js"

export {
  type EventStoreTransaction,
  eventStoreTransaction,
} from "./event-store-transaction.js"

export {
  inMemoryEventStore,
  AppendConditionError,
} from "./in-memory-event-store.js"

export { type EventStorageEngine, type AppendTransaction } from "./event-storage-engine.js"

export {
  type TagResolver,
  descriptorBasedTagResolver,
  metadataBasedTagResolver,
  multiTagResolver,
} from "./tag-resolver.js"

export { eventSourcedRepository } from "./event-sourced-repository.js"
export type { EventSourcedRepositoryOptions } from "./event-sourced-repository.js"

export {
  type SnapshotPolicy,
  type EvolutionResult,
  afterEvents,
  whenSourcingTimeExceeds,
  noSnapshotPolicy,
} from "./snapshot-policy.js"

export {
  type Snapshot,
  type SnapshotStore,
  inMemorySnapshotStore,
} from "./snapshot-store.js"

export { interceptingEventStore } from "./intercepting-event-store.js"

// Handler capabilities are reached via the HandlerContext (second handler
// argument in @kronos-ts/messaging). The implementations stay in this package
// as internal subpath exports ("./append", "./load", "./schedule") consumed
// by the context — only the ALS resource keys and types are public here.
export { STATE_MANAGER_KEY } from "./load.js"
export {
  EVENT_SCHEDULER_KEY,
  type ScheduleFunction,
  type ScheduleAfterFunction,
} from "./schedule.js"
export type { EventList } from "./append.js"
export {
  BUFFERED_EVENTS_KEY,
  SOURCING_INFOS_KEY,
  STATE_CACHE_KEY,
  STATE_MODULES_KEY,
} from "./append.js"
