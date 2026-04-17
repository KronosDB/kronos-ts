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
  createEventStoreTransaction,
} from "./event-store-transaction.js"

export {
  createInMemoryEventStore,
  AppendConditionError,
} from "./in-memory-event-store.js"

export { type EventStorageEngine, type AppendTransaction } from "./event-storage-engine.js"

export {
  type TagResolver,
  descriptorBasedTagResolver,
  metadataBasedTagResolver,
  multiTagResolver,
} from "./tag-resolver.js"

export { createEventSourcedRepository } from "./event-sourced-repository.js"
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
  createInMemorySnapshotStore,
} from "./snapshot-store.js"

export { createInterceptingEventStore } from "./intercepting-event-store.js"

export {
  MessagingConfigurer,
  ModellingConfigurer,
  EventSourcingConfigurer,
  type KronosApplication,
} from "./eventsourcing-configurer.js"

export {
  type KronosPlugin,
  type TrackingProcessorOptions,
  type SubscribingProcessorOptions,
  Kronos,
  kronos,
} from "./kronos.js"
