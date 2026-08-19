// ── primitives ─────────────────────────────────────────────────────────────
export {
  type QualifiedName,
  qn,
  qualifiedNameToString,
  qualifiedNameFromString,
  qualifiedNamesEqual,
} from "./primitives/qualified-name.js"

export { type Tag, tag, tagsFromRecord } from "./primitives/tag.js"

export {
  type Metadata,
  MetadataKeys,
  emptyMetadata,
  metadataWith,
  mergeMetadata,
  metadataAnd,
  metadataAndIfNotPresent,
  metadataWithoutKeys,
  metadataSubset,
  metadataContains,
} from "./primitives/metadata.js"

export { generateIdentifier } from "./primitives/identifier.js"

// The task's instant, as one function. `unitOfWork(clock?)` is where it enters.
export { type Clock } from "./primitives/clock.js"

export { type SerializedError } from "./primitives/serialized-error.js"

export {
  type SerializedObject,
  type Serializer,
  type SerializerDecorator,
} from "./primitives/converter.js"

export {
  withRetry,
  healthCheck,
  type ResilienceConfig,
  type RetryEvent,
} from "./primitives/resilience.js"

// ── task lifecycle: THE primitive ──────────────────────────────────────────
// A unit-of-work FACTORY is spelled `() => UnitOfWork` wherever a seam takes
// one. There is no named type for it, because the arrow IS the contract.
export {
  Phase,
  type PhaseValue,
  type PhaseAction,
  type UoWErrorHandler,
  type CompleteHandler,
  type SourcingInfo,
  type UoWEventBuffer,
  type UoWStateCache,
  type UnitOfWork,
  unitOfWork,
  requireInvocation,
  requireLive,
  NoActiveUnitOfWork,
  WrongUoWPhase,
} from "./unit-of-work/unit-of-work.js"

// ── messages & descriptors ─────────────────────────────────────────────────
export {
  type Message,
  type MessageKind,
  type CommandMessage,
  type CommandResultMessage,
  type EventMessage,
  type SequencedEventMessage,
  type QueryMessage,
  type Unstamped,
  stamped,
} from "./messages/message.js"

export {
  type CommandDescriptor,
  type EventDescriptor,
  type QueryDescriptor,
  type MessageDescriptor,
  type TagExtractors,
  type InferResult,
  command,
  event,
} from "./messages/descriptor.js"

export { withNamespace } from "./messages/with-namespace.js"

// Serialization + upcasting — the wire, for adapters that own one.
export {
  type SchemaRegistry,
  jsonSerializer,
  zodValidatingSerializer,
  eventSchemaRegistry,
  commandSchemaRegistry,
  querySchemaRegistry,
} from "./messages/serializer.js"

export {
  type IntermediateEventRepresentation,
  type EventUpcaster,
  singleEventUpcaster,
  upcasterChain,
  upcastingSerializer,
} from "./messages/upcaster.js"

// ── DCB queries: plain data, spec vocabulary ───────────────────────────────
// `EventCriteria` and `compileQuery` are the STORE side of the boundary.
export {
  type QueryItem,
  type EventQuery,
  type EventCriteria,
  type TagCriteria,
  type TypeRestrictedCriteria,
  type EitherCriteria,
  queryItems,
  compileQuery,
  resolveTypeName,
} from "./query/event-query.js"

// ── buses: shapes + local segments ─────────────────────────────────────────
export { type CommandBus } from "./buses/command-bus.js"
export { type QueryBus } from "./buses/query-bus.js"
export { simpleCommandBus } from "./buses/simple-command-bus.js"
export { simpleQueryBus } from "./buses/simple-query-bus.js"

// ── interception: one seam, one function ───────────────────────────────────
export {
  type Intercept,
  interceptingCommandBus,
  interceptingQueryBus,
  lineage,
} from "./buses/intercepting-bus.js"

// ── edge verbs: build the message, dispatch it ─────────────────────────────
// `query` is BOTH the descriptor constructor and the dispatch verb — the
// surface names both, and a barrel exports one binding per name. Arity tells
// them apart: one definition object declares, a bus first dispatches.
export { send, query, subscriptionQuery } from "./buses/verbs.js"

// Transports are packages, not a core concept. There is no connector seam, no
// distributed bus and no routing strategy here: a transport takes YOUR local
// bus and returns a bus of the same shape (`rabbitMqCommandBus(rabbit, local)`,
// `kronosDbCommandBus(kdb, local)`), so the routing lives with the wire format
// that determines it and core never learns the word "remote".

// Subscription queries
export {
  type SubscriptionQueryResult,
  type UpdateHandler,
  updateHandler,
  runAfterCommitOrImmediately,
} from "./buses/subscription-query.js"

export {
  type SubscriptionFilter,
  payloadEquals,
  applySubscriptionFilter,
  extractStructuredFilter,
  matchesPayloadEquals,
} from "./buses/subscription-filter.js"

// Event publication seam. `EventSink` is public because `ctx.append` and the
// in-memory scheduler both take one; `EventBus` / `SubscribableEventSource` are
// NOT — they are the internal shape `EventStore` is declared against, and no
// host writes them. `simpleEventBus` is gone with the on-commit lane it served:
// event delivery is tracked-only, through `eventProcessor`.
export { type EventSink } from "./buses/event-sink.js"

// No message-monitor seam either. A monitor is a wrapper over a bus you already
// have — `@kronos-ts/otlp` is the worked example — so the registry that existed
// to hold monitors nobody registered is gone.

// ── handlers: three contexts, one definition shape ─────────────────────────
export {
  type CommandHandlerDefinition,
  commandHandler,
} from "./handlers/command-handler.js"
export {
  type QueryHandlerDefinition,
  queryHandler,
} from "./handlers/query-handler.js"
export {
  type EventHandlerDefinition,
  eventHandler,
} from "./handlers/event-handler.js"

// Handler contexts — built FRESH per invocation as a closure over that
// invocation's unit of work, buses and stores.
export {
  type HandlerContext,
  type EventHandlerContext,
  type QueryHandlerContext,
  type HandlerContextDeps,
  type ContextAppendFunction,
  type ContextLoadFunction,
  type ContextSendFunction,
  type ContextQueryFunction,
  type ContextScheduleFunction,
  type ContextScheduleAfterFunction,
  handlerContext,
  eventHandlerContext,
  queryHandlerContext,
} from "./handlers/handler-context.js"
export { type EmitUpdateFunction } from "./buses/emit-update.js"
export { type CommandDispatchFunction } from "./handlers/ctx-send.js"
export { type QueryDispatchFunction } from "./handlers/ctx-query.js"

// ── event delivery: tracked only; the processor is a value ─────────────────
export { type Sequence, sequentialPerTag } from "./processor/sequence.js"
export {
  type EventProcessor,
  type EventProcessorStatus,
  type RunningProcessor,
  eventProcessor,
} from "./processor/event-processor.js"

// Event source (what a processor reads)
export {
  type SequencedEvent,
  type StreamableEventSource,
  type StreamingCondition,
  type MessageStream,
  messageStream,
  emptyMessageStream,
  failedMessageStream,
} from "./processor/event-source.js"

// Segments
export {
  type Segment,
  ROOT_SEGMENT,
  segment,
  segmentMatches,
  splitSegment,
  mergeSegments,
  isMergeable,
  segmentCount,
  hashOf,
  segments,
} from "./processor/segment.js"

// Tracking tokens
export {
  type TrackingToken,
  type GlobalSequenceToken,
  type GapAwareToken,
  type ReplayToken,
  type SerializedToken,
  globalSequenceToken,
  gapAwareToken,
  FIRST_TOKEN,
  LATEST_TOKEN,
  replayToken,
  isReplayToken,
  isGlobalSequenceToken,
  isGapAwareToken,
  advanceToken,
  advanceTokenTo,
  serializeToken,
  deserializeToken,
  isReplaying,
  unwrapToken,
  wasProcessedBeforeReset,
} from "./processor/tracking-token.js"

// Dead-letter reprocessing
export {
  type DeadLetterReprocessor,
  type DeadLetterReprocessorOptions,
  type DeadLetterReplay,
  deadLetterReprocessor,
} from "./processor/dead-letter-reprocessor.js"

// Event scheduling
export {
  type EventScheduler,
  type ScheduleToken,
  type CancelResult,
} from "./processor/event-scheduler.js"
export {
  type InMemoryEventScheduler,
  type InMemoryEventSchedulerOptions,
  inMemoryEventScheduler,
} from "./processor/in-memory-event-scheduler.js"

// ── assembly: four lists ───────────────────────────────────────────────────
export {
  kronos,
  type App,
  type HandlerSite,
  type Sited,
  type CommandHandlerEntry,
  type QueryHandlerEntry,
  type EventHandlerEntry,
  type StateEntry,
  type StateOptions,
} from "./assembly/kronos.js"

// ── decision models ────────────────────────────────────────────────────────
export {
  type StateModule,
  type StateLifecycle,
  type EvolverEntry,
  type EvolveEntries,
  type IdSchema,
  type InferIdFromSchema,
  type StateTags,
  type TagRecord,
  state,
} from "./state/state.js"

export {
  type LoadResult,
  type StateRepository,
  type StateManager,
  stateManager,
} from "./state/state-manager.js"

export { eventSourcedRepository } from "./state/event-sourced-repository.js"
export type { EventSourcedRepositoryOptions } from "./state/event-sourced-repository.js"

export {
  type SnapshotPolicy,
  type EvolutionResult,
  afterEvents,
  whenSourcingTimeExceeds,
  noSnapshotPolicy,
} from "./state/snapshot-policy.js"

// Handler capabilities are reached via the HandlerContext (second handler
// argument). The implementations live beside the state model; only their types
// are public here.
export type { StateManagerLike, LoadFunction } from "./state/load.js"
export type {
  ScheduleFunction,
  ScheduleAfterFunction,
  ScheduleFunctions,
} from "./state/schedule.js"
export type { EventList, AppendFunction } from "./state/append.js"

// ── store seams + in-memory implementations ────────────────────────────────
export {
  type ConsistencyMarker,
  ORIGIN,
  INFINITY,
  noMarker,
  markerAt,
  markerLowerBound,
  markerUpperBound,
} from "./stores/consistency-marker.js"

export {
  type SourcingCondition,
  sourcingCondition,
} from "./stores/sourcing-condition.js"

export {
  type AppendCondition,
  appendCondition,
} from "./stores/append-condition.js"

export { type EventStore, type SourcingResult } from "./stores/event-store.js"
export {
  type EventStoreTransaction,
  eventStoreTransaction,
} from "./stores/event-store-transaction.js"
export {
  type EventStorageEngine,
  type AppendTransaction,
} from "./stores/event-storage-engine.js"
export { inMemoryEventStore, AppendConditionError } from "./stores/in-memory-event-store.js"

export {
  type Snapshot,
  type SnapshotStore,
  inMemorySnapshotStore,
} from "./stores/snapshot-store.js"

export {
  type TagResolver,
  descriptorBasedTagResolver,
  metadataBasedTagResolver,
  multiTagResolver,
} from "./stores/tag-resolver.js"

// Token store — methods take (processorName, …, uow?)
export {
  type TokenStore,
  UnableToClaimTokenError,
  inMemoryTokenStore,
} from "./stores/token-store.js"

// Dead-letter queue — methods take (processingGroup, …, uow?)
export {
  type DeadLetter,
  type EnqueueDecision,
  type SequencedDeadLetterQueue,
  deadLetter,
  inMemoryDeadLetterQueue,
  DeadLetterQueueOverflowError,
} from "./stores/dead-letter-queue.js"

// Transaction plumbing is NOT here, and neither is any transaction vocabulary.
//
// A host imports its adapter's finished pieces — the factory
// (`drizzleUnitOfWork(db, unitOfWork)`) and the typed accessors
// (`drizzleTransaction(uow)` / `activeDrizzleTransaction(uow)`). Adapter
// AUTHORS reach the shared glue at `@kronos-ts/core/transaction`. The base
// `UnitOfWork` has no `transaction()` and the handler context has no
// `ctx.transaction`: a transaction is a fact about a driver, and only the
// adapter that owns the driver can type it.
//
// Neither is any TRACING vocabulary. There is no span seam, no metrics seam and
// no tracing or metering handler here — observability is a package of functions
// over these public shapes, which anybody could have written.
