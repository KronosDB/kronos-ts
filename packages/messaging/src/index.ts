// Messages
export {
  type Message,
  type CommandMessage,
  type CommandResultMessage,
  type EventMessage,
  type SequencedEventMessage,
  type QueryMessage,
} from "./message.js"

// Descriptors
export {
  type CommandDescriptor,
  type EventDescriptor,
  type QueryDescriptor,
  type MessageDescriptor,
  command,
  event,
  query,
} from "./descriptor.js"

// Event criteria
export {
  type TagCriteria,
  type TypeRestrictedCriteria,
  type EitherCriteria,
  type AnyTagCriteria,
  type RestrictableEventCriteria,
  EventCriteria,
  tags,
  anyTag,
  either,
} from "./event-criteria.js"

// Lifecycle phases (Plan 03-04: relocated from processing-context.ts)
export {
  Phase,
  type PhaseValue,
  // Module-level lifecycle accessors (CTX-03 / D-30)
  on as onPhase,
  onPrepareCommit,
  onCommit,
  onAfterCommit,
  onError,
  whenComplete,
  NoActiveUnitOfWork,
  WrongUoWPhase,
} from "./processing-state.js"

// Unit of Work runners (Plan 03-04: replaces UnitOfWorkFactory shape)
export {
  type UoWRunner,
  runInUoW,
  runInNewUoW,
} from "./unit-of-work.js"

// Handler registration
// Note: Handler context wrapper types deleted (Plan 04-02 / D-41).
// Helper function types deleted.
// Handler capabilities (load/append/send/emitUpdate/schedule/transaction) are
// reached via the HandlerContext passed to every handler — see handler-context.ts.
export {
  type EventHandlerRegistration,
  type EvolverRegistration,
  type QueryHandlerRegistration,
  on,
  onEvent,
} from "./handler.js"

// Command handlers
export {
  type CommandHandlerDefinition,
  commandHandler,
} from "./command-handler.js"

// Event handlers
export {
  type EventHandlerDefinition,
  eventHandler,
} from "./event-handler.js"

// Query handlers
export {
  type QueryHandlerDefinition,
  queryHandler,
} from "./query-handler.js"

// Interceptors
export {
  type DispatchInterceptor,
  type HandlerInterceptor,
} from "./interceptor.js"

// Routing strategies
export {
  type RoutingStrategy,
  metadataRoutingStrategy,
  payloadFieldRoutingStrategy,
} from "./routing-strategy.js"

// Handler enhancers
export {
  type HandlerEnhancerDefinition,
  type HandlerMetadata,
  multiHandlerEnhancerDefinition,
} from "./handler-enhancer.js"

// Metrics
export {
  type MetricsRecorder,
  type Counter,
  type Histogram,
  type MetricAttributes,
  type InstrumentOptions,
  type MeteringOptions,
  noOpMetricsRecorder,
  meteringHandlerEnhancerDefinition,
} from "./metrics.js"

// Correlation data
export {
  type CorrelationDataProvider,
  CORRELATION_DATA_KEY,
  getActiveCorrelationData,
  applyCorrelationData,
  contributeCorrelationData,
  messageOriginProvider,
  simpleCorrelationDataProvider,
  correlationDataHandlerInterceptor,
  correlationDataDispatchInterceptor,
} from "./correlation-data.js"

// Bus interfaces
export { type CommandBus } from "./command-bus.js"
export { type QueryBus } from "./query-bus.js"

// Bus implementations
export { createSimpleCommandBus } from "./simple-command-bus.js"
export { createSimpleQueryBus } from "./simple-query-bus.js"

// Intercepting bus decorators
export { createInterceptingCommandBus } from "./intercepting-command-bus.js"
export { createInterceptingQueryBus } from "./intercepting-query-bus.js"

// Tracing bus decorators
export { createTracingCommandBus } from "./tracing-command-bus.js"

// Gateways
export {
  type CommandGateway,
  type QueryGateway,
  createCommandGateway,
  createQueryGateway,
} from "./gateway.js"

// Subscription queries
export {
  type SubscriptionQueryResult,
  type UpdateHandler,
  createUpdateHandler,
  runAfterCommitOrImmediately,
} from "./subscription-query.js"

export {
  type SubscriptionFilter,
  payloadEquals,
  applySubscriptionFilter,
  extractStructuredFilter,
  matchesPayloadEquals,
} from "./subscription-filter.js"

// Event sink (publish-only)
export { type EventSink } from "./event-sink.js"

// Event bus (publish + subscribe)
export {
  type SubscribableEventSource,
  type EventBus,
  createSimpleEventBus,
} from "./event-bus.js"

// Intercepting event bus decorator
export { createInterceptingEventBus } from "./intercepting-event-bus.js"

// Event gateway
export {
  type EventGateway,
  createEventGateway,
} from "./event-gateway.js"

// Event processor common control + status surface (AF5 EventProcessor analog)
export { type EventProcessor, type EventProcessorStatus } from "./event-processor.js"

// Event source (for processors)
export {
  type SequencedEvent,
  type StreamableEventSource,
  type StreamingCondition,
  type MessageStream,
  createMessageStream,
  emptyMessageStream,
  failedMessageStream,
} from "./event-source.js"

// Event processor
export {
  type TrackingEventProcessor,
  type TrackingEventProcessorOptions,
  type EventProcessingErrorHandler,
  propagatingErrorHandler,
  createTrackingEventProcessor,
} from "./tracking-event-processor.js"

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
  createSegments,
} from "./segment.js"

// Token store
export {
  type TokenStore,
  UnableToClaimTokenError,
  createInMemoryTokenStore,
} from "./token-store.js"

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
} from "./tracking-token.js"

// Replay detection (ProcessingContext helper)
export {
  isReplay,
  REPLAY_STATE_KEY,
} from "./replay-token.js"

// Transaction management
export {
  type TransactionManager,
  noTransactionManager,
  getActiveTransaction,
  getOrBeginActiveTransaction,
  transactionalUnitOfWorkFactory,
  lazyTransactionalUnitOfWorkFactory,
  TRANSACTION_KEY,
} from "./transaction.js"

// Retrying command bus
export {
  type RetryPolicy,
  exponentialBackoffRetryPolicy,
  createRetryingCommandBus,
} from "./retrying-command-bus.js"

// Module-level handler helpers (Plan 04-01 / HDL-02 / D-42)
export { COMMAND_BUS_KEY } from "./send.js"
export {
  type HandlerContext,
  type EventHandlerContext,
  type QueryHandlerContext,
  type ContextAppendFunction,
  type ContextLoadFunction,
  type ContextSendFunction,
  HANDLER_CONTEXT,
  EVENT_HANDLER_CONTEXT,
  QUERY_HANDLER_CONTEXT,
} from "./handler-context.js"
export { QUERY_BUS_KEY, type EmitUpdateFunction } from "./emit-update.js"

// Event scheduling
export {
  type EventScheduler,
  type ScheduleToken,
  type CancelResult,
} from "./event-scheduler.js"
export {
  type InMemoryEventScheduler,
  type InMemoryEventSchedulerOptions,
  createInMemoryEventScheduler,
} from "./in-memory-event-scheduler.js"

// Modules — Plan 08-03a (D-82): function-style helpers replace Module-shape factories
export {
  registerCommandHandlersNatively,
  createCommandInvocation,
  type MinimalConfiguration,
} from "./command-handling-module.js"
export { registerQueryHandlersNatively } from "./query-handling-module.js"

// Subscribing event processor
export {
  type SubscribingEventProcessor,
  type SubscribingEventProcessorOptions,
  createSubscribingEventProcessor,
} from "./subscribing-event-processor.js"

// Streaming event processor
export {
  type StreamingEventProcessor,
  type StreamingEventProcessorOptions,
  createStreamingEventProcessor,
} from "./streaming-event-processor.js"

// Dead letter queue
export {
  type DeadLetter,
  type EnqueueDecision,
  type EnqueuePolicy,
  type SequencedDeadLetterQueue,
  createDeadLetter,
  createInMemoryDeadLetterQueue,
  DeadLetterQueueOverflowError,
} from "./dead-letter-queue.js"

// Enqueue policies + decisions
export {
  Decisions,
  alwaysEnqueuePolicy,
  retryThenEvictPolicy,
  type RetryThenEvictOptions,
  ATTEMPTS_DIAGNOSTIC,
} from "./enqueue-policy.js"

// Dead-lettering event delivery
export {
  type DeadLetteringOptions,
  createDeadLetteringDelivery,
} from "./dead-lettering-handler.js"

// Dead-letter reprocessing
export {
  type DeadLetterReprocessor,
  type DeadLetterReprocessorOptions,
  type DeadLetterReplay,
  createDeadLetterReprocessor,
} from "./dead-letter-reprocessor.js"

// Dead-letter observability
export {
  type DeadLetterListener,
  noOpDeadLetterListener,
  loggingDeadLetterListener,
  multiDeadLetterListener,
} from "./dead-letter-listener.js"

// Sequencing policy
export {
  type SequencingPolicy,
  sequentialPerTag,
  defaultSequencingPolicy,
  fullConcurrencyPolicy,
} from "./sequencing-policy.js"

// Upcasting
export {
  type IntermediateEventRepresentation,
  type EventUpcaster,
  singleEventUpcaster,
  upcasterChain,
  upcastingSerializer,
} from "./upcaster.js"

// Serialization
export {
  type SchemaRegistry,
  jsonSerializer,
  zodValidatingSerializer,
  createEventSchemaRegistry,
  createCommandSchemaRegistry,
  createQuerySchemaRegistry,
  multiSerializer,
} from "./serializer.js"

// Message monitors
export {
  type MessageMonitor,
  type MonitorCallback,
  noOpMessageMonitor,
  multiMessageMonitor,
} from "./message-monitor.js"

export {
  type MessageMonitorRegistry,
  createMessageMonitorRegistry,
} from "./message-monitor-registry.js"

// Tracing (core abstractions)
export {
  type Span,
  type SpanFactory,
  type SpanAttributesProvider,
  noOpSpanFactory,
} from "./span-factory.js"

export {
  tracingHandlerEnhancerDefinition,
} from "./tracing-handler-enhancer.js"

// Processor configuration (legacy)
export { type ProcessorConfiguration } from "./processor-configuration.js"

// Event processor builders
export {
  type EventProcessorModule,
  type TrackingProcessorModule,
  type SubscribingProcessorModule,
  TrackingProcessorBuilder,
  SubscribingProcessorBuilder,
  trackingProcessor,
  subscribingProcessor,
} from "./event-processor-builder.js"

// Namespace factory
export { withNamespace } from "./with-namespace.js"
