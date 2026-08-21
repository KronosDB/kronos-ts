// The folders under `src/` are named for what things MEAN, not for what they
// technically are. There is no `buses/`, no `handlers/`, no `stores/` — a bus,
// a handler and a store are the same three shapes repeated once per message
// kind, and filing by shape scatters each kind's life across the tree.
//
// The three lists `kronos` takes map 1:1 onto three of the activity folders:
//
//   commandHandlers  ← command-handling/     queryHandlers ← query-handling/
//   eventHandlers    ← event-processing/
//
// `event-sourcing/` is the fourth folder and takes no list, because what lives
// there is not behaviour to register but DATA to close over: the log, and the
// state values the handlers above fold it into. A state is passed to
// `ctx.load` at the call site, so there is nothing to announce in advance.
//
// with `messaging/` under all of them (what a message IS, before any kind picks
// it up — and therefore importing from none of them), `event-scheduling/` for
// events that have not happened yet, and `unit-of-work/` for the task lifecycle
// every one of them runs inside. FOUR MECHANISMS sit beside those:
// `interception/` at the bus boundary, `correlation/` for what a handling
// carries onward, `upcasting/` at the log boundary — what an event MEANS on the
// way out — and `validation/` at the handling boundary, what may cross it
// inbound and out. All four are wrap-ins that live HERE and serve every backend
// identically, which is exactly why snapshotting is NOT among them: fusing a
// cache lookup into a read is a property of the store you read from, so it is a
// CAPABILITY TIER added per family, in the family's own package. See the
// snapshotting section below.

// ── messaging: what a message IS, before any kind picks it up ──────────────
// Qualified names, metadata, the three message kinds and the descriptors that
// declare them are ONE file — `messaging/messages.ts` — because they are one
// subject. What stayed separate stayed separate because it stands alone.
export {
  type QualifiedName,
  qn,
  qualifiedNameToString,
  qualifiedNameFromString,
  qualifiedNamesEqual,
  is,
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
  type Message,
  type MessageKind,
  type CommandMessage,
  type CommandResultMessage,
  type EventMessage,
  type SequencedEventMessage,
  type QueryMessage,
  type CommandDescriptor,
  type EventDescriptor,
  type QueryDescriptor,
  type MessageDescriptor,
  type TagExtractors,
  type InferResult,
  command,
  event,
  withNamespace,
} from "./messaging/messages.js"

// There is no `Unstamped<M>` and no `stamped()`. `timestamp` is OPTIONAL on
// `Message` — unset means "not through a task yet" — and required again on
// `EventMessage`, because a fact you can read has an instant. Nothing about
// WHEN the instant is settled changed: the bus still stamps from `uow.now()`,
// a transport from system time at the wire, and `ctx.append` at birth. Only the
// vocabulary is gone.

// A tag is part of DECLARING an event — `event({ tags })` is where one is
// written — so it is message vocabulary, and the sourcing side imports it
// rather than the other way round.
export { type Tag, tag, tagsFromRecord } from "./messaging/tag.js"

export { generateIdentifier } from "./messaging/identifier.js"

export { type SerializedError } from "./messaging/serialized-error.js"

// SCHEMAS ARE STANDARD SCHEMA, not any one library's type. Descriptor payload,
// result and id constraints are `StandardSchemaV1`, so zod, valibot, arktype
// and anything else that speaks the standard all work, and core depends on none
// of them. The contract is vendored, types-only, in `messaging/standard-schema.ts`.
export {
  type StandardSchemaV1,
  type InferOutput,
} from "./messaging/standard-schema.js"

// ── messaging/serialization: the wire, for adapters that own one ───────────
export {
  type SerializedObject,
  type Serializer,
  type SerializerDecorator,
} from "./messaging/serialization/converter.js"

// A serializer ENCODES, and that is all it does. There is no validating
// decorator here and no schema registry for one to read: a serializer knows a
// type name and a revision, so validating from inside it meant looking a schema
// up by that pair. Validation moved to where the DESCRIPTOR is — see
// `validation/` below — and the question a registry answered stopped being
// asked.
export { jsonSerializer } from "./messaging/serialization/serializer.js"

// ── upcasting: interception at the LOG boundary ────────────────────────────
// The third mechanism, the same `(x) => x` shape as `Intercept`, one boundary
// over. It wraps the store's READ paths; the log itself is never rewritten.
export {
  type Upcast,
  upcastingEventStore,
} from "./upcasting/upcasting-event-store.js"

// ── snapshotting: NOT A MECHANISM. A CAPABILITY TIER ON THE LOG ────────────
// It used to be the fifth mechanism, with a store seam beside the log and a
// generic decorator to marry the two. Both are gone. There are FOUR mechanisms
// — interception, correlation, upcasting, validation — and this is not one of
// them, because a mechanism is a wrap-in that lives HERE and serves every
// backend the same way, and snapshotting cannot: fusing a cache lookup into a
// read is a property of the STORE you are reading from, and the store families
// live in their own packages.
//
// So the base `EventStore` contract says nothing about snapshots — it is
// complete for event sourcing without them, and most well-designed projects
// never need one line more — and a host that DOES adds the capability by
// WRAPPING, once, in the family that owns the query:
//
//   inMemorySnapshottingEventStore(inMemoryEventStore())
//   postgresSnapshottingEventStore(postgresEventStore(pg, …), pg, { serializer })
//   kronosDbSnapshottingEventStore(kronosDbEventStore(kdb, ctx), kdb, ctx)
//   axonServerSnapshottingEventStore(axonServerEventStore(axon, ctx), axon, ctx)
//
// AND THE COMPILER MAKES YOU. `state({ snapshot: { key, when } })` types the
// state as snapshotting, and `ctx.load` refuses one against an entry whose
// `eventStore` cannot serve it — a wiring mistake that used to be a silent full
// replay is now a build error naming the fix. One anchor says it for every read
// surface: `IfSnapshotCapable`, exported below and documented in `load.ts`.
//
// THE KEY IS YOURS, and it is the whole invalidation story: `ctx.source(query,
// { snapshot })` at the raw layer, `state({ snapshot: { key, when } })` through
// the sugar. Changed the fold's meaning? Change the key, and every old entry is
// unreachable that instant — no migration, no version column, and no heuristic
// guessing on your behalf. Nothing is derived from your code.
//
// Never rewritten, never migrated, never load-bearing: a miss, an unusable
// entry or an unreachable cache all fall back to full sourcing, silently.
export {
  type Snapshot,
  type SnapshotPolicy,
  type SnapshotConfig,
  type EvolutionResult,
  afterEvents,
  whenSourcingTimeExceeds,
  noSnapshotPolicy,
  snapshotIdentifier,
} from "./event-sourcing/snapshot.js"
export { matchesInitialStructure } from "./event-sourcing/structural-fitness.js"
export { inMemorySnapshottingEventStore } from "./event-sourcing/in-memory-snapshotting-event-store.js"

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

// THE PERSISTENCE-FAMILY SLOT. Core owns the mark and knows no occupants: each
// adapter package writes `PersistenceFamily<"drizzle", "…">` once and brands
// what its unit-of-work decorator mints, so wiring one family's token store
// against another family's task is a compile error naming the factory to call.
// Erased entirely — a phantom on an ambient unique symbol, never constructed.
export type { PersistenceFamily } from "./unit-of-work/persistence-family.js"

// ── command-handling: the command kind's whole life ────────────────────────
// The bus shape, the local segment, both births of a command (the edge verb
// and `ctx.send`, one file), the handler, and the context a handling runs in.
export { type CommandBus } from "./command-handling/bus.js"
export { localCommandBus } from "./command-handling/local-bus.js"
export { send, type CommandDispatchFunction } from "./command-handling/send.js"
export {
  type CommandHandler,
  commandHandler,
} from "./command-handling/handler.js"

// Handler contexts — built FRESH per invocation as a closure over that
// invocation's unit of work, buses and stores. The COMMAND context is the
// widest of the three, so the capability types and the deps record all three
// are built from live with it.
export {
  type HandlerContext,
  type HandlerContextDeps,
  type ContextAppendFunction,
  type ContextLoadFunction,
  type ContextSourceFunction,
  type ContextSendFunction,
  type ContextQueryFunction,
  handlerContext,
} from "./command-handling/context.js"

// ── query-handling: the query kind's whole life ────────────────────────────
// `query` is BOTH the descriptor constructor and the dispatch verb — the
// surface names both, and a barrel exports one binding per name. Arity tells
// them apart: one definition object declares, a bus first dispatches.
export { type QueryBus } from "./query-handling/bus.js"
export { localQueryBus } from "./query-handling/local-bus.js"
export { query, type QueryDispatchFunction } from "./query-handling/query.js"
export {
  type QueryHandler,
  queryHandler,
} from "./query-handling/handler.js"
export {
  type QueryHandlerContext,
  queryHandlerContext,
} from "./query-handling/context.js"

// Subscription queries — a query that keeps answering.
export {
  type SubscriptionQueryResult,
  type UpdateHandler,
  updateHandler,
  runAfterCommitOrImmediately,
  subscriptionQuery,
} from "./query-handling/subscription-query.js"

export {
  type SubscriptionFilter,
  payloadEquals,
  applySubscriptionFilter,
  extractStructuredFilter,
  matchesPayloadEquals,
} from "./query-handling/subscription-filter.js"

export { type EmitUpdateFunction } from "./query-handling/emit-update.js"

// Transports are packages, not a core concept. There is no connector seam, no
// distributed bus and no routing strategy here: a transport takes YOUR local
// bus and returns a bus of the same shape (`rabbitMqCommandBus(local, rabbit)`,
// `kronosDbCommandBus(local, kdb)`), so the routing lives with the wire format
// that determines it and core never learns the word "remote".

// ── interception: one seam, one function ───────────────────────────────────
export {
  type Intercept,
  interceptingCommandBus,
  interceptingQueryBus,
} from "./interception/intercepting-bus.js"

// ── correlation: the functions you wrap in ─────────────────────────────────
// Correlation is the CARRYING MECHANISM — metadata jumping from the message a
// handler is handling onto everything that handling gives birth to, and on down
// the chain. It is three functions and nothing else:
//
//   correlating(uow)                 a unit of work that carries a map
//   correlatingHandler(next, from)   the wrapper that fills it and overlays it
//   correlation                      the EDGE intercept that seeds roots
//
// `from` is a plain `(message) => Metadata` the host writes — what jumps is the
// host's call. The pair everybody starts from is two lines:
//
//   const correlationFrom = (parent: Message): Metadata => ({
//     correlationId: String(parent.metadata.correlationId ?? parent.identifier),
//     causationId: String(parent.identifier),   // the PARENT — a hop re-stamps
//   })
//
// Nothing in core demands any of this: compose it and the compiler starts
// requiring it, ignore it and the word never appears in your build.
export { correlating, type CorrelatingUnitOfWork } from "./correlation/correlating.js"
export { correlatingHandler } from "./correlation/correlating-handler.js"
export { correlation } from "./correlation/correlation.js"

// ── validation: the gate, and it needs no registry ─────────────────────────
// The FOURTH mechanism, and the one whose absence used to be filled by a
// registry. Every site that validates already holds the descriptor as an
// ARGUMENT — the edge verbs take one, the birth verbs take one, an entry pairs
// one with its handler — and a descriptor carries its own payload schema. So
// there is nothing to look up and nothing to register:
//
//   validate(descriptor, payload)              the primitive, anywhere
//   validatingHandler(next, descriptor)        inbound, and every ctx birth
//
// The parsed value REPLACES the input on both paths, because standard
// validation is a parse and a schema's coercions and defaults are part of what
// it says.
export { validate } from "./validation/validate.js"
export { validatingHandler } from "./validation/validating-handler.js"

// ── event-sourcing: the log AND the folds over it ──────────────────────────
// State IS sourcing — a decision model is a fold of the same events the store
// holds, so the DCB query, the store seams and the state model are one folder.

// DCB queries: plain data, spec vocabulary. `EventCriteria` and `compileQuery`
// are the STORE side of the boundary.
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
} from "./event-sourcing/dcb-query.js"

export {
  type TagResolver,
  descriptorBasedTagResolver,
  metadataBasedTagResolver,
  multiTagResolver,
} from "./event-sourcing/tag-resolver.js"

export {
  type ConsistencyMarker,
  ORIGIN,
  INFINITY,
  noMarker,
  markerAt,
  markerLowerBound,
  markerUpperBound,
} from "./event-sourcing/consistency-marker.js"

// The sourcing condition carries the SNAPSHOTTING STRATEGY as an optional
// `snapshot` key — see the snapshotting section below. Only the state-load path
// ever sets it; `ctx.source` never does.
export {
  type SourcingCondition,
  type SnapshotKey,
  sourcingCondition,
  withoutSnapshotKey,
} from "./event-sourcing/sourcing-condition.js"

export {
  type AppendCondition,
  appendCondition,
} from "./event-sourcing/append-condition.js"

export {
  type EventStore,
  type SnapshotCapableEventStore,
  type SnapshotCapability,
  type SourcingResult,
} from "./event-sourcing/event-store.js"
export {
  type EventStoreTransaction,
  eventStoreTransaction,
} from "./event-sourcing/event-store-transaction.js"
export {
  type EventStorageEngine,
  type AppendTransaction,
} from "./event-sourcing/event-storage-engine.js"
export { inMemoryEventStore, AppendConditionError } from "./event-sourcing/in-memory.js"

// Event publication seam. `EventSink` is public because `ctx.append` takes one;
// `EventBus` / `SubscribableEventSource` are
// NOT — they are the internal shape `EventStore` is declared against, and no
// host writes them. `simpleEventBus` is gone with the on-commit lane it served:
// event delivery is tracked-only, through `eventProcessor`.
export { type EventSink } from "./event-sourcing/event-sink.js"

// Decision models — the fold side of the same events.
export {
  type State,
  type StateLifecycle,
  type EvolverEntry,
  type EvolveShape,
  type EvolveTuple,
  type InitialState,
  type IdSchema,
  type InferIdFromSchema,
  type StateTags,
  type TagRecord,
  state,
} from "./event-sourcing/state.js"

// The fold at a site. `repositoryFor` is the LAZY CACHE `ctx.load` goes
// through — nothing registers with it, and forgetting it costs a rebuild.
export {
  type LoadResult,
  type StateRepository,
  eventSourcedRepository,
  repositoryFor,
} from "./event-sourcing/repository.js"

// Handler capabilities are reached via the HandlerContext (second handler
// argument). The implementations live beside the state model; only their types
// are public here.
// THE DEMAND lives with the source types, because it is a question about what a
// READ can ask for. `IfSnapshotCapable` is the one predicate; `SnapshotReads`
// and `SnapshotDemand` are its two faces, and anything later anchors here too.
export type {
  LoadFunction,
  SourceFunction,
  FusedSourceFunction,
  SnapshottedSource,
  IfSnapshotCapable,
  SnapshotReads,
  SnapshotDemand,
} from "./event-sourcing/load.js"
export type { EventList, AppendFunction } from "./event-sourcing/append.js"

// ── event-processing: tracked delivery; the processor is a value ───────────
export {
  type EventHandler,
  eventHandler,
} from "./event-processing/handler.js"
export {
  type EventHandlerContext,
  eventHandlerContext,
} from "./event-processing/context.js"

export { type Sequence, sequentialPerTag } from "./event-processing/sequence.js"
export {
  type EventProcessor,
  type EventProcessorConfig,
  type EventProcessorSite,
  type EventProcessorLane,
  type EventProcessorStatus,
  type RunningProcessor,
  type SequenceDemand,
  eventProcessor,
} from "./event-processing/processor.js"

// Event source (what a processor reads)
export {
  type SequencedEvent,
  type StreamableEventSource,
  type StreamingCondition,
  type MessageStream,
  messageStream,
  emptyMessageStream,
  failedMessageStream,
} from "./event-processing/source.js"

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
} from "./event-processing/segment.js"

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
} from "./event-processing/tracking-token.js"

// Token store — methods take (processorName, …, uow?)
export {
  type TokenStore,
  UnableToClaimTokenError,
  inMemoryTokenStore,
} from "./event-processing/token-store.js"

// Dead-letter queue — methods take (processingGroup, …, uow?)
export {
  type DeadLetter,
  type EnqueueDecision,
  type SequencedDeadLetterQueue,
  deadLetter,
  inMemoryDeadLetterQueue,
  DeadLetterQueueOverflowError,
} from "./event-processing/dead-letter-queue.js"

// Dead-letter reprocessing
export {
  type DeadLetterReprocessor,
  type DeadLetterReprocessorOptions,
  type DeadLetterReplay,
  deadLetterReprocessor,
} from "./event-processing/dead-letter-reprocessor.js"

// ── event-scheduling: events that have not happened yet ────────────────────
// THE SECOND STORE TIER. There is no `EventScheduler` seam any more and no
// standalone scheduler to construct: a log that can hold a future event is a
// log that was WRAPPED, and the three verbs reach a handler only when its
// entry wired one.
export {
  type ScheduleCapability,
  type ScheduleCapableEventStore,
  type ScheduleToken,
  type CancelResult,
} from "./event-scheduling/scheduler.js"
export {
  type InMemorySchedulingOptions,
  type InMemorySchedulingControl,
  inMemorySchedulingEventStore,
} from "./event-scheduling/in-memory-scheduling-event-store.js"
export type {
  IfScheduleCapable,
  ScheduleVerbs,
  ScheduleFunction,
  ScheduleAfterFunction,
  CancelScheduleFunction,
  ScheduleFunctions,
} from "./event-scheduling/schedule.js"

// ── assembly: three lists ──────────────────────────────────────────────────
// Behaviour is registered; data is not. There is no `states` list and no
// `StateEntry`/`StateOptions` to describe one — a state is a value a handler
// closes over, and its snapshot policy rides on that value.
export {
  kronos,
  type App,
  type HandlerSite,
  type Sited,
  type CommandHandlerEntry,
  type QueryHandlerEntry,
  type EventHandlerEntry,
} from "./kronos.js"

// No message-monitor seam. A monitor is a wrapper over a bus you already have —
// `@kronos-ts/otlp` is the worked example — so the registry that existed to hold
// monitors nobody registered is gone.
//
// Transaction plumbing is NOT here, and neither is any transaction vocabulary.
//
// A host imports its adapter's finished pieces — the factory
// (`drizzleUnitOfWork(unitOfWork, db)`) and the typed accessors
// (`drizzleTransaction(uow)` / `activeDrizzleTransaction(uow)`). There is no
// `@kronos-ts/core/transaction` subpath any more: that glue only ever used the
// PUBLIC phase API (`uow.on(Phase.COMMIT, …)`, `uow.onError(…)`), which made it
// a helper, and helpers live with their users — each persistence package owns
// its own private copy now. The base `UnitOfWork` has no `transaction()` and
// the handler context has no `ctx.transaction`: a transaction is a fact about a
// driver, and only the adapter that owns the driver can type it.
//
// Neither is any TRACING vocabulary. There is no span seam, no metrics seam and
// no tracing or metering handler here — observability is a package of functions
// over these public shapes, which anybody could have written.
