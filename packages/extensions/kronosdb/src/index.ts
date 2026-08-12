export {
  type KronosDbConnectionConfig,
  type KronosDbConnection,
  type ConnectionState,
  connectToKronosDb,
  kronosMetadata,
} from "./connection.js"

export {
  kronosDbEventStore,
} from "./kronosdb-event-store.js"

export {
  kronosDbSnapshotStore,
} from "./kronosdb-snapshot-store.js"

export {
  kronosDb,
  distributedQueryBus,
  type KronosDbConfig,
  type KronosDbDependencies,
  type KronosDbComponents,
  type KronosDbBackend,
  type FlowControlConfig,
  type ProcessingInstructions,
} from "./kronosdb.js"

export {
  kronosDbControlPlane,
  type ManagedEventProcessor,
  type ManagedProcessorSource,
  type KronosDbControlPlane,
} from "./control-plane.js"

export {
  type PlatformConnection,
  type PlatformInstruction,
  type InstructionHandler,
  type PlatformServiceOptions,
  platformConnection,
} from "./platform-service.js"

export {
  type ProcessorStatus,
  type SegmentStatus,
  type ProcessorStatusSupplier,
  toEventProcessorInfo,
} from "./event-processor-info.js"

export {
  type FlowControlledSender,
  flowControlledSender,
} from "./flow-controlled-sender.js"

export {
  type ShutdownLatch,
  type ActivityHandle,
  ShutdownInProgressError,
  shutdownLatch,
} from "./shutdown-latch.js"

export {
  KronosDbErrorCode,
  type KronosDbErrorCodeValue,
  KronosDbError,
  NoHandlerForCommandError,
  NoHandlerForQueryError,
  CommandExecutionError,
  QueryExecutionError,
  CommandDispatchError,
  QueryDispatchError,
  ConcurrencyError,
  ConnectionFailedError,
  AuthenticationError,
  mapErrorCode,
  isTransientError,
} from "./errors.js"

export {
  type OutboundStream,
  outboundStream,
} from "./outbound-stream.js"

export {
  metadataToProto,
  metadataFromProto,
  metadataToStringMap,
  metadataFromStringMap,
} from "./metadata-conversion.js"

export {
  kronosDbServiceDefinitions,
  PlatformServiceDefinition,
  CommandServiceDefinition,
  QueryServiceDefinition,
  EventStoreDefinition,
  SnapshotStoreDefinition,
} from "./service-definitions.js"
export type { KronosDbOptions } from "./kronosdb.js"
