export {
  type KronosDbConnectionConfig,
  type KronosDbConnection,
  type ConnectionState,
  connectToKronosDb,
  createKronosMetadata,
} from "./connection.js"

export {
  createKronosDbEventStore,
} from "./kronosdb-event-store.js"

export {
  createKronosDbSnapshotStore,
} from "./kronosdb-snapshot-store.js"

export {
  kronosDb,
  createDistributedQueryBus,
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
  createPlatformConnection,
} from "./platform-service.js"

export {
  type ProcessorStatus,
  type SegmentStatus,
  type ProcessorStatusSupplier,
  toEventProcessorInfo,
} from "./event-processor-info.js"

export {
  type FlowControlledSender,
  createFlowControlledSender,
} from "./flow-controlled-sender.js"

export {
  type ShutdownLatch,
  type ActivityHandle,
  ShutdownInProgressError,
  createShutdownLatch,
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
  createOutboundStream,
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
