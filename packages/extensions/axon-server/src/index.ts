export {
  type AxonServerConnectionConfig,
  type AxonServerConnection,
  connectToAxonServer,
} from "./connection.js"

export {
  type AxonServerConnectionManager,
  createConnectionManager,
} from "./connection-manager.js"

export {
  createAxonServerEventStore,
} from "./axon-server-event-store.js"

export {
  createAxonServerSnapshotStore,
} from "./axon-server-snapshot-store.js"

export {
  axonServer,
  type AxonServerExtensionConfig,
  type FlowControlConfig,
  type ProcessingInstructions,
} from "./axon-server.js"

export {
  type MessageSizeConfig,
  MessageSizeExceededError,
  createMessageSizeValidator,
} from "./message-size.js"

export {
  type ProcessorStatus,
  type SegmentStatus,
  type ProcessorStatusSupplier,
  toEventProcessorInfo,
} from "./event-processor-info.js"

export {
  type PlatformConnection,
  type PlatformInstruction,
  type InstructionHandler,
  type PlatformServiceOptions,
  createPlatformConnection,
} from "./platform-service.js"

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
  AxonServerErrorCode,
  type AxonServerErrorCodeValue,
  AxonServerError,
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
