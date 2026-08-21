export {
  type AxonServerConnectionConfig,
  type AxonServerConnection,
  type AxonServerConnectionOptions,
  type AxonServerConnectionHandle,
  type AxonServerStoreSource,
  type AxonServerBusSource,
  type AxonServerPlatformSource,
  connectToAxonServer,
  axonServerConnection,
} from "./connection.js"

export { axonServerEventStore } from "./axon-server-event-store.js"

export { axonServerSnapshottingEventStore } from "./axon-server-snapshotting-event-store.js"

export {
  axonServerCommandBus,
  axonServerQueryBus,
  type AxonServerCommandBusOptions,
  type AxonServerQueryBusOptions,
  type FlowControlConfig,
  type ProcessingInstructions,
} from "./axon-server.js"

export {
  axonServerControlPlane,
  type AxonServerControlPlane,
  type ManagedEventProcessor,
} from "./control-plane.js"

export {
  type MessageSizeConfig,
  MessageSizeExceededError,
  messageSizeValidator,
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
  platformConnection,
} from "./platform-service.js"

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
