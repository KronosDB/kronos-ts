export {
  type KronosDbConnectionConfig,
  type KronosDbConnection,
  type ConnectionState,
  connectToKronosDb,
} from "./connection.js"

export {
  kronosDbEventStore,
} from "./kronosdb-event-store.js"

export {
  kronosDbSubscription,
  kronosDbTail,
  kronosDbTagsAt,
  kronosDbPositionAt,
} from "./kronosdb-log-reads.js"

export {
  type SubscriptionOptions,
  type ProgressingStream,
} from "./kronosdb-subscription.js"

export {
  kronosDbSnapshottingEventStore,
} from "./kronosdb-snapshotting-event-store.js"

export {
  type NativeLink,
} from "./native-link.js"

export {
  type NativeTransport,
  type NativeWelcome,
} from "./native-transport.js"

export {
  type KronosDbSchedulingControl,
  type PendingSchedule,
  ScheduleAlreadyExistsError,
  kronosDbSchedulingEventStore,
} from "./kronosdb-scheduling-event-store.js"

export {
  kronosDbConnection,
  type BusHost,
  type KronosDbConnectionOptions,
  type KronosDbConnectionHandle,
} from "./kronosdb.js"

export {
  kronosDbCommandBus,
  type KronosDbCommandBusOptions,
} from "./kronosdb-command-bus.js"

export {
  kronosDbQueryBus,
  type KronosDbQueryBusOptions,
} from "./kronosdb-query-bus.js"

export {
  type FlowControlConfig,
  type ProcessingInstructions,
} from "./native-messaging.js"

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
  KronosDbRequestError,
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

