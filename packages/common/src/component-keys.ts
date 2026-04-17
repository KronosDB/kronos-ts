/**
 * Well-known component keys used by the framework.
 * These are the string keys for looking up components in the registry.
 */
export const ComponentKeys = {
  COMMAND_BUS: "commandBus",
  COMMAND_GATEWAY: "commandGateway",
  QUERY_BUS: "queryBus",
  QUERY_GATEWAY: "queryGateway",
  EVENT_STORE: "eventStore",
  STATE_MANAGER: "stateManager",
  EVENT_PROCESSORS: "eventProcessors",
  UNIT_OF_WORK_FACTORY: "unitOfWorkFactory",
  TOKEN_STORE: "tokenStore",
  TRANSACTION_MANAGER: "transactionManager",
  /** Default serializer — fallback for all serialization. */
  SERIALIZER: "serializer",
  /** Serializer for event payloads. Falls back to SERIALIZER if not configured. */
  EVENT_SERIALIZER: "eventSerializer",
  /** Serializer for command/query message payloads. Falls back to SERIALIZER if not configured. */
  MESSAGE_SERIALIZER: "messageSerializer",
  /** Schema registry for event payload validation. */
  EVENT_SCHEMA_REGISTRY: "eventSchemaRegistry",
  /** Schema registry for command payload validation. */
  COMMAND_SCHEMA_REGISTRY: "commandSchemaRegistry",
  /** Schema registry for query payload validation. */
  QUERY_SCHEMA_REGISTRY: "querySchemaRegistry",
  /** Correlation data providers for automatic metadata propagation. */
  CORRELATION_DATA_PROVIDERS: "correlationDataProviders",
  /** Handler enhancer definitions for wrapping handlers at registration time. */
  HANDLER_ENHANCER_DEFINITIONS: "handlerEnhancerDefinitions",
  /** Routing strategy for command routing in distributed scenarios. */
  ROUTING_STRATEGY: "routingStrategy",
  /** Snapshot store for entity state caching. */
  SNAPSHOT_STORE: "snapshotStore",
  /** Event bus — combines event publication with push-based subscription. */
  EVENT_BUS: "eventBus",
  /** Event gateway — user-facing API for direct event publication. */
  EVENT_GATEWAY: "eventGateway",
  /** Event sink — publish-only contract for event distribution. */
  EVENT_SINK: "eventSink",
  /** Event storage engine — raw storage backend for events. */
  EVENT_STORAGE_ENGINE: "eventStorageEngine",
  /** Tag resolver for deriving tags from event messages. */
  TAG_RESOLVER: "tagResolver",
  /** Message monitor registry for observability. */
  MESSAGE_MONITOR_REGISTRY: "messageMonitorRegistry",
} as const
