import {
  ComponentKeys,
  createComponentRegistry,
  createLifecycleRegistry,
  generateIdentifier,
  qualifiedNameToString,
  type ComponentRegistry,
  type Configuration,
  type ConfigurationEnhancer,
  type ComponentBuilder,
  type ApplicationConfigurer,
  type Module,
  type LifecycleRegistry,
} from "@kronos-ts/common"
import type { EntityModule } from "@kronos-ts/modelling"
import { createStateManager } from "@kronos-ts/modelling"
import type {
  CommandHandlerDefinition,
  QueryHandlersDefinition,
  CommandBus,
  QueryBus,
  CommandGateway,
  QueryGateway,
  DispatchInterceptor,
  HandlerInterceptor,
  CommandMessage,
  EventMessage,
  QueryMessage,
  UoWRunner,
  TokenStore,
  TrackingEventProcessor,
  StreamableEventSource,
  SubscribableEventSource,
  SubscribingEventProcessor,
  EventProcessorModule,
  CorrelationDataProvider,
  HandlerEnhancerDefinition,
  EventSink,
  EventBus,
  EventGateway,
  MessageMonitor,
  MessageMonitorRegistry,
  TransactionManager,
} from "@kronos-ts/messaging"
import {
  createMessageMonitorRegistry,
  jsonSerializer,
  registerCommandHandlersNatively,
  registerQueryHandlersNatively,
  createSimpleCommandBus,
  createSimpleQueryBus,
  createInterceptingCommandBus,
  createInterceptingQueryBus,
  createCommandGateway,
  createQueryGateway,
  createRetryingCommandBus,
  exponentialBackoffRetryPolicy,
  createTrackingEventProcessor,
  createSubscribingEventProcessor,
  runInNewUoW,
  transactionalUnitOfWorkFactory,
  whenComplete,
  onError,
  messageOriginProvider,
  correlationDataHandlerInterceptor,
  correlationDataDispatchInterceptor,
  payloadFieldRoutingStrategy,
  TrackingProcessorBuilder,
  SubscribingProcessorBuilder,
  createEventGateway,
} from "@kronos-ts/messaging"
import type { EventStore } from "./event-store.js"
import { createInMemoryEventStore } from "./in-memory-event-store.js"
import { createEventSourcedRepository } from "./event-sourced-repository.js"
import type { SnapshotStore } from "./snapshot-store.js"
import type { SnapshotPolicy } from "./snapshot-policy.js"
import type { EventStorageEngine } from "./event-storage-engine.js"
import type { TagResolver } from "./tag-resolver.js"
import { descriptorBasedTagResolver } from "./tag-resolver.js"
import { createInterceptingEventStore } from "./intercepting-event-store.js"

/**
 * A built Kronos application. Provides typed access to gateways and lifecycle.
 */
export interface KronosApplication {
  readonly configuration: Configuration
  readonly commandGateway: CommandGateway
  readonly queryGateway: QueryGateway
  readonly eventGateway: EventGateway
  readonly eventStore: EventStore
  readonly eventBus: EventBus
  start(): Promise<void>
  stop(): Promise<void>
}

// =============================================================================
// MessagingConfigurer
// =============================================================================

/**
 * Configurer for messaging infrastructure: buses, interceptors,
 * correlation data, command/query handlers, and event processors.
 *
 * Aligned with Kronos Framework's messaging layer.
 */
export class MessagingConfigurer implements ApplicationConfigurer {
  /** @internal */ readonly _reg: ComponentRegistry & { build(): Configuration }
  /** @internal */ readonly _lifecycle: LifecycleRegistry & { start(): Promise<void>; shutdown(): Promise<void> }
  /** @internal */ readonly _commandHandlerBuilders: ComponentBuilder<CommandHandlerDefinition<any, any>>[] = []
  /** @internal */ readonly _eventProcessorBuilders: ComponentBuilder<EventProcessorModule>[] = []
  /** @internal */ readonly _queryHandlerBuilders: ComponentBuilder<QueryHandlersDefinition>[] = []
  /** @internal */ readonly _correlationDataProviderBuilders: ComponentBuilder<CorrelationDataProvider>[] = []
  /** @internal */ readonly _eventDispatchInterceptorBuilders: ComponentBuilder<DispatchInterceptor<EventMessage>>[] = []
  /** @internal */ readonly _eventHandlerInterceptorBuilders: ComponentBuilder<HandlerInterceptor>[] = []
  /** @internal */ readonly _onStartCallbacks: Array<(config: Configuration) => void | Promise<void>> = []

  /** @internal */
  constructor(
    reg: ComponentRegistry & { build(): Configuration },
    lifecycle: LifecycleRegistry & { start(): Promise<void>; shutdown(): Promise<void> },
  ) {
    this._reg = reg
    this._lifecycle = lifecycle
  }

  // -- Handler registration --

  /** Register a command handler. */
  registerCommandHandler(builder: ComponentBuilder<CommandHandlerDefinition<any, any>>): this {
    this._commandHandlerBuilders.push(builder)
    return this
  }

  /**
   * Register an event processor with its handler groups.
   *
   * ```typescript
   * m.registerEventProcessor(
   *   trackingProcessor("course-projection")
   *     .registerEventHandler(courseProjection)
   *     .batchSize(50)
   * )
   * ```
   */
  registerEventProcessor(builder: ComponentBuilder<EventProcessorModule>): this {
    this._eventProcessorBuilders.push(builder)
    return this
  }

  /** Register a group of query handlers. */
  registerQueryHandlers(builder: ComponentBuilder<QueryHandlersDefinition>): this {
    this._queryHandlerBuilders.push(builder)
    return this
  }

  /** Register a pre-built module directly. */
  registerModule(module: Module): this {
    this._reg.registerModule(module)
    return this
  }

  // -- Infrastructure --

  /** Override the command bus implementation. */
  registerCommandBus(builder: ComponentBuilder<CommandBus>): this {
    this._reg.register(ComponentKeys.COMMAND_BUS, builder)
    return this
  }

  /** Override the query bus implementation. */
  registerQueryBus(builder: ComponentBuilder<QueryBus>): this {
    this._reg.register(ComponentKeys.QUERY_BUS, builder)
    return this
  }

  /** Override the UnitOfWork runner. */
  registerUnitOfWorkFactory(builder: ComponentBuilder<UoWRunner>): this {
    this._reg.register(ComponentKeys.UNIT_OF_WORK_FACTORY, builder)
    return this
  }

  /**
   * Override the event sink implementation.
   *
   * The EventSink is the publish-only contract for event publication.
   * In an event sourcing context, the EventStore serves as the EventSink.
   * Use this for non-event-sourced setups or custom event distribution.
   *
   * Aligned with Kronos Framework's `MessagingConfigurer.registerEventSink()`.
   */
  registerEventSink(builder: ComponentBuilder<EventSink>): this {
    this._reg.register(ComponentKeys.EVENT_SINK, builder)
    return this
  }

  /**
   * Override the event bus implementation.
   *
   * In an event sourcing context, the EventStore serves as the EventBus.
   * In non-event-sourcing setups, use {@link createSimpleEventBus}.
   *
   * Aligned with Kronos Framework's `MessagingConfigurer.registerEventBus()`.
   */
  registerEventBus(builder: ComponentBuilder<EventBus>): this {
    this._reg.register(ComponentKeys.EVENT_BUS, builder)
    return this
  }

  /**
   * Override the event gateway implementation.
   *
   * The EventGateway wraps an EventSink for direct event publication.
   *
   * Aligned with Kronos Framework's `MessagingConfigurer.registerEventGateway()`.
   */
  registerEventGateway(builder: ComponentBuilder<EventGateway>): this {
    this._reg.register(ComponentKeys.EVENT_GATEWAY, builder)
    return this
  }

  // -- Correlation data --

  /**
   * Register a correlation data provider.
   *
   * By default, a {@link messageOriginProvider} is registered that propagates
   * `correlationId` and `causationId`.
   */
  registerCorrelationDataProvider(builder: ComponentBuilder<CorrelationDataProvider>): this {
    this._correlationDataProviderBuilders.push(builder)
    return this
  }

  // -- Interceptors --

  /** Register a dispatch interceptor for commands. */
  registerCommandDispatchInterceptor(builder: ComponentBuilder<DispatchInterceptor<CommandMessage>>): this {
    this._onStartCallbacks.push((config) => {
      const interceptor = builder(config)
      const bus = config.getComponent<any>(ComponentKeys.COMMAND_BUS)
      if (bus.registerDispatchInterceptor) bus.registerDispatchInterceptor(interceptor)
    })
    return this
  }

  /** Register a handler interceptor for commands. */
  registerCommandHandlerInterceptor(builder: ComponentBuilder<HandlerInterceptor>): this {
    this._onStartCallbacks.push((config) => {
      const interceptor = builder(config)
      const bus = config.getComponent<any>(ComponentKeys.COMMAND_BUS)
      if (bus.registerHandlerInterceptor) bus.registerHandlerInterceptor(interceptor)
    })
    return this
  }

  /** Register a dispatch interceptor for queries. */
  registerQueryDispatchInterceptor(builder: ComponentBuilder<DispatchInterceptor<QueryMessage>>): this {
    this._onStartCallbacks.push((config) => {
      const interceptor = builder(config)
      const bus = config.getComponent<any>(ComponentKeys.QUERY_BUS)
      if (bus.registerDispatchInterceptor) bus.registerDispatchInterceptor(interceptor)
    })
    return this
  }

  /** Register a handler interceptor for queries. */
  registerQueryHandlerInterceptor(builder: ComponentBuilder<HandlerInterceptor>): this {
    this._onStartCallbacks.push((config) => {
      const interceptor = builder(config)
      const bus = config.getComponent<any>(ComponentKeys.QUERY_BUS)
      if (bus.registerHandlerInterceptor) bus.registerHandlerInterceptor(interceptor)
    })
    return this
  }

  /** Register a dispatch interceptor for events (applied during event processor delivery). */
  registerEventDispatchInterceptor(builder: ComponentBuilder<DispatchInterceptor<EventMessage>>): this {
    this._eventDispatchInterceptorBuilders.push(builder)
    return this
  }

  /** Register a handler interceptor for events (applied during event processor delivery). */
  registerEventHandlerInterceptor(builder: ComponentBuilder<HandlerInterceptor>): this {
    this._eventHandlerInterceptorBuilders.push(builder)
    return this
  }

  // -- Message monitors --

  /**
   * Register a monitor for all message types.
   * Aligned with Kronos Framework's `MessagingConfigurer.registerMessageMonitor()`.
   */
  registerMessageMonitor(builder: ComponentBuilder<MessageMonitor>): this {
    this._onStartCallbacks.push((config) => {
      const registry = config.getComponent<MessageMonitorRegistry>(ComponentKeys.MESSAGE_MONITOR_REGISTRY)
      registry.registerMonitor(builder(config))
    })
    return this
  }

  /** Register a monitor for command messages only. */
  registerCommandMonitor(builder: ComponentBuilder<MessageMonitor>): this {
    this._onStartCallbacks.push((config) => {
      const registry = config.getComponent<MessageMonitorRegistry>(ComponentKeys.MESSAGE_MONITOR_REGISTRY)
      registry.registerCommandMonitor(builder(config))
    })
    return this
  }

  /** Register a monitor for event messages only. */
  registerEventMonitor(builder: ComponentBuilder<MessageMonitor>): this {
    this._onStartCallbacks.push((config) => {
      const registry = config.getComponent<MessageMonitorRegistry>(ComponentKeys.MESSAGE_MONITOR_REGISTRY)
      registry.registerEventMonitor(builder(config))
    })
    return this
  }

  /** Register a monitor for query messages only. */
  registerQueryMonitor(builder: ComponentBuilder<MessageMonitor>): this {
    this._onStartCallbacks.push((config) => {
      const registry = config.getComponent<MessageMonitorRegistry>(ComponentKeys.MESSAGE_MONITOR_REGISTRY)
      registry.registerQueryMonitor(builder(config))
    })
    return this
  }

  /** Direct access to the component registry. */
  componentRegistry(fn: (registry: ComponentRegistry) => void): this {
    fn(this._reg)
    return this
  }

  /** Access the lifecycle registry for startup/shutdown phase ordering. */
  lifecycleRegistry(fn: (registry: LifecycleRegistry) => void): this {
    fn(this._lifecycle)
    return this
  }
}

// =============================================================================
// ModellingConfigurer
// =============================================================================

/**
 * Configurer for entity modelling: entity registration and state management.
 * Wraps a {@link MessagingConfigurer} and delegates messaging concerns to it.
 *
 * Aligned with Kronos Framework's modelling layer.
 */
export class ModellingConfigurer implements ApplicationConfigurer {
  /** @internal */ readonly _msg: MessagingConfigurer
  /** @internal */ readonly _entities: Array<{ entity: EntityModule<any, any>; snapshotPolicy?: SnapshotPolicy }> = []

  /** @internal */
  constructor(msg: MessagingConfigurer) {
    this._msg = msg
  }

  // -- Modelling-level (OWN) --

  /**
   * Register an event-sourced entity module.
   *
   * Optionally configure a per-entity snapshot policy, aligned with Kronos Framework's
   * `EventSourcedEntityModule.snapshotPolicy()`.
   */
  registerEntity(entity: EntityModule<any, any>, options?: { snapshotPolicy?: SnapshotPolicy }): this {
    this._entities.push({ entity, snapshotPolicy: options?.snapshotPolicy })
    return this
  }

  // -- Convenience delegations to messaging --

  /** Register a command handler. Delegates to {@link MessagingConfigurer}. */
  registerCommandHandler(builder: ComponentBuilder<CommandHandlerDefinition<any, any>>): this {
    this._msg.registerCommandHandler(builder)
    return this
  }

  /** Register a group of query handlers. Delegates to {@link MessagingConfigurer}. */
  registerQueryHandlers(builder: ComponentBuilder<QueryHandlersDefinition>): this {
    this._msg.registerQueryHandlers(builder)
    return this
  }

  /** Register an event processor. Delegates to {@link MessagingConfigurer}. */
  registerEventProcessor(builder: ComponentBuilder<EventProcessorModule>): this {
    this._msg.registerEventProcessor(builder)
    return this
  }

  /**
   * Access the messaging layer for configuration.
   *
   * Use this for messaging-specific concerns: buses, interceptors,
   * correlation data providers, UnitOfWork factory.
   */
  messaging(fn: (configurer: MessagingConfigurer) => void): this {
    fn(this._msg)
    return this
  }

  /** Direct access to the component registry. */
  componentRegistry(fn: (registry: ComponentRegistry) => void): this {
    fn(this._msg._reg)
    return this
  }

  /** Access the lifecycle registry for startup/shutdown phase ordering. */
  lifecycleRegistry(fn: (registry: LifecycleRegistry) => void): this {
    fn(this._msg._lifecycle)
    return this
  }
}

// =============================================================================
// EventSourcingConfigurer
// =============================================================================

/**
 * The top-level configurer for a Kronos application with event sourcing.
 *
 * Wraps a {@link ModellingConfigurer} which wraps a {@link MessagingConfigurer}.
 *
 * ```typescript
 * const app = await EventSourcingConfigurer.create()
 *   .registerEntity(CourseEntity)
 *   .registerCommandHandler(config => createCourse)
 *   .registerEventProcessor(
 *     trackingProcessor("course-projection")
 *       .registerEventHandler(courseProjection)
 *   )
 *   .registerQueryHandlers(config => courseQueries)
 *   .messaging(m => {
 *     m.registerCorrelationDataProvider(config => simpleCorrelationDataProvider("tenantId"))
 *   })
 *   .start()
 * ```
 */
export class EventSourcingConfigurer implements ApplicationConfigurer {
  private readonly mdl: ModellingConfigurer
  private readonly msg: MessagingConfigurer
  private readonly reg: ComponentRegistry & { build(): Configuration }
  private readonly lifecycle: ReturnType<typeof createLifecycleRegistry>
  private readonly enhancers: ConfigurationEnhancer[] = []
  private readonly onStopCallbacks: Array<() => void | Promise<void>> = []
  private processors: Array<TrackingEventProcessor | SubscribingEventProcessor> = []

  private constructor(options?: { eventStore?: EventStore }) {
    this.reg = createComponentRegistry()
    this.lifecycle = createLifecycleRegistry()
    this.msg = new MessagingConfigurer(this.reg, this.lifecycle)
    this.mdl = new ModellingConfigurer(this.msg)
    if (options?.eventStore) {
      const es = options.eventStore
      this.reg.register(ComponentKeys.EVENT_STORE, () => es)
    }
  }

  /** Create a new configurer. */
  static create(options?: { eventStore?: EventStore }): EventSourcingConfigurer {
    return new EventSourcingConfigurer(options)
  }

  // -- Event sourcing-level (OWN) --

  /** Override the event store implementation. */
  registerEventStore(builder: ComponentBuilder<EventStore>): this {
    this.reg.register(ComponentKeys.EVENT_STORE, builder)
    return this
  }

  /**
   * Override the event storage engine implementation.
   *
   * The EventStorageEngine is the raw persistence backend for events.
   * Database extensions (drizzle, knex, prisma, etc.) provide implementations.
   * The EventStore composes the storage engine with event distribution.
   *
   * Aligned with Kronos Framework's `EventSourcingConfigurer.registerEventStorageEngine()`.
   */
  registerEventStorageEngine(builder: ComponentBuilder<EventStorageEngine>): this {
    this.reg.register(ComponentKeys.EVENT_STORAGE_ENGINE, builder)
    return this
  }

  /**
   * Override the tag resolver.
   *
   * The TagResolver derives tags from event messages during storage.
   * Default: descriptor-based (reads tags from the event descriptor's `tags` function).
   *
   * Aligned with Kronos Framework's `EventSourcingConfigurer.registerTagResolver()`.
   */
  registerTagResolver(builder: ComponentBuilder<TagResolver>): this {
    this.reg.register(ComponentKeys.TAG_RESOLVER, builder)
    return this
  }

  /** Register a configuration enhancer (e.g., Axon Server connector, OpenTelemetry). */
  registerEnhancer(enhancer: ConfigurationEnhancer): this {
    this.enhancers.push(enhancer)
    return this
  }

  // -- Convenience delegations to modelling --

  /**
   * Register an event-sourced entity module. Delegates to {@link ModellingConfigurer}.
   */
  registerEntity(entity: EntityModule<any, any>, options?: { snapshotPolicy?: SnapshotPolicy }): this {
    this.mdl.registerEntity(entity, options)
    return this
  }

  // -- Convenience delegations to modelling → messaging --

  /** Register a command handler. Delegates through modelling to messaging. */
  registerCommandHandler(builder: ComponentBuilder<CommandHandlerDefinition<any, any>>): this {
    this.mdl.registerCommandHandler(builder)
    return this
  }

  /** Register a group of query handlers. Delegates through modelling to messaging. */
  registerQueryHandlers(builder: ComponentBuilder<QueryHandlersDefinition>): this {
    this.mdl.registerQueryHandlers(builder)
    return this
  }

  /** Register an event processor. Delegates through modelling to messaging. */
  registerEventProcessor(builder: ComponentBuilder<EventProcessorModule>): this {
    this.mdl.registerEventProcessor(builder)
    return this
  }

  // -- Layer access --

  /**
   * Access the modelling layer for configuration.
   *
   * Use this for entity-specific concerns not available as convenience methods.
   */
  modelling(fn: (configurer: ModellingConfigurer) => void): this {
    fn(this.mdl)
    return this
  }

  /**
   * Access the messaging layer for configuration.
   *
   * Use this for messaging-specific concerns: buses, interceptors,
   * correlation data providers, UnitOfWork factory.
   */
  messaging(fn: (configurer: MessagingConfigurer) => void): this {
    this.mdl.messaging(fn)
    return this
  }

  /** Direct access to the component registry. */
  componentRegistry(fn: (registry: ComponentRegistry) => void): this {
    fn(this.reg)
    return this
  }

  /** Access the lifecycle registry for startup/shutdown phase ordering. */
  lifecycleRegistry(fn: (registry: LifecycleRegistry) => void): this {
    fn(this.lifecycle)
    return this
  }

  // -- Composition --

  /**
   * Apply a configuration function. Each domain slice is a function
   * that receives and configures the configurer.
   *
   * ```typescript
   * EventSourcingConfigurer.create()
   *   .configure(configureCourses)
   *   .configure(configureEnrollment)
   *   .start()
   * ```
   */
  configure(fn: (configurer: EventSourcingConfigurer) => EventSourcingConfigurer | void): this {
    fn(this)
    return this
  }

  // -- Build & start --

  /** Build the application configuration. */
  build(): KronosApplication {
    this.registerDefaults()

    // Apply enhancers
    const sorted = [...this.enhancers].sort((a, b) => (a.order ?? 0) - (b.order ?? 0))
    for (const enhancer of sorted) {
      enhancer.enhance(this.reg)
    }

    // Build command handlers from ComponentBuilders
    const config = this.reg.build()
    const commandHandlers = this.msg._commandHandlerBuilders.map(b => b(config))
    if (commandHandlers.length > 0) {
      // Plan 08-03a: Module-shape wrapper around the new native helper, so the
      // legacy configurer's module-initialize machinery still works for the
      // enhancer-using e2es. Plan 04 deletes the entire configurer.
      this.reg.registerModule({
        name: "commands",
        initialize(cfg) {
          const cmdBus = cfg.getComponent<CommandBus>(ComponentKeys.COMMAND_BUS)
          const enhancer = cfg.getOptionalComponent<HandlerEnhancerDefinition>(
            ComponentKeys.HANDLER_ENHANCER_DEFINITIONS,
          )
          registerCommandHandlersNatively(commandHandlers, {
            commandBus: cmdBus,
            config: cfg,
            handlerEnhancer: enhancer,
            moduleName: "commands",
          })
        },
      })
    }

    // Build query handlers from ComponentBuilders
    const queryHandlerGroups = this.msg._queryHandlerBuilders.map(b => b(config))
    if (queryHandlerGroups.length > 0) {
      this.reg.registerModule({
        name: "queries",
        initialize(cfg) {
          const qryBus = cfg.getComponent<QueryBus>(ComponentKeys.QUERY_BUS)
          registerQueryHandlersNatively(queryHandlerGroups, { queryBus: qryBus })
        },
      })
    }

    // Rebuild config after modules are registered
    const finalConfig = this.reg.build()
    const startCbs = this.msg._onStartCallbacks
    const stopCbs = this.onStopCallbacks
    const enhancersSorted = sorted
    const lifecycleReg = this.lifecycle
    const procs = this

    return {
      configuration: finalConfig,

      get commandGateway(): CommandGateway {
        return finalConfig.getComponent<CommandGateway>(ComponentKeys.COMMAND_GATEWAY)
      },

      get queryGateway(): QueryGateway {
        return finalConfig.getComponent<QueryGateway>(ComponentKeys.QUERY_GATEWAY)
      },

      get eventGateway(): EventGateway {
        return finalConfig.getComponent<EventGateway>(ComponentKeys.EVENT_GATEWAY)
      },

      get eventStore(): EventStore {
        return finalConfig.getComponent<EventStore>(ComponentKeys.EVENT_STORE)
      },

      get eventBus(): EventBus {
        return finalConfig.getComponent<EventBus>(ComponentKeys.EVENT_BUS)
      },

      async start() {
        // Initialize modules
        for (const module of finalConfig.getModules()) {
          module.initialize(finalConfig)
        }

        // Enhancer onStart hooks
        for (const enhancer of enhancersSorted) {
          if (enhancer.onStart) await enhancer.onStart(finalConfig)
        }

        // Start callbacks (interceptor registration, etc.)
        for (const cb of startCbs) {
          await cb(finalConfig)
        }

        // Lifecycle start handlers (phase-ordered)
        await lifecycleReg.start()

        // Start event processors
        const processors = finalConfig.getOptionalComponent<Array<TrackingEventProcessor | SubscribingEventProcessor>>(
          ComponentKeys.EVENT_PROCESSORS,
        )
        if (processors) {
          procs.processors = processors
          for (const proc of processors) {
            await proc.start()
          }
        }
      },

      async stop() {
        for (const proc of procs.processors) {
          proc.stop()
        }

        // Lifecycle shutdown handlers (reverse phase order)
        await lifecycleReg.shutdown()

        for (const enhancer of enhancersSorted) {
          if (enhancer.onStop) await enhancer.onStop()
        }
        for (const cb of stopCbs) {
          await cb()
        }
      },
    }
  }

  /** Build and start the application. */
  async start() {
    const app = this.build()
    await app.start()
    return app
  }

  // -- Internal defaults --

  private registerDefaults(): void {
    // Default serializer (JSON) — used by event store, snapshot store, buses
    this.reg.registerIfAbsent(ComponentKeys.SERIALIZER, () => jsonSerializer())

    // Event store
    this.reg.registerIfAbsent(ComponentKeys.EVENT_STORE, () => createInMemoryEventStore())

    // InterceptingEventStore decorator — wraps event store with dispatch interceptors
    // Applied only when event dispatch interceptors are configured.
    // Follows the same decorator pattern as InterceptingCommandBus/InterceptingQueryBus.
    const evtDispatchBuilders = this.msg._eventDispatchInterceptorBuilders
    if (evtDispatchBuilders.length > 0) {
      this.reg.registerDecorator<EventStore>(
        ComponentKeys.EVENT_STORE,
        0,
        (config, _name, delegate) => {
          const interceptors = evtDispatchBuilders.map(b => b(config))
          return createInterceptingEventStore(delegate, interceptors)
        },
      )
    }

    // Tag resolver — default passes through descriptor-derived tags
    this.reg.registerIfAbsent(ComponentKeys.TAG_RESOLVER, () => descriptorBasedTagResolver())

    // State manager — always registered (Gap 14). Entities populated only if configured.
    const entityEntries = this.mdl._entities
    this.reg.registerIfAbsent(ComponentKeys.STATE_MANAGER, (config) => {
      const stateManager = createStateManager()
      if (entityEntries.length > 0) {
        const eventStore = config.getComponent<EventStore>(ComponentKeys.EVENT_STORE)
        const snapshotStore = config.getOptionalComponent<SnapshotStore>(ComponentKeys.SNAPSHOT_STORE)
        for (const { entity, snapshotPolicy } of entityEntries) {
          // Fail fast: snapshot policy without snapshot store
          if (snapshotPolicy && !snapshotStore) {
            throw new Error(
              `Entity "${entity.name}" has a snapshot policy configured but no SnapshotStore ` +
              `is registered. Register a SnapshotStore via componentRegistry() or use the ` +
              `the Axon Server connector which provides one automatically.`,
            )
          }
          stateManager.register(
            entity,
            createEventSourcedRepository(
              entity,
              eventStore,
              snapshotPolicy ? snapshotStore : undefined,
              snapshotPolicy,
            ),
          )
        }
      }
      return stateManager
    })

    // Default routing strategy — payload field "id" (Gap 15)
    this.reg.registerIfAbsent(ComponentKeys.ROUTING_STRATEGY, () => payloadFieldRoutingStrategy("id"))

    // UnitOfWork runner (Plan 03-04: UoWRunner replaces UnitOfWorkFactory).
    // Entry-point semantics: gateways and event processors are top-of-stack —
    // each call should establish a fresh UoW, not reuse a snapshotted ALS
    // context that may have leaked through async boundaries (e.g., setTimeout
    // inheriting the dispatching UoW's ALS state). runInNewUoW guarantees a
    // new state per invocation, then bus.dispatch's runInUoW auto-nests.
    this.reg.registerIfAbsent(ComponentKeys.UNIT_OF_WORK_FACTORY, (config) => {
      const base: UoWRunner = runInNewUoW
      if (config.hasComponent(ComponentKeys.TRANSACTION_MANAGER)) {
        return transactionalUnitOfWorkFactory(
          base,
          config.getComponent<TransactionManager<unknown>>(ComponentKeys.TRANSACTION_MANAGER),
        )
      }
      return base
    })

    // Buses — SimpleCommandBus/SimpleQueryBus wrapped with InterceptingCommandBus/InterceptingQueryBus
    // This follows Java's pattern: SimpleCommandBus (dispatch+subscribe) + InterceptingCommandBus (interceptor chains)
    // Plan 03-04: SimpleCommandBus/QueryBus use ALS-aware runInUoW internally; no factory arg.
    this.reg.registerIfAbsent(ComponentKeys.COMMAND_BUS, (_config) =>
      createInterceptingCommandBus(createSimpleCommandBus()),
    )
    this.reg.registerIfAbsent(ComponentKeys.QUERY_BUS, (_config) =>
      createInterceptingQueryBus(createSimpleQueryBus()),
    )

    // EventSink — in ES setups, the EventStore IS the EventSink
    this.reg.registerIfAbsent(ComponentKeys.EVENT_SINK, (config) =>
      config.getComponent<EventSink>(ComponentKeys.EVENT_STORE),
    )

    // EventBus — in ES setups, the EventStore IS the EventBus
    this.reg.registerIfAbsent(ComponentKeys.EVENT_BUS, (config) =>
      config.getComponent<EventBus>(ComponentKeys.EVENT_STORE),
    )

    // Gateways — inject the configured UoWRunner so transactional wrappers
    // (registered under UNIT_OF_WORK_FACTORY) span the dispatch boundary.
    // Plan 03-04 (CTX-04 / D-34): without this, gateway calls would always
    // use the raw `runInNewUoW`, bypassing the TX wrapper.
    this.reg.registerIfAbsent(ComponentKeys.COMMAND_GATEWAY, (config) =>
      createCommandGateway(
        config.getComponent<CommandBus>(ComponentKeys.COMMAND_BUS),
        config.getComponent<UoWRunner>(ComponentKeys.UNIT_OF_WORK_FACTORY),
      ),
    )
    this.reg.registerIfAbsent(ComponentKeys.QUERY_GATEWAY, (config) =>
      createQueryGateway(
        config.getComponent<QueryBus>(ComponentKeys.QUERY_BUS),
        config.getComponent<UoWRunner>(ComponentKeys.UNIT_OF_WORK_FACTORY),
      ),
    )
    this.reg.registerIfAbsent(ComponentKeys.EVENT_GATEWAY, (config) =>
      createEventGateway(config.getComponent<EventSink>(ComponentKeys.EVENT_SINK)),
    )

    // Message monitor registry
    this.reg.registerIfAbsent(ComponentKeys.MESSAGE_MONITOR_REGISTRY, () => createMessageMonitorRegistry())

    // Note: Retrying command bus is NOT auto-registered (unlike earlier versions).
    // Users should register it explicitly if they want retrying behavior:
    //   messaging(m => m.componentRegistry(r =>
    //     r.registerDecorator("commandBus", 50, (_cfg, _name, bus) =>
    //       createRetryingCommandBus(bus, exponentialBackoffRetryPolicy()))
    //   ))

    // Correlation data — default: messageOriginProvider (correlationId + causationId)
    const providerBuilders = this.msg._correlationDataProviderBuilders
    this.reg.registerIfAbsent(
      ComponentKeys.CORRELATION_DATA_PROVIDERS,
      (config) => providerBuilders.length > 0
        ? providerBuilders.map(b => b(config))
        : [messageOriginProvider()],
    )
    this.msg._onStartCallbacks.push((config) => {
      const providers = config.getComponent<CorrelationDataProvider[]>(
        ComponentKeys.CORRELATION_DATA_PROVIDERS,
      )
      const handlerInterceptor = correlationDataHandlerInterceptor(providers)
      const dispatchInterceptor = correlationDataDispatchInterceptor()

      const cmdBus = config.getComponent<any>(ComponentKeys.COMMAND_BUS)
      if (cmdBus.registerHandlerInterceptor) cmdBus.registerHandlerInterceptor(handlerInterceptor)
      if (cmdBus.registerDispatchInterceptor) cmdBus.registerDispatchInterceptor(dispatchInterceptor)

      const qryBus = config.getComponent<any>(ComponentKeys.QUERY_BUS)
      if (qryBus.registerHandlerInterceptor) qryBus.registerHandlerInterceptor(handlerInterceptor)
      if (qryBus.registerDispatchInterceptor) qryBus.registerDispatchInterceptor(dispatchInterceptor)
    })

    // Message monitors — wire as handler interceptors on buses
    this.msg._onStartCallbacks.push((config) => {
      const monitorRegistry = config.getComponent<MessageMonitorRegistry>(ComponentKeys.MESSAGE_MONITOR_REGISTRY)

      // Command monitoring interceptor
      const cmdMonitor = monitorRegistry.commandMonitor()
      const cmdBus = config.getComponent<any>(ComponentKeys.COMMAND_BUS)
      if (cmdBus.registerHandlerInterceptor) {
        cmdBus.registerHandlerInterceptor((message: any, next: () => Promise<any>) => {
          const callback = cmdMonitor.onMessageIngested(message)
          return next().then(
            (result: any) => { callback.reportSuccess(); return result },
            (error: any) => { callback.reportFailure(error); throw error },
          )
        })
      }

      // Query monitoring interceptor
      const qryMonitor = monitorRegistry.queryMonitor()
      const qryBus = config.getComponent<any>(ComponentKeys.QUERY_BUS)
      if (qryBus.registerHandlerInterceptor) {
        qryBus.registerHandlerInterceptor((message: any, next: () => Promise<any>) => {
          const callback = qryMonitor.onMessageIngested(message)
          return next().then(
            (result: any) => { callback.reportSuccess(); return result },
            (error: any) => { callback.reportFailure(error); throw error },
          )
        })
      }

    })

    // Event processors
    if (this.msg._eventProcessorBuilders.length > 0) {
      const processorBuilders = this.msg._eventProcessorBuilders
      const evtDispatchInterceptorBuilders = this.msg._eventDispatchInterceptorBuilders
      const evtHandlerInterceptorBuilders = this.msg._eventHandlerInterceptorBuilders

      this.reg.registerIfAbsent(ComponentKeys.EVENT_PROCESSORS, (config) => {
        if (!config.hasComponent(ComponentKeys.EVENT_STORE)) return []

        const eventSource = config.getComponent<StreamableEventSource>(ComponentKeys.EVENT_STORE)
        const uowRunner = config.getComponent<UoWRunner>(ComponentKeys.UNIT_OF_WORK_FACTORY)
        const globalTokenStore = config.getOptionalComponent<TokenStore>(ComponentKeys.TOKEN_STORE)
        const queryBus = config.getComponent<QueryBus>(ComponentKeys.QUERY_BUS)

        // Event monitoring (Gap 7) — get combined event monitor for ingestion tracking
        const monitorRegistry = config.getComponent<MessageMonitorRegistry>(ComponentKeys.MESSAGE_MONITOR_REGISTRY)
        const evtMonitor = monitorRegistry.eventMonitor()

        // Build event interceptors (Gap 8)
        const evtDispatchInterceptors = evtDispatchInterceptorBuilders.map(b => b(config))
        const evtHandlerInterceptors = evtHandlerInterceptorBuilders.map(b => b(config))

        // D-44 wiring: resolve framework components once; processors inject them
        // per-event into ALS at handler-invocation entry (Plan 04-02).
        const rawCommandBus = config.getComponent<CommandBus>(ComponentKeys.COMMAND_BUS)
        const rawStateManager = config.hasComponent(ComponentKeys.STATE_MANAGER)
          ? config.getComponent<any>(ComponentKeys.STATE_MANAGER)
          : undefined

        // Per-event monitoring callback (Gap 7): registered inside the UoW via module-level
        // whenComplete/onError. The processor fires this per event delivery.
        const makeEventMonitoringCallback = () => {
          const callback = evtMonitor.onMessageIngested({
            identifier: generateIdentifier(),
            name: { namespace: "", localName: "" } as any,
            version: "0",
            payload: undefined,
            metadata: {},
            timestamp: Date.now(),
            tags: [],
          } as unknown as EventMessage)
          whenComplete(() => callback.reportSuccess())
          onError(async (err: unknown) => callback.reportFailure(err instanceof Error ? err : new Error(String(err))))
        }

        const enhancer = config.getOptionalComponent<HandlerEnhancerDefinition>(
          ComponentKeys.HANDLER_ENHANCER_DEFINITIONS,
        )

        // Build processor configs from ComponentBuilders
        const processors = processorBuilders.map(b => b(config))

        const created: Array<TrackingEventProcessor | SubscribingEventProcessor> = []
        for (const proc of processors) {
          if (proc.kind === "subscribing") {
            const subscribableSource = eventSource as unknown as SubscribableEventSource
            if (!subscribableSource.subscribe) {
              throw new Error(
                `Event source does not support subscription. ` +
                `Cannot create subscribing processor "${proc.name}".`,
              )
            }
            created.push(createSubscribingEventProcessor({
              name: proc.name,
              eventSource: subscribableSource,
              handlerGroups: proc.handlerGroups,
              stateManager: rawStateManager,
              commandBus: rawCommandBus,
              queryBus,
              onEventDelivery: makeEventMonitoringCallback,
              unitOfWorkRunner: proc.unitOfWorkRunner ?? uowRunner,
              errorHandler: proc.errorHandler,
            }))
          } else {
            created.push(createTrackingEventProcessor({
              name: proc.name,
              eventSource,
              handlerGroups: proc.handlerGroups,
              stateManager: rawStateManager,
              commandBus: rawCommandBus,
              queryBus,
              onEventDelivery: makeEventMonitoringCallback,
              unitOfWorkRunner: proc.unitOfWorkRunner ?? uowRunner,
              tokenStore: proc.tokenStore ?? globalTokenStore,
              batchSize: proc.batchSize,
              pollingIntervalMs: proc.pollingIntervalMs,
              errorHandler: proc.errorHandler,
              handlerEnhancer: enhancer,
            }))
          }
        }

        return created
      })
    }
  }
}
