import type { z } from "zod"
import type {
  ComponentBuilder,
  ComponentRegistry,
  ConfigurationEnhancer,
  LifecycleRegistry,
} from "@kronos-ts/common"
import type {
  CommandHandlerDefinition,
  CommandHandlerContext,
  CommandDescriptor,
  CommandMessage,
  CommandBus,
  QueryHandlersDefinition,
  QueryHandlerRegistration,
  QueryMessage,
  QueryBus,
  EventHandlersDefinition,
  EventHandlerRegistration,
  EventMessage,
  EventBus,
  EventSink,
  EventGateway,
  EventProcessorModule,
  DispatchInterceptor,
  HandlerInterceptor,
  HandlerEnhancerDefinition,
  CorrelationDataProvider,
  UnitOfWorkFactory,
  TokenStore,
  EventProcessingErrorHandler,
  SequencedDeadLetterQueue,
  MessageMonitor,
  TransactionManager,
  RoutingStrategy,
} from "@kronos-ts/messaging"
import type { Serializer } from "@kronos-ts/common"
import {
  commandHandler as createCommandHandlerDef,
  eventHandlers,
  queryHandlers as createQueryHandlersDef,
  trackingProcessor as createTrackingProcessorBuilder,
  subscribingProcessor as createSubscribingProcessorBuilder,
} from "@kronos-ts/messaging"
import type {
  EntityModule,
  EntityLifecycle,
  IdSchema,
  InferIdFromSchema,
} from "@kronos-ts/modelling"
import { eventSourcedEntity as createEntityModule } from "@kronos-ts/modelling"
import type { EventCriteria, EvolverRegistration } from "@kronos-ts/messaging"
import {
  EventSourcingConfigurer,
  type KronosApplication,
} from "./eventsourcing-configurer.js"
import type { EventStore } from "./event-store.js"
import type { EventStorageEngine } from "./event-storage-engine.js"
import type { SnapshotStore } from "./snapshot-store.js"
import type { SnapshotPolicy } from "./snapshot-policy.js"
import type { TagResolver } from "./tag-resolver.js"

/**
 * A Kronos plugin — a function that receives a Kronos instance and registers
 * domain components with it.
 *
 * Domain slices are plugins:
 * ```typescript
 * function courses(k: Kronos) {
 *   const CourseEntity = k.eventSourcedEntity({ ... })
 *   k.commandHandler(CreateCourse, async (cmd, { load, append }) => { ... })
 *   k.trackingProcessor("course-projection", [ ... ])
 *   k.queryHandlers("course-queries", [ ... ])
 * }
 * ```
 */
export type KronosPlugin = (k: Kronos) => void

// ---------------------------------------------------------------------------
// Options for processors
// ---------------------------------------------------------------------------

export interface TrackingProcessorOptions {
  batchSize?: number
  pollingIntervalMs?: number
  tokenStore?: TokenStore
  unitOfWorkFactory?: UnitOfWorkFactory
  errorHandler?: EventProcessingErrorHandler
  deadLetterQueue?: SequencedDeadLetterQueue
  initialSegmentCount?: number
  sequencedBy?: (event: unknown) => unknown
  onReset?: () => Promise<void> | void
}

export interface SubscribingProcessorOptions {
  unitOfWorkFactory?: UnitOfWorkFactory
  errorHandler?: EventProcessingErrorHandler
  sequencedBy?: (event: unknown) => unknown
  onReset?: () => Promise<void> | void
}

// ---------------------------------------------------------------------------
// Kronos
// ---------------------------------------------------------------------------

/**
 * Plugin-based configuration for Kronos applications.
 *
 * Provides a flat, TypeScript-native API where defining a component also
 * registers it — no separate configuration step needed.
 *
 * ```typescript
 * const app = await kronos()
 *   .register(courses)
 *   .register(enrollment)
 *   .start()
 * ```
 */
export class Kronos {
  /** @internal */
  private readonly _configurer: EventSourcingConfigurer

  /** @internal */
  constructor(options?: { eventStore?: EventStore }) {
    this._configurer = EventSourcingConfigurer.create(options)
  }

  // =========================================================================
  // state
  // =========================================================================

  /**
   * Define and register an event-sourced state inline. Returns the
   * EntityModule for use in `load()` calls within command handlers.
   *
   * The state type is inferred from the `initial` function — no separate
   * type definition needed.
   *
   * ```typescript
   * const Course = k.state({
   *   name: "Course",
   *   id: { courseId: z.string() },
   *   initial: () => ({ created: false, name: "", capacity: 0 }),
   *   criteria: (id) => EventCriteria.havingTags({ courseId: id.courseId }),
   *   evolve: [
   *     on(CourseCreated, (s, e) => ({ ...s, created: true, name: e.name })),
   *   ],
   * })
   * ```
   */
  state<IS extends IdSchema, S>(def: {
    name: string
    id: IS
    initial: (id: InferIdFromSchema<IS>) => S
    criteria: (id: InferIdFromSchema<IS>) => EventCriteria
    evolve: Array<EvolverRegistration<S, any>>
    lifecycle?: EntityLifecycle<InferIdFromSchema<IS>, S>
    snapshotPolicy?: SnapshotPolicy
  }): EntityModule<InferIdFromSchema<IS>, S>

  /**
   * Register a pre-built EntityModule.
   */
  state<Id, S>(
    entity: EntityModule<Id, S>,
    options?: { snapshotPolicy?: SnapshotPolicy },
  ): this

  state(defOrEntity: EntityModule | Record<string, unknown>, options?: { snapshotPolicy?: SnapshotPolicy }): EntityModule | this {
    if ((defOrEntity as EntityModule).kind === "entity-module") {
      this._configurer.registerEntity(defOrEntity as EntityModule, options)
      return this
    }
    const def = defOrEntity as Record<string, unknown>
    const { snapshotPolicy, ...entityDef } = def
    const entity = createEntityModule(entityDef as Parameters<typeof createEntityModule>[0])
    this._configurer.registerEntity(
      entity,
      snapshotPolicy ? { snapshotPolicy: snapshotPolicy as SnapshotPolicy } : undefined,
    )
    return entity
  }


  // =========================================================================
  // commandHandler
  // =========================================================================

  /**
   * Define and register a command handler inline (void command).
   */
  commandHandler<P extends z.ZodType>(
    descriptor: CommandDescriptor<P, undefined>,
    handler: (
      command: z.infer<P>,
      context: CommandHandlerContext,
    ) => Promise<void> | void,
  ): this

  /**
   * Define and register a command handler inline (typed result).
   */
  commandHandler<P extends z.ZodType, R extends z.ZodType>(
    descriptor: CommandDescriptor<P, R>,
    handler: (
      command: z.infer<P>,
      context: CommandHandlerContext,
    ) => Promise<z.infer<R>> | z.infer<R>,
  ): this

  /**
   * Define and register a command handler inline with options
   * (handler + optional appendCondition override).
   */
  commandHandler<P extends z.ZodType>(
    descriptor: CommandDescriptor<P, undefined>,
    options: {
      handler: (
        command: z.infer<P>,
        context: CommandHandlerContext,
      ) => Promise<void> | void
      appendCondition?: (
        command: z.infer<P>,
        sourcedCriteria: EventCriteria,
      ) => EventCriteria
    },
  ): this

  /**
   * Register a pre-built CommandHandlerDefinition.
   */
  commandHandler<P extends z.ZodType, R extends z.ZodType | undefined>(definition: CommandHandlerDefinition<P, R>): this

  commandHandler(
    descriptorOrDef: CommandDescriptor | CommandHandlerDefinition,
    handlerOrOptions?: Function | Record<string, unknown>,
  ): this {
    if ("kind" in descriptorOrDef && descriptorOrDef.kind === "command-handler") {
      this._configurer.registerCommandHandler(() => descriptorOrDef as CommandHandlerDefinition)
      return this
    }
    const def = createCommandHandlerDef(descriptorOrDef as CommandDescriptor, handlerOrOptions as Parameters<typeof createCommandHandlerDef>[1])
    this._configurer.registerCommandHandler(() => def)
    return this
  }

  // =========================================================================
  // queryHandlers
  // =========================================================================

  /**
   * Define and register query handlers inline.
   */
  queryHandlers(
    name: string,
    handlers: QueryHandlerRegistration<any, any>[],
  ): this

  /**
   * Register a pre-built QueryHandlersDefinition.
   */
  queryHandlers(definition: QueryHandlersDefinition): this

  queryHandlers(
    nameOrDef: string | QueryHandlersDefinition,
    handlers?: QueryHandlerRegistration<any, any>[],
  ): this {
    if (typeof nameOrDef === "object" && nameOrDef.kind === "query-handlers") {
      this._configurer.registerQueryHandlers(() => nameOrDef)
      return this
    }
    const def = createQueryHandlersDef({
      name: nameOrDef as string,
      handlers: handlers!,
    })
    this._configurer.registerQueryHandlers(() => def)
    return this
  }

  // =========================================================================
  // trackingProcessor
  // =========================================================================

  /**
   * Define and register a tracking event processor with inline handler
   * registrations (from `on()` calls).
   */
  trackingProcessor(
    name: string,
    handlers: EventHandlerRegistration<any>[],
    options?: TrackingProcessorOptions,
  ): this

  /**
   * Define and register a tracking event processor with pre-built
   * EventHandlersDefinition groups.
   */
  trackingProcessor(
    name: string,
    handlerGroups: EventHandlersDefinition[],
    options?: TrackingProcessorOptions,
  ): this

  /**
   * Register a pre-built EventProcessorModule.
   */
  trackingProcessor(module: EventProcessorModule): this

  trackingProcessor(
    nameOrModule: string | EventProcessorModule,
    handlersOrGroups?: Array<EventHandlerRegistration | EventHandlersDefinition>,
    options?: TrackingProcessorOptions,
  ): this {
    if (typeof nameOrModule === "object" && "kind" in nameOrModule) {
      this._configurer.registerEventProcessor(() => nameOrModule)
      return this
    }

    const name = nameOrModule as string
    const items = handlersOrGroups ?? []

    // Determine if items are raw EventHandlerRegistrations or EventHandlersDefinitions
    let handlerGroups: EventHandlersDefinition[]
    if (items.length > 0 && "kind" in items[0] && items[0].kind === "event-handlers") {
      handlerGroups = items as EventHandlersDefinition[]
    } else {
      handlerGroups = [
        eventHandlers({
          name,
          handlers: items as EventHandlerRegistration<any>[],
          sequencedBy: options?.sequencedBy,
          onReset: options?.onReset,
        }),
      ]
    }

    const builder = createTrackingProcessorBuilder(name)
    for (const group of handlerGroups) {
      builder.registerEventHandler(group)
    }
    if (options?.batchSize) builder.batchSize(options.batchSize)
    if (options?.pollingIntervalMs) builder.pollingIntervalMs(options.pollingIntervalMs)
    if (options?.tokenStore) builder.tokenStore(options.tokenStore)
    if (options?.unitOfWorkFactory) builder.unitOfWorkFactory(options.unitOfWorkFactory)
    if (options?.errorHandler) builder.errorHandler(options.errorHandler)
    if (options?.deadLetterQueue) builder.deadLetterQueue(options.deadLetterQueue)
    if (options?.initialSegmentCount) builder.initialSegmentCount(options.initialSegmentCount)

    this._configurer.registerEventProcessor(() => builder.build())
    return this
  }

  // =========================================================================
  // subscribingProcessor
  // =========================================================================

  /**
   * Define and register a subscribing event processor with inline handlers.
   */
  subscribingProcessor(
    name: string,
    handlers: EventHandlerRegistration<any>[],
    options?: SubscribingProcessorOptions,
  ): this

  /**
   * Define and register a subscribing event processor with pre-built groups.
   */
  subscribingProcessor(
    name: string,
    handlerGroups: EventHandlersDefinition[],
    options?: SubscribingProcessorOptions,
  ): this

  /**
   * Register a pre-built subscribing EventProcessorModule.
   */
  subscribingProcessor(module: EventProcessorModule): this

  subscribingProcessor(
    nameOrModule: string | EventProcessorModule,
    handlersOrGroups?: Array<EventHandlerRegistration | EventHandlersDefinition>,
    options?: SubscribingProcessorOptions,
  ): this {
    if (typeof nameOrModule === "object" && "kind" in nameOrModule) {
      this._configurer.registerEventProcessor(() => nameOrModule)
      return this
    }

    const name = nameOrModule as string
    const items = handlersOrGroups ?? []

    let handlerGroups: EventHandlersDefinition[]
    if (items.length > 0 && "kind" in items[0] && items[0].kind === "event-handlers") {
      handlerGroups = items as EventHandlersDefinition[]
    } else {
      handlerGroups = [
        eventHandlers({
          name,
          handlers: items as EventHandlerRegistration<any>[],
          sequencedBy: options?.sequencedBy,
          onReset: options?.onReset,
        }),
      ]
    }

    const builder = createSubscribingProcessorBuilder(name)
    for (const group of handlerGroups) {
      builder.registerEventHandler(group)
    }
    if (options?.unitOfWorkFactory) builder.unitOfWorkFactory(options.unitOfWorkFactory)
    if (options?.errorHandler) builder.errorHandler(options.errorHandler)

    this._configurer.registerEventProcessor(() => builder.build())
    return this
  }

  // =========================================================================
  // register — plugins and enhancers
  // =========================================================================

  /**
   * Register a plugin (domain slice) or a ConfigurationEnhancer.
   *
   * Domain slices:
   * ```typescript
   * kronos().register(courses).register(enrollment).start()
   * ```
   *
   * Infrastructure enhancers (e.g., Axon Server connector):
   * ```typescript
   * kronos().register(axonServerConfigurationEnhancer({ ... })).start()
   * ```
   */
  register(plugin: KronosPlugin): this
  register(enhancer: ConfigurationEnhancer): this
  register(pluginOrEnhancer: KronosPlugin | ConfigurationEnhancer): this {
    if (typeof pluginOrEnhancer === "function") {
      pluginOrEnhancer(this)
    } else {
      this._configurer.registerEnhancer(pluginOrEnhancer)
    }
    return this
  }

  // =========================================================================
  // Infrastructure overrides
  // =========================================================================

  /** Override the event store implementation. */
  eventStore(builder: ComponentBuilder<EventStore>): this {
    this._configurer.registerEventStore(builder)
    return this
  }

  /** Override the event storage engine (raw persistence backend). */
  eventStorageEngine(builder: ComponentBuilder<EventStorageEngine>): this {
    this._configurer.registerEventStorageEngine(builder)
    return this
  }

  /** Override the tag resolver. */
  tagResolver(builder: ComponentBuilder<TagResolver>): this {
    this._configurer.registerTagResolver(builder)
    return this
  }

  /** Override the snapshot store. */
  snapshotStore(builder: ComponentBuilder<SnapshotStore>): this {
    this._configurer.componentRegistry((r) => r.register("snapshotStore", builder))
    return this
  }

  /** Override the token store (used by tracking processors). */
  tokenStore(builder: ComponentBuilder<TokenStore>): this {
    this._configurer.componentRegistry((r) => r.register("tokenStore", builder))
    return this
  }

  /** Register a transaction manager. */
  transactionManager(builder: ComponentBuilder<TransactionManager>): this {
    this._configurer.componentRegistry((r) => r.register("transactionManager", builder))
    return this
  }

  /** Override the serializer. */
  serializer(builder: ComponentBuilder<Serializer>): this {
    this._configurer.componentRegistry((r) => r.register("serializer", builder))
    return this
  }

  /** Override the command bus. */
  commandBus(builder: ComponentBuilder<CommandBus>): this {
    this._configurer.messaging((m) => m.registerCommandBus(builder))
    return this
  }

  /** Override the query bus. */
  queryBus(builder: ComponentBuilder<QueryBus>): this {
    this._configurer.messaging((m) => m.registerQueryBus(builder))
    return this
  }

  /** Override the event bus. */
  eventBus(builder: ComponentBuilder<EventBus>): this {
    this._configurer.messaging((m) => m.registerEventBus(builder))
    return this
  }

  /** Override the event sink. */
  eventSink(builder: ComponentBuilder<EventSink>): this {
    this._configurer.messaging((m) => m.registerEventSink(builder))
    return this
  }

  /** Override the event gateway. */
  eventGateway(builder: ComponentBuilder<EventGateway>): this {
    this._configurer.messaging((m) => m.registerEventGateway(builder))
    return this
  }

  /** Override the UnitOfWork factory. */
  unitOfWorkFactory(builder: ComponentBuilder<UnitOfWorkFactory>): this {
    this._configurer.messaging((m) => m.registerUnitOfWorkFactory(builder))
    return this
  }

  /** Override the routing strategy. */
  routingStrategy(builder: ComponentBuilder<RoutingStrategy>): this {
    this._configurer.componentRegistry((r) => r.register("routingStrategy", builder))
    return this
  }

  // =========================================================================
  // Cross-cutting concerns
  // =========================================================================

  /** Register a command dispatch interceptor. */
  commandDispatchInterceptor(builder: ComponentBuilder<DispatchInterceptor<CommandMessage>>): this {
    this._configurer.messaging((m) => m.registerCommandDispatchInterceptor(builder))
    return this
  }

  /** Register a command handler interceptor. */
  commandHandlerInterceptor(builder: ComponentBuilder<HandlerInterceptor>): this {
    this._configurer.messaging((m) => m.registerCommandHandlerInterceptor(builder))
    return this
  }

  /** Register a query dispatch interceptor. */
  queryDispatchInterceptor(builder: ComponentBuilder<DispatchInterceptor<QueryMessage>>): this {
    this._configurer.messaging((m) => m.registerQueryDispatchInterceptor(builder))
    return this
  }

  /** Register a query handler interceptor. */
  queryHandlerInterceptor(builder: ComponentBuilder<HandlerInterceptor>): this {
    this._configurer.messaging((m) => m.registerQueryHandlerInterceptor(builder))
    return this
  }

  /** Register an event dispatch interceptor. */
  eventDispatchInterceptor(builder: ComponentBuilder<DispatchInterceptor<EventMessage>>): this {
    this._configurer.messaging((m) => m.registerEventDispatchInterceptor(builder))
    return this
  }

  /** Register an event handler interceptor. */
  eventHandlerInterceptor(builder: ComponentBuilder<HandlerInterceptor>): this {
    this._configurer.messaging((m) => m.registerEventHandlerInterceptor(builder))
    return this
  }

  /** Register a correlation data provider. */
  correlationDataProvider(builder: ComponentBuilder<CorrelationDataProvider>): this {
    this._configurer.messaging((m) => m.registerCorrelationDataProvider(builder))
    return this
  }

  /** Register a message monitor. */
  messageMonitor(builder: ComponentBuilder<MessageMonitor>): this {
    this._configurer.messaging((m) => m.registerMessageMonitor(builder))
    return this
  }

  // =========================================================================
  // Escape hatches
  // =========================================================================

  /** Direct access to the component registry for advanced configuration. */
  componentRegistry(fn: (registry: ComponentRegistry) => void): this {
    this._configurer.componentRegistry(fn)
    return this
  }

  /** Direct access to the lifecycle registry. */
  lifecycleRegistry(fn: (registry: LifecycleRegistry) => void): this {
    this._configurer.lifecycleRegistry(fn)
    return this
  }

  // =========================================================================
  // Lifecycle
  // =========================================================================

  /** Build the application (does not start processors). */
  build(): KronosApplication {
    return this._configurer.build()
  }

  /** Build and start the application. */
  async start(): Promise<KronosApplication> {
    return this._configurer.start()
  }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/**
 * Create a Kronos application configuration.
 *
 * ```typescript
 * // Domain slices as plugins
 * function courses(k: Kronos) {
 *   const CourseEntity = k.eventSourcedEntity({ ... })
 *   k.commandHandler(CreateCourse, async (cmd, { load, append }) => { ... })
 *   k.trackingProcessor("course-projection", [ ... ])
 *   k.queryHandlers("course-queries", [ ... ])
 * }
 *
 * // Bootstrap
 * const app = await kronos()
 *   .register(courses)
 *   .register(enrollment)
 *   .start()
 *
 * await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro" })
 * ```
 */
export function kronos(options?: { eventStore?: EventStore }): Kronos {
  return new Kronos(options)
}
