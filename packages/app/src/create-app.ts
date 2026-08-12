import type { Serializer } from "@kronos-ts/common"
import {
  createEventSourcedRepository,
  createInMemoryEventStore,
  createInMemorySnapshotStore,
  descriptorBasedTagResolver,
  type EventStore,
  type SnapshotStore,
  type TagResolver,
} from "@kronos-ts/eventsourcing"
import {
  type CommandBus,
  type CommandGateway,
  type CommandHandlerDefinition,
  createCommandGateway,
  createInMemoryTokenStore,
  createQueryGateway,
  createSimpleCommandBus,
  createSimpleQueryBus,
  createTrackingEventProcessor,
  jsonSerializer,
  type EventProcessorModule,
  noTransactionManager,
  type QueryBus,
  type QueryGateway,
  type QueryHandlerDefinition,
  registerCommandHandlersNatively,
  registerQueryHandlersNatively,
  runInNewUoW,
  type TokenStore,
  type TransactionManager,
  type UoWRunner,
} from "@kronos-ts/messaging"
import { createStateManager, type StateManager, type StateModule } from "@kronos-ts/modelling"

// ---------------------------------------------------------------------------
// A functional app builder. No registry, no slots, no decorator pipeline, no
// extensions — components are a plain record you build with plain functions,
// and a module is a plain record that may bring its own.
//
// The point of the spike: everything the container does here is either (a) a
// property access, or (b) a function call you write yourself and can read.
// ---------------------------------------------------------------------------

/** Everything an app needs. A record, not a key space — no string lookups. */
export interface Components {
  eventStore: EventStore
  snapshotStore: SnapshotStore
  commandBus: CommandBus
  queryBus: QueryBus
  serializer: Serializer
  unitOfWorkFactory: UoWRunner
  tagResolver: TagResolver
  tokenStore: TokenStore
  transactionManager: TransactionManager
}

/** The zero-config path, as a function returning a value you can spread. */
export function inMemoryComponents(overrides: Partial<Components> = {}): Components {
  const unitOfWorkFactory = overrides.unitOfWorkFactory ?? runInNewUoW
  return {
    eventStore: createInMemoryEventStore(),
    snapshotStore: createInMemorySnapshotStore(),
    commandBus: createSimpleCommandBus(unitOfWorkFactory),
    queryBus: createSimpleQueryBus(),
    serializer: jsonSerializer(),
    unitOfWorkFactory,
    tagResolver: descriptorBasedTagResolver(),
    tokenStore: createInMemoryTokenStore(),
    transactionManager: noTransactionManager(),
    ...overrides,
  }
}

/**
 * A module: a name, optionally its OWN event store, and the things it
 * registers. Where the container needed slot inheritance and scope resolution,
 * this needs `??`.
 */
export interface AppModule {
  readonly name: string
  /** The module's own event store. Falls back to the app's. */
  readonly eventStore?: EventStore
  /** The module's own token store (processor cursors). Falls back to the app's. */
  readonly tokenStore?: TokenStore
  readonly states?: ReadonlyArray<StateModule<any, any>>
  readonly commands?: ReadonlyArray<CommandHandlerDefinition<any, any>>
  readonly queries?: ReadonlyArray<QueryHandlerDefinition>
  readonly processors?: ReadonlyArray<EventProcessorModule>
}

export interface App {
  readonly commandGateway: CommandGateway
  readonly queryGateway: QueryGateway
  /** Per-module state managers, keyed by module name — for tests/introspection. */
  readonly stateManagers: ReadonlyMap<string, StateManager>
  stop(): Promise<void>
}

/** The config shim the command/query invocation path reads at dispatch. */
function shimFor(components: Components, eventStore: EventStore, stateManager: StateManager) {
  const map: Record<string, unknown> = {
    stateManager,
    eventStore,
    commandBus: components.commandBus,
    queryBus: components.queryBus,
    snapshotStore: components.snapshotStore,
    serializer: components.serializer,
    unitOfWorkFactory: components.unitOfWorkFactory,
    tagResolver: components.tagResolver,
    tokenStore: components.tokenStore,
    transactionManager: components.transactionManager,
  }
  return {
    hasComponent: (type: string) => type in map,
    getComponent: <T,>(type: string): T => {
      if (!(type in map)) throw new Error(`createApp: no component "${type}"`)
      return map[type] as T
    },
    getOptionalComponent: <T,>(type: string): T | undefined => map[type] as T | undefined,
  }
}

export function createApp(opts: { components?: Components; modules: ReadonlyArray<AppModule> }): App {
  const components = opts.components ?? inMemoryComponents()
  const stateManagers = new Map<string, StateManager>()
  const started: Array<{ start(): void; stop(): void }> = []

  for (const module of opts.modules) {
    // A module's own store, or the app's. This one line is the whole of what
    // slot scoping + resolution existed to express.
    const eventStore = module.eventStore ?? components.eventStore
    const tokenStore = module.tokenStore ?? components.tokenStore

    const stateManager = createStateManager()
    for (const state of module.states ?? []) {
      stateManager.register(
        state,
        createEventSourcedRepository(state, eventStore, components.snapshotStore, undefined),
      )
    }
    stateManagers.set(module.name, stateManager)

    const config = shimFor(components, eventStore, stateManager)

    registerCommandHandlersNatively(module.commands ?? [], {
      commandBus: components.commandBus,
      config,
      moduleName: module.name,
    })
    registerQueryHandlersNatively(module.queries ?? [], {
      queryBus: components.queryBus,
      moduleName: module.name,
      config,
    })

    for (const proc of module.processors ?? []) {
      const built = createTrackingEventProcessor({
        name: proc.name,
        eventSource: eventStore as never,
        eventHandlers: proc.eventHandlers,
        stateManager,
        commandBus: components.commandBus,
        queryBus: components.queryBus,
        correlationDataProviders: [],
        unitOfWorkRunner: proc.unitOfWorkRunner ?? components.unitOfWorkFactory,
        tokenStore,
      })
      built.start()
      started.push(built)
    }
  }

  return {
    commandGateway: createCommandGateway(components.commandBus),
    queryGateway: createQueryGateway(components.queryBus, components.unitOfWorkFactory),
    stateManagers,
    async stop() {
      for (const p of started) p.stop()
    },
  }
}
