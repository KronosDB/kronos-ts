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
 * Anything a module can register. Every one of these already carries a `kind`
 * discriminator, so the author never has to sort them into buckets — the values
 * describe themselves and `createApp` partitions them.
 */
export type Registration =
  | StateModule<any, any>
  | CommandHandlerDefinition<any, any>
  | QueryHandlerDefinition
  | EventProcessorModule

/**
 * A module: a name, optionally its OWN persistence, and a flat list of what it
 * registers. Where the container needed slot inheritance and scope resolution,
 * this needs `??`.
 */
export interface AppModule {
  readonly name: string
  /** Components this module runs on instead of the app's. Any of them. */
  readonly overrides: ModuleOverrides
  /** Everything the module contributes, in any order. */
  readonly register: ReadonlyArray<Registration>
}

/**
 * Whatever a module wants to run on instead of the app's. ANY component is
 * overridable — nothing here privileges persistence. Messaging trickling down
 * from the app while the event store is module-scoped is the *common* shape,
 * not the only one.
 *
 *   module("billing", {
 *     eventStore: postgresEventStore(pool),
 *     tokenStore: postgresTokenStore(pool),
 *   }, ...slices)
 *
 * Each component is named on its own. There is deliberately no `postgres(pool)`
 * bundle: an event store and a token store are different things with different
 * schemas and lifecycles, and they are separable — cursors in Postgres while the
 * log lives in KronosDB is a legitimate arrangement. Naming both costs one line
 * and keeps that true.
 *
 * Omit the record and the module inherits everything.
 */
export type ModuleOverrides = Partial<Components>

/**
 * Declare a module. A factory like `commandHandler` / `state` are factories —
 * name, optional persistence, then everything it registers, variadically:
 *
 * ```ts
 * const billing = module("billing", { eventStore: postgresEventStore(pool) },
 *   ...billLinesSlice(deps),
 *   ...creditLineSlice(deps),
 * )
 * ```
 *
 * Persistence is optional — `module("ordering", ...registrations)` inherits the
 * app's stores — and is told apart from registrations by the `kind`
 * discriminator every registration carries.
 */
export function module(name: string, ...register: Registration[]): AppModule
export function module(
  name: string,
  overrides: ModuleOverrides,
  ...register: Registration[]
): AppModule
export function module(
  name: string,
  optionsOrFirst?: ModuleOverrides | Registration,
  ...rest: Registration[]
): AppModule {
  const hasOptions =
    optionsOrFirst !== undefined && !("kind" in (optionsOrFirst as { kind?: unknown }))
  const overrides = (hasOptions ? optionsOrFirst : {}) as ModuleOverrides
  const register = hasOptions ? rest : ([optionsOrFirst, ...rest].filter(Boolean) as Registration[])
  return { name, overrides, register }
}

/** Partition a flat registration list by the discriminator each value carries. */
function partition(register: ReadonlyArray<Registration>) {
  const states: StateModule<any, any>[] = []
  const commands: CommandHandlerDefinition<any, any>[] = []
  const queries: QueryHandlerDefinition[] = []
  const processors: EventProcessorModule[] = []
  for (const item of register) {
    switch ((item as { kind: string }).kind) {
      case "state-module": states.push(item as StateModule<any, any>); break
      case "command-handler": commands.push(item as CommandHandlerDefinition<any, any>); break
      case "query-handler": queries.push(item as QueryHandlerDefinition); break
      default: processors.push(item as EventProcessorModule); break
    }
  }
  return { states, commands, queries, processors }
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
  const built: Array<{ start(): void; stop(): void }> = []

  for (const module of opts.modules) {
    // The module's component record: the app's, with its overrides on top. One
    // spread is the whole of what slot scoping + resolution existed to express,
    // and it caps nothing — every component is overridable.
    const c: Components = { ...components, ...module.overrides }

    const { states, commands, queries, processors } = partition(module.register)

    const stateManager = createStateManager()
    for (const state of states) {
      stateManager.register(
        state,
        createEventSourcedRepository(state, c.eventStore, c.snapshotStore, undefined),
      )
    }
    stateManagers.set(module.name, stateManager)

    const config = shimFor(c, c.eventStore, stateManager)

    registerCommandHandlersNatively(commands, {
      commandBus: c.commandBus,
      config,
      moduleName: module.name,
    })
    registerQueryHandlersNatively(queries, {
      queryBus: c.queryBus,
      moduleName: module.name,
      config,
    })

    // Processors are BUILT here but not started — see the two-phase note below.
    for (const proc of processors) {
      built.push(
        createTrackingEventProcessor({
          name: proc.name,
          eventSource: c.eventStore as never,
          eventHandlers: proc.eventHandlers,
          stateManager,
          commandBus: c.commandBus,
          queryBus: c.queryBus,
          correlationDataProviders: [],
          unitOfWorkRunner: proc.unitOfWorkRunner ?? c.unitOfWorkFactory,
          tokenStore: c.tokenStore,
        }),
      )
    }
  }

  // TWO-PHASE START. Every module's handlers are subscribed before ANY
  // processor runs, so an automation replaying from a cold store can never
  // dispatch to a command that is not yet registered. With one app per process
  // this removes module boot-ordering as a concern entirely: N-instance setups
  // need a consumer-first topological order precisely because each instance
  // starts replaying the moment it boots.
  for (const processor of built) {
    processor.start()
    started.push(processor)
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
