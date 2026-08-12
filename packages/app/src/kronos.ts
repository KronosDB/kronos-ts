import {
  eventSourcedRepository,
  inMemoryEventStore,
  inMemorySnapshotStore,
  descriptorBasedTagResolver,
  type EventStore,
  type SnapshotPolicy,
  type SnapshotStore,
  type TagResolver,
} from "@kronos-ts/eventsourcing"
import {
  type CommandBus,
  type CommandGateway,
  type CommandHandlerDefinition,
  commandGateway,
  inMemoryTokenStore,
  queryGateway,
  simpleCommandBus,
  simpleQueryBus,
  trackingEventProcessor,
  subscribingEventProcessor,
  interceptingCommandBus,
  interceptingQueryBus,
  correlationDataDispatchInterceptor,
  messageOriginProvider,
  type CorrelationDataProvider,
  type HandlerEnhancerDefinition,
  type EventProcessor,
  type EventProcessorModule,
  type QueryBus,
  type QueryGateway,
  type QueryHandlerDefinition,
  registerCommandHandlersNatively,
  registerQueryHandlersNatively,
  runInNewUoW,
  type TokenStore,
  type UoWRunner,
} from "@kronos-ts/messaging"
import { stateManager, type StateManager, type StateModule } from "@kronos-ts/modelling"

// ---------------------------------------------------------------------------
// A functional app builder. No registry, no slots, no decorator pipeline, no
// extensions — components are a plain record you build with plain functions,
// and a module is a plain record that may bring its own.
//
// The point of the spike: everything the container does here is either (a) a
// property access, or (b) a function call you write yourself and can read.
// ---------------------------------------------------------------------------

/**
 * Everything an app needs. A record, not a key space — no string lookups.
 *
 * Deliberately NOT here: `serializer` and `transactionManager`. Their only real
 * consumers are event stores, snapshot stores and transports — i.e. backends —
 * so a backend takes them directly (`postgres({ serializer, tagResolver })`).
 * Keeping them on Components too gave two sources of truth for one value, with
 * the app-level copy silently ignored.
 */
export interface Components {
  eventStore: EventStore
  snapshotStore: SnapshotStore
  commandBus: CommandBus
  queryBus: QueryBus
  unitOfWorkFactory: UoWRunner
  tagResolver: TagResolver
  tokenStore: TokenStore
}

/**
 * Fill in whatever a caller did not supply, IN DEPENDENCY ORDER.
 *
 * This exists because component construction is ordered: `simpleCommandBus`
 * captures the UoW factory when it is built. Spreading a fully-built record under
 * a backend — `{ ...inMemoryComponents(), ...pg.components }` — therefore leaves
 * the bus holding the in-memory `runInNewUoW` even though `pg.components` supplied
 * a transactional one, and handlers run OUTSIDE the transaction. That failure is
 * silent: a row survives a rollback. Resolving here, after the merge, makes the
 * ordering impossible to get wrong from the outside.
 */
function resolveComponents(supplied: Partial<Components>): Components {
  const unitOfWorkFactory = supplied.unitOfWorkFactory ?? runInNewUoW
  return {
    eventStore: supplied.eventStore ?? inMemoryEventStore(),
    snapshotStore: supplied.snapshotStore ?? inMemorySnapshotStore(),
    // Built LAST of the bus-adjacent components, so it sees the final UoW factory.
    commandBus: supplied.commandBus ?? defaultCommandBus(unitOfWorkFactory),
    queryBus: supplied.queryBus ?? defaultQueryBus(),
    unitOfWorkFactory,
    tagResolver: supplied.tagResolver ?? descriptorBasedTagResolver(),
    tokenStore: supplied.tokenStore ?? inMemoryTokenStore(),
  }
}

/**
 * A full in-memory component record. Prefer passing a PARTIAL record to
 * `kronos` and letting it resolve the rest — see {@link resolveComponents}
 * for why spreading a complete record under a backend is a trap.
 */
export function inMemoryComponents(overrides: Partial<Components> = {}): Components {
  return resolveComponents(overrides)
}

/**
 * Per-state persistence options. Everything here is about how ONE state's
 * repository is built, which is precisely what cannot be expressed by a
 * module-level override: two states in the same module legitimately want
 * different snapshot policies, because they have different event volumes.
 *
 * Omit it and the state runs on the module's snapshot store with no policy —
 * i.e. snapshots are never written, which is the safe default.
 */
export interface StateOptions {
  /** When to write a snapshot for this state. Default: never. */
  readonly snapshotPolicy?: SnapshotPolicy
  /** A snapshot store for THIS state only. Defaults to the module's/app's. */
  readonly snapshotStore?: SnapshotStore
}

/**
 * A state with its own repository options, written as a tuple in the flat
 * registration list:
 *
 * ```ts
 * module("uni",
 *   [Course, { snapshotPolicy: afterEvents(3) }],  // tuple = state + options
 *   Student,                                       // bare state still fine
 *   createCourse,
 * )
 * ```
 */
export type StateRegistration<Id = any, S = any> = readonly [
  state: StateModule<Id, S>,
  options: StateOptions,
]

/**
 * Anything a module can register. All but one of these carry a `kind`
 * discriminator, so the author never has to sort them into buckets — the values
 * describe themselves and `kronos` partitions them. The exception is the
 * `[state, options]` tuple, which is an ARRAY and therefore has no `kind`;
 * {@link isStateRegistration} is what tells it apart, everywhere it matters.
 */
export type Registration =
  | StateModule<any, any>
  | StateRegistration
  | CommandHandlerDefinition<any, any>
  | QueryHandlerDefinition
  | EventProcessorModule

/**
 * The one registration that is not self-describing via `kind`.
 *
 * A type predicate rather than a cast, so both branches narrow honestly:
 * `Array.isArray` alone does not narrow a READONLY tuple out of a union (it is
 * typed `arg is any[]`, and `readonly [A, B]` is not assignable to `any[]`).
 */
function isStateRegistration(value: unknown): value is StateRegistration {
  return Array.isArray(value)
}

/** The in-memory query bus with correlation lineage on dispatch, mirroring the command side. */
function defaultQueryBus(): QueryBus {
  const bus = interceptingQueryBus(simpleQueryBus())
  bus.registerDispatchInterceptor(correlationDataDispatchInterceptor())
  return bus
}

/** The in-memory command bus with correlation lineage applied, as the container had. */
function defaultCommandBus(unitOfWorkFactory: UoWRunner): CommandBus {
  const bus = interceptingCommandBus(simpleCommandBus(unitOfWorkFactory))
  bus.registerDispatchInterceptor(correlationDataDispatchInterceptor())
  return bus
}

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
 * A state that wants its own repository options is written as a tuple; a bare
 * state is the same thing with `{}`:
 *
 * ```ts
 * module("uni",
 *   [Course, { snapshotPolicy: afterEvents(3) }],
 *   Student,
 *   createCourse,
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
  const hasOverrides = optionsOrFirst !== undefined && !looksLikeRegistration(optionsOrFirst)
  const overrides = (hasOverrides ? optionsOrFirst : {}) as ModuleOverrides
  const register = hasOverrides
    ? rest
    : ([optionsOrFirst, ...rest].filter(Boolean) as Registration[])
  return { name, overrides, register }
}

/**
 * Is this first argument a registration, or the optional overrides record?
 *
 * The ARRAY CHECK MUST COME FIRST. A `[state, options]` tuple has no `kind`,
 * so a `"kind" in first` test alone reads `module("uni", [Course, {...}], ...)`
 * as an overrides record — silently dropping the state AND treating a tuple as
 * a component record. Every registration is either an array (the state tuple)
 * or carries `kind`; an overrides record is neither.
 */
function looksLikeRegistration(value: ModuleOverrides | Registration): value is Registration {
  return isStateRegistration(value) || "kind" in value
}

/** A state paired with the options its repository is built from. */
interface PartitionedState {
  readonly state: StateModule<any, any>
  readonly options: StateOptions
}

/** Partition a flat registration list by the discriminator each value carries. */
function partition(register: ReadonlyArray<Registration>) {
  const states: PartitionedState[] = []
  const commands: CommandHandlerDefinition<any, any>[] = []
  const queries: QueryHandlerDefinition[] = []
  const processors: EventProcessorModule[] = []
  for (const item of register) {
    // A tuple carries no `kind`, so it has to be taken out before the switch —
    // otherwise it falls through `default` and is built as a processor.
    if (isStateRegistration(item)) {
      states.push({ state: item[0], options: item[1] ?? {} })
      continue
    }
    switch (item.kind) {
      case "state-module": states.push({ state: item as StateModule<any, any>, options: {} }); break
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
  /**
   * The LIVE processor instances, keyed by name. Distributed control planes
   * (Axon Server, KronosDB) need these to honour pause/start/split/merge and to
   * report segment status — a descriptor alone makes those instructions no-ops.
   */
  readonly processors: ReadonlyMap<string, EventProcessor>
  stop(): Promise<void>
}

/** The config shim the command/query invocation path reads at dispatch. */
function shimFor(components: Components, eventStore: EventStore, stateManager: StateManager) {
  // EXACTLY the keys commandInvocation / registerQueryHandlersNatively
  // read. The container's shim mirrored every slot "for parity"; those extra
  // entries were never read, and carrying them made Components look like it
  // owned things it does not.
  const map: Record<string, unknown> = {
    stateManager,
    eventStore,
    commandBus: components.commandBus,
    queryBus: components.queryBus,
    tagResolver: components.tagResolver,
  }
  return {
    hasComponent: (type: string) => type in map,
    getComponent: <T,>(type: string): T => {
      if (!(type in map)) throw new Error(`kronos: no component "${type}"`)
      return map[type] as T
    },
    getOptionalComponent: <T,>(type: string): T | undefined => map[type] as T | undefined,
  }
}

export function kronos(opts: {
  components?: Partial<Components>
  modules: ReadonlyArray<AppModule>
  /** Cross-cutting handler wrapper (tracing, metrics). Applied to commands, queries and processors. */
  handlerEnhancer?: HandlerEnhancerDefinition
  /** Seeds correlation data on each processed event. Defaults to messageOriginProvider(). */
  correlationDataProviders?: ReadonlyArray<CorrelationDataProvider>
}): App {
  const components = resolveComponents(opts.components ?? {})
  const handlerEnhancer = opts.handlerEnhancer
  const correlationDataProviders = opts.correlationDataProviders ?? [messageOriginProvider()]
  const stateManagers = new Map<string, StateManager>()
  const started: Array<{ start(): void; stop(): void }> = []
  const built: Array<{ start(): void; stop(): void }> = []
  const processorsByName = new Map<string, EventProcessor>()

  for (const module of opts.modules) {
    // The module's component record: the app's, with its overrides on top. One
    // spread is the whole of what slot scoping + resolution existed to express,
    // and it caps nothing — every component is overridable.
    const c: Components = resolveComponents({ ...components, ...module.overrides })

    const { states, commands, queries, processors } = partition(module.register)

    const manager = stateManager()
    for (const { state, options } of states) {
      // Per-state options win over the module's, which win over the app's.
      // `snapshotPolicy` has no component-level counterpart on purpose: "how
      // often does THIS state snapshot" is a property of the state's event
      // volume, not of the store it happens to be written to.
      manager.register(
        state,
        eventSourcedRepository(
          state,
          c.eventStore,
          options.snapshotStore ?? c.snapshotStore,
          options.snapshotPolicy,
        ),
      )
    }
    stateManagers.set(module.name, manager)

    const config = shimFor(c, c.eventStore, manager)

    registerCommandHandlersNatively(commands, {
      commandBus: c.commandBus,
      config,
      moduleName: module.name,
      ...(handlerEnhancer ? { handlerEnhancer } : {}),
      correlationDataProviders,
    })
    registerQueryHandlersNatively(queries, {
      queryBus: c.queryBus,
      moduleName: module.name,
      config,
      ...(handlerEnhancer ? { handlerEnhancer } : {}),
      correlationDataProviders,
    })

    // Processors are BUILT here but not started — see the two-phase note below.
    for (const proc of processors) {
      if (proc.kind === "subscribing") {
        const subscribable = c.eventStore as unknown as { subscribe?: unknown }
        if (!subscribable.subscribe) {
          throw new Error(
            `Event source does not support subscription. Cannot create subscribing processor "${proc.name}".`,
          )
        }
        built.push(
          subscribingEventProcessor({
            name: proc.name,
            eventSource: c.eventStore as never,
            eventHandlers: proc.eventHandlers,
            stateManager: manager,
            commandBus: c.commandBus,
            queryBus: c.queryBus,
            correlationDataProviders,
            unitOfWorkRunner: proc.unitOfWorkRunner ?? c.unitOfWorkFactory,
            ...(proc.errorHandler ? { errorHandler: proc.errorHandler } : {}),
            ...(handlerEnhancer ? { handlerEnhancer } : {}),
          }) as never,
        )
        continue
      }
      built.push(
        trackingEventProcessor({
          name: proc.name,
          eventSource: c.eventStore as never,
          eventHandlers: proc.eventHandlers,
          stateManager: manager,
          commandBus: c.commandBus,
          queryBus: c.queryBus,
          correlationDataProviders,
          unitOfWorkRunner: proc.unitOfWorkRunner ?? c.unitOfWorkFactory,
          // Per-processor override wins over the module/app token store.
          tokenStore: proc.tokenStore ?? c.tokenStore,
          // Everything else the builder accepts. Dropping any of these makes
          // `.errorHandler(...)` / `.deadLetterQueue(...)` compile and silently
          // do nothing — which is how a projection's propagate-never-skip
          // semantics would quietly become an automation's.
          ...(proc.batchSize !== undefined ? { batchSize: proc.batchSize } : {}),
          ...(proc.pollingIntervalMs !== undefined ? { pollingIntervalMs: proc.pollingIntervalMs } : {}),
          ...(proc.errorHandler ? { errorHandler: proc.errorHandler } : {}),
          ...(proc.deadLetterQueue ? { deadLetterQueue: proc.deadLetterQueue } : {}),
          ...(proc.enqueuePolicy ? { enqueuePolicy: proc.enqueuePolicy } : {}),
          ...(proc.sequencingPolicy ? { sequencingPolicy: proc.sequencingPolicy } : {}),
          ...(proc.deadLetterListener ? { deadLetterListener: proc.deadLetterListener } : {}),
          ...(proc.resetClearsDeadLetters !== undefined
            ? { resetClearsDeadLetters: proc.resetClearsDeadLetters }
            : {}),
          ...(proc.dlqRetryIntervalMs !== undefined ? { dlqRetryIntervalMs: proc.dlqRetryIntervalMs } : {}),
          ...(proc.initialSegmentCount !== undefined
            ? { initialSegmentCount: proc.initialSegmentCount }
            : {}),
          ...(proc.claimExtensionThresholdMs !== undefined
            ? { claimExtensionThresholdMs: proc.claimExtensionThresholdMs }
            : {}),
          ...(proc.tokenClaimIntervalMs !== undefined
            ? { tokenClaimIntervalMs: proc.tokenClaimIntervalMs }
            : {}),
          ...(proc.onReset ? { onReset: proc.onReset } : {}),
          ...(handlerEnhancer ? { handlerEnhancer } : {}),
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
    processorsByName.set((processor as unknown as EventProcessor).name, processor as unknown as EventProcessor)
    processor.start()
    started.push(processor)
  }

  return {
    commandGateway: commandGateway(components.commandBus),
    queryGateway: queryGateway(components.queryBus, components.unitOfWorkFactory),
    stateManagers,
    processors: processorsByName,
    async stop() {
      for (const p of started) p.stop()
    },
  }
}
