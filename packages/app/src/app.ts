import type { StateModule } from "@kronos-ts/modelling"
import { moduleScopesOf } from "./module-scope.js"
import { createStateManager, type StateManager } from "@kronos-ts/modelling"
import type {
  CommandHandlerDefinition,
  QueryHandlerDefinition,
  EventProcessorModule,
  CommandGateway,
  QueryGateway,
  CommandMessage,
  QueryMessage,
  EventMessage,
  DispatchInterceptor,
  HandlerInterceptor,
  HandlerEnhancerDefinition,
  EventProcessor,
  TrackingEventProcessor,
  SubscribingEventProcessor,
  StreamableEventSource,
  SubscribableEventSource,
  CorrelationDataProvider,
} from "@kronos-ts/messaging"
import {
  registerCommandHandlersNatively,
  registerQueryHandlersNatively,
  createCommandGateway,
  createQueryGateway,
  createTrackingEventProcessor,
  createSubscribingEventProcessor,
  multiHandlerEnhancerDefinition,
  messageOriginProvider,
  correlationDataHandlerInterceptor,
  correlationDataDispatchInterceptor,
} from "@kronos-ts/messaging"
import { createEventSourcedRepository } from "@kronos-ts/eventsourcing"
import type { SnapshotPolicy, SnapshotStore } from "@kronos-ts/eventsourcing"
import type { MinimalConfiguration } from "@kronos-ts/messaging"
import { ALL_SLOTS, type KronosComponents, type SlotName } from "./components.js"
import { SlotRegistry, type SlotFactory, type SlotMeta } from "./slot-registry.js"
import { buildResolved, type Resolved } from "./resolved.js"
import type { WarningChannel } from "./warnings.js"
import type { DecoratorEntry, DecoratorFactory, DecoratorHandle } from "./decorator.js"
import { applyDecorators } from "./decorator.js"
import { AppNotStartedError, UnknownDecoratorHandleError } from "./errors.js"
import type { LifecycleStage, LifecycleHook } from "./lifecycle.js"

/** Thrown when the App's mutating methods are called after .start() (D-50 footgun closure). */
export class AppAlreadyStartedError extends Error {
  constructor() {
    super("[kronos] App has already been started; configuration is immutable after start().")
    this.name = "AppAlreadyStartedError"
  }
}

/**
 * Extension shape in Phase 5 (D-50). Phase 6/7/9 will widen this; Phase 5 keeps it minimal.
 */
export type Extension = (app: App) => void | Promise<void>

/**
 * Plan 09-01 (D-88): per-state options accepted in App.states() tuple form.
 * Both fields optional — extensions or user code that wants snapshotting on a
 * particular state passes one or both, and the value flows through into
 * createEventSourcedRepository at start-time Step 5a.
 */
export interface StateOptions {
  readonly snapshotPolicy?: SnapshotPolicy
  readonly snapshotStore?: SnapshotStore
}

/**
 * Plan 09-01 (D-88): argument shape accepted by App.states() — either a bare
 * StateModule or a [module, options] tuple. Mixed lists are fine.
 */
// StateModule<any, any>: the Id parameter sits in a contravariant position
// (`create`/`criteria` accept it), so a concrete StateModule<{courseId:string},…>
// is NOT assignable to StateModule<unknown,unknown>. `any` accepts any module.
export type StatesArg =
  | StateModule<any, any>
  | readonly [StateModule<any, any>, StateOptions]

export interface KronosIdentity {
  /** Stable logical service/application name. Same across replicas. */
  readonly serviceName: string
  /** Unique physical runtime instance id. Different per process/pod. */
  readonly instanceId: string
}

export interface App {
  readonly identity: KronosIdentity
  states(...args: StatesArg[]): App
  commands(...handlers: CommandHandlerDefinition<any, any>[]): App
  queries(...handlers: QueryHandlerDefinition[]): App
  /**
   * Read accessor (Plan 09-01, D-103): when called with zero arguments,
   * returns the registered EventProcessorModule[] in registration order.
   * Returned array is a frozen view; mutations have no effect on app state.
   * Consumed by extensions (e.g. Axon Server) inside `onStart('connect', ...)`
   * to build a fan-out registration list before processors start.
   */
  processors(): readonly EventProcessorModule[]
  /** Writer overload (D-103): appends EventProcessorModule registrations. */
  processors(...modules: EventProcessorModule[]): App
  /** D-73: register an Extension (function) — runs during start() before slot resolution. */
  use(extension: Extension): App
  setDefault<K extends SlotName>(
    slot: K,
    factory: SlotFactory<K> | KronosComponents[K],
    meta?: SlotMeta,
  ): App
  set<K extends SlotName>(slot: K, factory: SlotFactory<K> | KronosComponents[K]): App
  forceSet<K extends SlotName>(slot: K, factory: SlotFactory<K> | KronosComponents[K]): App
  decorate<K extends SlotName>(slot: K, factory: DecoratorFactory<K>): DecoratorHandle<K>
  removeDecorator<K extends SlotName>(handle: DecoratorHandle<K>): App
  /**
   * Register a dispatch interceptor for the command bus. The interceptor runs as
   * part of the framework `intercepting` default decorator (Defaults.commandBus.intercepting).
   * If that default has been removed via `removeDecorator()`, registered interceptors
   * have no effect on dispatch.
   */
  commandDispatchInterceptor(fn: DispatchInterceptor<CommandMessage>): App
  /** Same as commandDispatchInterceptor, scoped to the query bus. */
  queryDispatchInterceptor(fn: DispatchInterceptor<QueryMessage>): App
  /** Same as commandDispatchInterceptor, scoped to the event bus. */
  eventDispatchInterceptor(fn: DispatchInterceptor<EventMessage>): App
  /**
   * Register a bus-agnostic handler interceptor. Wired into the framework `intercepting`
   * defaults for commandBus and queryBus only — eventBus has no handler-interceptor concept
   * in the existing intercepting wrapper.
   */
  handlerInterceptor(fn: HandlerInterceptor): App
  /**
   * Register one or more correlation data providers. The providers feed both
   * the command/query handler "extract" interceptor and the per-event seeding
   * in every event processor, and their output is applied to outgoing
   * commands/events (and appended events) via the correlation dispatch
   * interceptor + event appender. This is the single place correlation lineage
   * is configured — registering here makes an event handler's outgoing messages
   * inherit the triggering event's correlationId/causationId.
   *
   * Multiple calls accumulate. When none are registered, the framework defaults
   * to a single `messageOriginProvider()`. Returns App for chaining.
   */
  correlationDataProvider(...providers: CorrelationDataProvider[]): App
  /**
   * Plan 09-01 (D-86): accumulator for HandlerEnhancerDefinition. Mirrors the
   * Phase 6 dispatch-interceptor accumulator pattern. Multiple registrations
   * compose left-to-right via multiHandlerEnhancerDefinition (first registered
   * wraps outermost). Composed at start-time and threaded through:
   * - registerCommandHandlersNatively (Step 5c)
   * - registerQueryHandlersNatively (Step 5d, RESEARCH Open Question #4)
   * - createTrackingEventProcessor / createSubscribingEventProcessor (Step 5e)
   *
   * Returns App for chaining. Throws AppAlreadyStartedError after .start().
   */
  handlerEnhancer(def: HandlerEnhancerDefinition): App
  /**
   * Live CommandGateway. Throws AppNotStartedError if accessed before the
   * `register` lifecycle stage completes during `.start()`. Available inside
   * `onStart('warmup'|'register'|'processors'|'serve', fn)` hooks (after register)
   * and after `.start()` resolves. (Plan 08-01.)
   */
  readonly commandGateway: CommandGateway
  /**
   * Live QueryGateway. Throws AppNotStartedError if accessed before the
   * `register` lifecycle stage completes during `.start()`. Available inside
   * `onStart('warmup'|'register'|'processors'|'serve', fn)` hooks (after register)
   * and after `.start()` resolves. (Plan 08-01.)
   */
  readonly queryGateway: QueryGateway
  /**
   * Register a hook that runs at the given lifecycle stage during `.start()`.
   * Stages execute in forward order (connect → warmup → register → processors → serve).
   * Within a stage, hooks execute in registration order. Throws AppAlreadyStartedError
   * if called after `.start()`. (LIF-02, D-68, D-69, D-70)
   */
  onStart(stage: LifecycleStage, fn: LifecycleHook): App
  /**
   * Register a hook that runs at the given lifecycle stage during `.stop()`.
   * Stages execute in reverse order (serve → processors → register → warmup → connect).
   * Within a stage, hooks execute in registration order. Throws AppAlreadyStartedError
   * if called after `.start()`. (LIF-02, D-68, D-69, D-70)
   */
  onStop(stage: LifecycleStage, fn: LifecycleHook): App
  start(): Promise<RunningApp>
}

export interface RunningApp {
  readonly identity: KronosIdentity
  readonly commandGateway: CommandGateway
  readonly queryGateway: QueryGateway
  /**
   * The built event processors, keyed by name — the kronos analog of AF5's
   * `EventProcessingConfiguration.eventProcessors()`. This is the seam a host
   * or admin UI drives: enumerate processors, read `status()` (on tracking
   * processors), and `start()`/`stop()`/`resetTokens()`. The framework ships
   * no watchdog or auto-restart; operating the processors is the host's call.
   */
  eventProcessors(): ReadonlyMap<string, EventProcessor>
  stop(): Promise<void>
}

/** Internal accumulators populated by fluent methods; consumed by .start(). */
export interface AppState {
  readonly slotRegistry: SlotRegistry
  /** Plan 09-01 (D-88): replaces flat `states: StateModule[]` to carry per-state options. */
  readonly stateEntries: Array<{ module: StateModule; options: StateOptions }>
  readonly commandHandlers: CommandHandlerDefinition<any, any>[]
  readonly queryHandlers: QueryHandlerDefinition[]
  readonly processors: EventProcessorModule[]
  readonly extensions: Extension[]
  readonly warningChannel: WarningChannel
  readonly decoratorRegistrations: DecoratorEntry[]   // NEW: per-app registration order; pipeline = left-to-right
  readonly commandDispatchInterceptors: DispatchInterceptor<CommandMessage>[]
  readonly queryDispatchInterceptors: DispatchInterceptor<QueryMessage>[]
  readonly eventDispatchInterceptors: DispatchInterceptor<EventMessage>[]
  readonly handlerInterceptors: HandlerInterceptor[]
  /** Correlation data providers; defaults to [messageOriginProvider()] at start when empty. */
  readonly correlationDataProviders: CorrelationDataProvider[]
  /** Plan 09-01 (D-86): accumulator composed via multiHandlerEnhancerDefinition at start. */
  readonly handlerEnhancers: HandlerEnhancerDefinition[]
  readonly startHooks: Array<{ stage: LifecycleStage; fn: LifecycleHook }>
  readonly stopHooks:  Array<{ stage: LifecycleStage; fn: LifecycleHook }>
}

export interface AppImplOptions {
  warningChannel: WarningChannel
  /** Stable logical service/application name. Same across replicas. */
  serviceName?: string
  /** Unique physical runtime instance id. Different per process/pod. */
  instanceId?: string
  /** Per-stage timeout (ms) for native lifecycle execution (D-77). Default: 5000. */
  stageTimeoutMs?: number
}

export class AppImpl implements App {
  readonly _state: AppState
  private _started = false
  readonly identity: KronosIdentity
  private _commandGateway: CommandGateway | undefined = undefined
  private _queryGateway: QueryGateway | undefined = undefined
  /** D-77: per-stage timeout for native lifecycle execution. */
  private readonly _stageTimeoutMs: number

  /**
   * Live CommandGateway. Throws AppNotStartedError if accessed before the
   * `register` lifecycle stage completes during `.start()`. (Plan 08-01.)
   */
  get commandGateway(): CommandGateway {
    if (!this._commandGateway) throw new AppNotStartedError("commandGateway")
    return this._commandGateway
  }

  /**
   * Live QueryGateway. Throws AppNotStartedError if accessed before the
   * `register` lifecycle stage completes during `.start()`. (Plan 08-01.)
   */
  get queryGateway(): QueryGateway {
    if (!this._queryGateway) throw new AppNotStartedError("queryGateway")
    return this._queryGateway
  }

  constructor(options: AppImplOptions) {
    this._stageTimeoutMs = options.stageTimeoutMs ?? 5000
    this.identity = {
      serviceName: options.serviceName ?? "kronos-app",
      instanceId: options.instanceId ?? createDefaultInstanceId(),
    }
    this._state = {
      slotRegistry: new SlotRegistry(),
      stateEntries: [],
      commandHandlers: [],
      queryHandlers: [],
      processors: [],
      extensions: [],
      warningChannel: options.warningChannel,
      decoratorRegistrations: [],
      commandDispatchInterceptors: [],
      queryDispatchInterceptors: [],
      eventDispatchInterceptors: [],
      handlerInterceptors: [],
      correlationDataProviders: [],
      handlerEnhancers: [],
      startHooks: [],
      stopHooks: [],
    }
  }

  /** @internal — used by tests + by registerInMemoryDefaults indirectly through setDefault. */
  getRegistry(): SlotRegistry {
    return this._state.slotRegistry
  }

  /** @internal — used by Task 2's start() implementation. */
  isStarted(): boolean {
    return this._started
  }

  /** @internal — set just before .start() returns; Task 2 toggles this. */
  markStarted(): void {
    this._started = true
  }

  private guard(): void {
    if (this._started) throw new AppAlreadyStartedError()
  }

  states(...args: StatesArg[]): App {
    this.guard()
    for (const arg of args) {
      if (Array.isArray(arg)) {
        const [module, options] = arg as readonly [StateModule, StateOptions]
        this._state.stateEntries.push({ module, options })
      } else {
        this._state.stateEntries.push({ module: arg as StateModule, options: {} })
      }
    }
    return this
  }

  commands(...handlers: CommandHandlerDefinition<any, any>[]): App {
    this.guard()
    this._state.commandHandlers.push(...handlers)
    return this
  }

  queries(...handlers: QueryHandlerDefinition[]): App {
    this.guard()
    this._state.queryHandlers.push(...handlers)
    return this
  }

  // Plan 09-01 (D-103): dual-overload processors() — read accessor + writer.
  processors(): readonly EventProcessorModule[]
  processors(...modules: EventProcessorModule[]): App
  processors(
    ...modules: EventProcessorModule[]
  ): App | readonly EventProcessorModule[] {
    if (modules.length === 0) {
      // Frozen view so accidental .push() on the returned array doesn't smuggle
      // mutations into _state. The underlying array is still mutable for the
      // writer overload below.
      return Object.freeze([...this._state.processors]) as readonly EventProcessorModule[]
    }
    this.guard()
    this._state.processors.push(...modules)
    return this
  }


  use(extension: Extension): App {
    this.guard()
    this._state.extensions.push(extension)
    return this
  }

  setDefault<K extends SlotName>(
    slot: K,
    factory: SlotFactory<K> | KronosComponents[K],
    meta?: SlotMeta,
  ): App {
    this.guard()
    this._state.slotRegistry.setDefault(slot, factory, meta)
    return this
  }

  set<K extends SlotName>(slot: K, factory: SlotFactory<K> | KronosComponents[K]): App {
    this.guard()
    this._state.slotRegistry.set(slot, factory)
    return this
  }

  forceSet<K extends SlotName>(slot: K, factory: SlotFactory<K> | KronosComponents[K]): App {
    this.guard()
    this._state.slotRegistry.forceSet(slot, factory)
    return this
  }

  decorate<K extends SlotName>(
    slot: K,
    factory: DecoratorFactory<K>,
  ): DecoratorHandle<K> {
    this.guard()
    const handle: DecoratorHandle<K> = Object.freeze({
      __slot: slot,
      __id: Symbol(`user.${slot}`),
      __name: `user.${slot}.${this._state.decoratorRegistrations.length}`,
    }) as DecoratorHandle<K>
    this._state.decoratorRegistrations.push({
      handle: handle as DecoratorHandle<SlotName>,
      factory: factory as unknown as DecoratorFactory<SlotName>,
      frameworkDefault: false,
    })
    return handle
  }

  removeDecorator<K extends SlotName>(handle: DecoratorHandle<K>): App {
    this.guard()
    const idx = this._state.decoratorRegistrations.findIndex(
      (entry) => entry.handle.__id === handle.__id,
    )
    if (idx < 0) {
      throw new UnknownDecoratorHandleError(handle as DecoratorHandle<SlotName>)
    }
    this._state.decoratorRegistrations.splice(idx, 1)
    return this
  }

  commandDispatchInterceptor(fn: DispatchInterceptor<CommandMessage>): App {
    this.guard()
    this._state.commandDispatchInterceptors.push(fn)
    return this
  }

  queryDispatchInterceptor(fn: DispatchInterceptor<QueryMessage>): App {
    this.guard()
    this._state.queryDispatchInterceptors.push(fn)
    return this
  }

  eventDispatchInterceptor(fn: DispatchInterceptor<EventMessage>): App {
    this.guard()
    this._state.eventDispatchInterceptors.push(fn)
    return this
  }

  handlerInterceptor(fn: HandlerInterceptor): App {
    this.guard()
    this._state.handlerInterceptors.push(fn)
    return this
  }

  correlationDataProvider(...providers: CorrelationDataProvider[]): App {
    this.guard()
    this._state.correlationDataProviders.push(...providers)
    return this
  }

  handlerEnhancer(def: HandlerEnhancerDefinition): App {
    this.guard()
    this._state.handlerEnhancers.push(def)
    return this
  }

  onStart(stage: LifecycleStage, fn: LifecycleHook): App {
    this.guard()
    this._state.startHooks.push({ stage, fn })
    return this
  }

  onStop(stage: LifecycleStage, fn: LifecycleHook): App {
    this.guard()
    this._state.stopHooks.push({ stage, fn })
    return this
  }

  /**
   * @internal — used by `kronos()` bootstrap (Plan 02) to register framework-default
   * decorators with pre-allocated handle identities from `Defaults`. Distinguished
   * from user `.decorate()` registrations by `frameworkDefault: true` — at `.start()`,
   * framework defaults wrap innermost (handler-adjacent) and user decorators wrap
   * outside them (D-62, DESIGN.md §8 line 248).
   *
   * Called once per slot per kronos() invocation. Idempotent guard not added here
   * because kronos() bootstrap controls call sites.
   */
  _registerFrameworkDefaultDecorator<K extends SlotName>(
    handle: DecoratorHandle<K>,
    factory: DecoratorFactory<K>,
  ): void {
    this._state.decoratorRegistrations.push({
      handle: handle as DecoratorHandle<SlotName>,
      factory: factory as unknown as DecoratorFactory<SlotName>,
      frameworkDefault: true,
    })
  }

  async start(): Promise<RunningApp> {
    if (this._started) throw new AppAlreadyStartedError()

    // 1. Run extensions FIRST so they can mutate the slot registry / accumulators (D-50).
    for (const ext of this._state.extensions) {
      const result = ext(this)
      if (result instanceof Promise) await result
    }

    // 2. Mark started AFTER extensions ran (so extensions can mutate) but BEFORE slot resolution
    //    (so resolved factories can't trigger fluent calls — defensive).
    this._started = true

    // 2b. Resolve correlation lineage from one source. Providers default to a
    //     single messageOriginProvider() when the app registered none. Wire the
    //     "extract" handler interceptor (command/query) and the "apply" dispatch
    //     interceptors (command/query/event) into the accumulators BEFORE
    //     decorators are applied below, so the intercepting wrappers pick them
    //     up. Event processors receive the same providers in step 5e and seed
    //     per-event; the event appender applies the resulting data to appended
    //     events. The interceptors no-op outside a UnitOfWork, so primary
    //     gateway dispatches are unaffected — only handler-to-handler flows
    //     carry lineage.
    const correlationProviders =
      this._state.correlationDataProviders.length > 0
        ? this._state.correlationDataProviders
        : [messageOriginProvider()]
    this._state.handlerInterceptors.push(correlationDataHandlerInterceptor(correlationProviders))
    this._state.commandDispatchInterceptors.push(correlationDataDispatchInterceptor())
    this._state.queryDispatchInterceptors.push(correlationDataDispatchInterceptor())
    this._state.eventDispatchInterceptors.push(correlationDataDispatchInterceptor())

    // 3. Build the lazy Resolved proxy and EAGERLY resolve all 8 slots up-front
    //    (Pitfall 1 — interleaving slot resolution with configurer registration creates stale-cache hazards).
    const resolved = buildResolved(this._state.slotRegistry)
    const built: { -readonly [K in SlotName]: KronosComponents[K] } = {
      eventStore: resolved.eventStore,
      snapshotStore: resolved.snapshotStore,
      commandBus: resolved.commandBus,
      queryBus: resolved.queryBus,
      eventBus: resolved.eventBus,
      serializer: resolved.serializer,
      unitOfWorkFactory: resolved.unitOfWorkFactory,
      tagResolver: resolved.tagResolver,
      // Plan 09-01 (D-84): two new typed slots — eagerly resolved so decorator
      // application + processor wiring see the same instance.
      tokenStore: resolved.tokenStore,
      transactionManager: resolved.transactionManager,
      eventScheduler: resolved.eventScheduler,
    }

    // 3b. Apply decorators in two passes per slot (D-62, D-64, DESIGN.md §8):
    //     - framework defaults first (innermost, handler-adjacent)
    //     - user decorators after (outer; last .decorate() = outermost wrap)
    //     Pipeline visualization for slot K (outer → inner):
    //       [user decorators in registration order, last=outermost]
    //         [framework defaults in registration order, last=outermost]
    //           [base = resolved[K]]
    const writableBuilt = built as Record<SlotName, unknown>
    for (const slot of ALL_SLOTS) {
      writableBuilt[slot] = applyDecorators(slot, built[slot], this._state.decoratorRegistrations, resolved)
    }

    // 4. Emit startup warnings for any slot still using a flagged in-memory default (SLT-04).
    //    Iterate ALL_SLOTS for deterministic order. Warning emission goes through the channel
    //    so quiet:true / logger options route correctly (D-51).
    for (const slot of ALL_SLOTS) {
      const entry = this._state.slotRegistry.getEntry(slot)
      if (entry?.meta?.inMemory && entry.meta.warning) {
        this._state.warningChannel.emit(entry.meta.warning)
      }
    }

    // ----------------------------------------------------------------------
    // 5. Native wiring (Plan 08-03a — Configurer chain deleted).
    //    Build StateManager from registered entities + resolved eventStore, then
    //    subscribe command/query handlers and event-handler subscribing processors
    //    directly off the resolved buses. No legacy configurer chain, no Module
    //    initialize() shells, no LifecycleRegistry numeric-phase bridge.
    // ----------------------------------------------------------------------

    // 5a. Construct StateManager from registered entities (mirrors the configurer's
    //     registerDefaults() pattern at eventsourcing-configurer.ts ~line 755).
    //     StateModule has no .initialize() — state modules are wired purely via
    //     repository registration on the StateManager.
    //     Plan 09-01 (D-88): per-state tuple options (snapshotPolicy, snapshotStore)
    //     thread through into createEventSourcedRepository.
    const stateManager: StateManager = createStateManager()
    for (const { module, options } of this._state.stateEntries) {
      stateManager.register(
        module,
        createEventSourcedRepository(
          module,
          built.eventStore,
          options.snapshotStore ?? built.snapshotStore,
          options.snapshotPolicy,
        ),
      )
    }

    // 5b. Build the minimal Configuration shim that createCommandInvocation reads
    //     during dispatch (D-82). Surface kept narrow on purpose — anything outside
    //     the documented set throws loudly so misuse is obvious.
    const configShim = createConfigShim(built, stateManager)

    // Plan 09-01 (D-86, RESEARCH Open Question #4): compose the accumulated
    // handlerEnhancers ONCE and thread the composed definition into command,
    // query, tracking, and subscribing handler registration so cross-cutting
    // (tracing, timing, security) wraps every handler kind uniformly.
    const composedHandlerEnhancer: HandlerEnhancerDefinition | undefined =
      this._state.handlerEnhancers.length > 0
        ? multiHandlerEnhancerDefinition(this._state.handlerEnhancers)
        : undefined

    // 5c. Subscribe command handlers natively. createCommandInvocation seeds the
    //     ALS three-key set (STATE_MANAGER + COMMAND_BUS + QUERY_BUS) and registers
    //     the onPrepareCommit event-flush — verbatim from D-82.
    registerCommandHandlersNatively(this._state.commandHandlers, {
      commandBus: built.commandBus,
      config: configShim,
      moduleName: "commands",
      handlerEnhancer: composedHandlerEnhancer,
    })

    // 5d. Subscribe query handlers directly onto the queryBus.
    //     Plan 09-01: query handlers receive the same enhancer treatment as
    //     commands (closes RESEARCH Open Question #4).
    registerQueryHandlersNatively(this._state.queryHandlers, {
      queryBus: built.queryBus,
      moduleName: "queries",
      handlerEnhancer: composedHandlerEnhancer,
    })

    // 5e. Build event processors from explicit `.processors(...)` modules.
    //     Users wanting a subscribing processor write
    //     `subscribingProcessor(name).eventHandlers(...).build()` and pass it
    //     to `app.processors(...)` — there is no implicit shortcut.
    // Processor construction, parameterized over the components + state manager
    // it should bind to. The root app calls it with the resolved root set; each
    // module scope calls it with its own (see 5e-bis), which is what lets a
    // module's processors read from that module's event store.
    const buildProcessorsFor = (
      procs: readonly EventProcessorModule[],
      ctxBuilt: { -readonly [K in SlotName]: KronosComponents[K] },
      ctxStateManager: StateManager,
    ): Array<TrackingEventProcessor | SubscribingEventProcessor> => {
      const out: Array<TrackingEventProcessor | SubscribingEventProcessor> = []
      for (const proc of procs) {
      if (proc.kind === "subscribing") {
        const subscribable = ctxBuilt.eventStore as unknown as SubscribableEventSource
        if (!subscribable.subscribe) {
          throw new Error(
            `Event source does not support subscription. ` +
              `Cannot create subscribing processor "${proc.name}".`,
          )
        }
        out.push(
          createSubscribingEventProcessor({
            name: proc.name,
            eventSource: subscribable,
            eventHandlers: proc.eventHandlers,
            stateManager: ctxStateManager,
            commandBus: ctxBuilt.commandBus,
            queryBus: ctxBuilt.queryBus,
            eventScheduler: ctxBuilt.eventScheduler,
            correlationDataProviders: correlationProviders,
            unitOfWorkRunner: proc.unitOfWorkRunner ?? ctxBuilt.unitOfWorkFactory,
            errorHandler: proc.errorHandler,
            handlerEnhancer: composedHandlerEnhancer,
          }),
        )
      } else {
        out.push(
          createTrackingEventProcessor({
            name: proc.name,
            eventSource: ctxBuilt.eventStore as unknown as StreamableEventSource,
            eventHandlers: proc.eventHandlers,
            stateManager: ctxStateManager,
            commandBus: ctxBuilt.commandBus,
            queryBus: ctxBuilt.queryBus,
            eventScheduler: ctxBuilt.eventScheduler,
            correlationDataProviders: correlationProviders,
            unitOfWorkRunner: proc.unitOfWorkRunner ?? ctxBuilt.unitOfWorkFactory,
            // Plan 09-01 (D-84): per-processor override wins, otherwise fall
            // back to the resolved tokenStore slot so the default in-memory
            // store (or any extension-supplied replacement) drives position
            // persistence — the slot is the single source of truth.
            tokenStore: proc.tokenStore ?? ctxBuilt.tokenStore,
            deadLetterQueue: proc.deadLetterQueue,
            enqueuePolicy: proc.enqueuePolicy,
            sequencingPolicy: proc.sequencingPolicy,
            deadLetterListener: proc.deadLetterListener,
            resetClearsDeadLetters: proc.resetClearsDeadLetters,
            dlqRetryIntervalMs: proc.dlqRetryIntervalMs,
            batchSize: proc.batchSize,
            pollingIntervalMs: proc.pollingIntervalMs,
            errorHandler: proc.errorHandler,
            handlerEnhancer: composedHandlerEnhancer,
            // Plan 11-02: onReset lives on the tracking processor module.
            // Tracking processors support reset; subscribing processors don't.
            onReset: proc.onReset,
          }),
        )
      }
      }
      return out
    }

    const builtProcessors = buildProcessorsFor(this._state.processors, built, stateManager)

    // 5e-bis. Module scopes (encapsulation). Each scope inherits the root's
    //   resolved components BY IDENTITY — the same commandBus/queryBus/eventBus
    //   instances — and re-resolves only the slots it overrode via `m.set(...)`.
    //   So a module can own its event store while sharing the messaging fabric.
    //   Its states get a state manager over ITS store, and its command/query
    //   handlers dispatch through a config shim bound to that pair, which is
    //   what makes `ctx.load` / the PREPARE_COMMIT event flush hit the scoped
    //   store instead of the root one.
    for (const scope of moduleScopesOf(this)) {
      const scopedBuilt = { ...built } as { -readonly [K in SlotName]: KronosComponents[K] }
      if (scope.slotOverrides.length > 0) {
        // Overrides observe the scope as it resolves, so they can build on the
        // root's components (or on an earlier override in the same module).
        const scopedView = new Proxy(
          {},
          { get: (_t, prop) => scopedBuilt[prop as SlotName] },
        ) as Resolved
        for (const { slot, factory } of scope.slotOverrides) {
          const base = factory(scopedView) as KronosComponents[SlotName]
          scopedBuilt[slot] = applyDecorators(
            slot,
            base,
            this._state.decoratorRegistrations,
            scopedView,
          ) as never
        }
      }

      const scopedStateManager: StateManager = createStateManager()
      for (const { module, options } of scope.stateEntries) {
        scopedStateManager.register(
          module,
          createEventSourcedRepository(
            module,
            scopedBuilt.eventStore,
            options.snapshotStore ?? scopedBuilt.snapshotStore,
            options.snapshotPolicy,
          ),
        )
      }

      const scopedShim = createConfigShim(scopedBuilt, scopedStateManager)
      const scopeLabel = `module:${scope.name}`
      registerCommandHandlersNatively(scope.commandHandlers, {
        commandBus: scopedBuilt.commandBus,
        config: scopedShim,
        moduleName: scopeLabel,
        handlerEnhancer: composedHandlerEnhancer,
      })
      registerQueryHandlersNatively(scope.queryHandlers, {
        queryBus: scopedBuilt.queryBus,
        moduleName: scopeLabel,
        handlerEnhancer: composedHandlerEnhancer,
      })
      builtProcessors.push(...buildProcessorsFor(scope.processors, scopedBuilt, scopedStateManager))
    }

    // 5f. Build CommandGateway / QueryGateway from resolved buses, threading the
    //     configured UoW runner through so transactional wrappers span the dispatch
    //     boundary (CTX-04 / D-34). Gateways are constructed eagerly but the
    //     `app.commandGateway` / `app.queryGateway` accessors are only populated at
    //     the `register` stage — preserving the AppNotStartedError contract for
    //     pre-register hooks (Plan 08-01).
    const commandGateway = createCommandGateway(built.commandBus)
    const queryGateway = createQueryGateway(built.queryBus, built.unitOfWorkFactory)

    // 5g. Run typed-stage start hooks in forward order with D-77 warn-then-continue
    //     per-stage timeout. Hooks within a stage run concurrently via Promise.all.
    //     At the `register` stage, populate the live-gateway accessors so any
    //     register/processors/serve-stage hooks (and downstream handlers) see them.
    for (const stage of FORWARD_STAGES) {
      if (stage === "register") {
        this._commandGateway = commandGateway
        this._queryGateway = queryGateway
      }
      const hooks = this._state.startHooks.filter((h) => h.stage === stage)
      if (hooks.length === 0) continue
      await this._runStageWithTimeout(
        stage,
        hooks.map((h) => h.fn),
        this._stageTimeoutMs,
      )
    }

    // 5h. Start event processors AFTER processors-stage hooks have run — mirrors
    //     the configurer's old sequencing (eventsourcing-configurer.ts lines 670+).
    for (const proc of builtProcessors) {
      await proc.start()
    }

    // 5i. Build the RunningApp. stop() reverses: processors first, then user stop
    //     hooks in reverse stage order, again with the warn-then-continue timeout.
    const runStageWithTimeout = this._runStageWithTimeout.bind(this)
    const stageTimeoutMs = this._stageTimeoutMs
    const stopHooks = this._state.stopHooks
    const identity = this.identity
    // Name-keyed registry of built processors — the EventProcessingConfiguration
    // .eventProcessors() analog a host/admin UI enumerates and drives. Frozen so
    // callers can't mutate the framework's processor set.
    const processorRegistry: ReadonlyMap<string, EventProcessor> = new Map(
      builtProcessors.map((proc) => [proc.name, proc as EventProcessor]),
    )
    return {
      get identity(): KronosIdentity {
        return identity
      },
      get commandGateway(): CommandGateway {
        return commandGateway
      },
      get queryGateway(): QueryGateway {
        return queryGateway
      },
      eventProcessors(): ReadonlyMap<string, EventProcessor> {
        return processorRegistry
      },
      async stop() {
        // Stop processors first (mirrors legacy shutdown order).
        for (const proc of builtProcessors) {
          proc.stop()
        }
        // Reverse stage order for stop hooks.
        for (const stage of REVERSE_STAGES) {
          const hooks = stopHooks.filter((h) => h.stage === stage)
          if (hooks.length === 0) continue
          await runStageWithTimeout(
            stage,
            hooks.map((h) => h.fn),
            stageTimeoutMs,
          )
        }
      },
    }
  }

  /**
   * D-77 native lifecycle execution: per-stage Promise.all + Promise.race with
   * warn-then-continue. If the stage exceeds `timeoutMs`, log a warning and
   * STOP WAITING — the slow hooks continue to pend in the background; they are
   * NOT cancelled. Reproduces createLifecycleRegistry's per-phase semantics
   * verbatim, but over typed stages instead of numeric phases.
   */
  private async _runStageWithTimeout(
    stage: LifecycleStage,
    fns: LifecycleHook[],
    timeoutMs: number,
  ): Promise<void> {
    const stageWork = Promise.all(fns.map((fn) => Promise.resolve(fn())))
    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => resolve("timeout"), timeoutMs)
    })
    // Swallow background rejections from the slow hooks if the stage has already
    // returned via the timeout branch — without this, an unhandled rejection
    // could surface long after start() resolved (the hook is intentionally not
    // cancelled per D-77, but its eventual rejection is no longer observable).
    stageWork.catch(() => {
      /* warn-then-continue: failures after the timeout are intentionally dropped */
    })
    const result = await Promise.race([
      stageWork.then(() => "done" as const),
      timeout,
    ])
    if (timer) clearTimeout(timer)
    if (result === "timeout") {
      this._state.warningChannel.emit(
        `[kronos] Lifecycle stage '${stage}' exceeded ${timeoutMs}ms timeout — continuing without waiting for completion (warn-then-continue per D-77).`,
      )
    }
  }
}

// ---------------------------------------------------------------------------
// Module-private helpers (Plan 08-03a native execution)
// ---------------------------------------------------------------------------

function createDefaultInstanceId(): string {
  const randomUUID = globalThis.crypto?.randomUUID?.bind(globalThis.crypto)
  if (randomUUID) return randomUUID()
  return `instance-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
}

/** Forward typed-stage order for `.start()` execution (D-77, LIF-01). */
const FORWARD_STAGES: ReadonlyArray<LifecycleStage> = [
  "connect",
  "warmup",
  "register",
  "processors",
  "serve",
] as const

/** Reverse typed-stage order for `.stop()` execution (D-77, LIF-01). */
const REVERSE_STAGES: ReadonlyArray<LifecycleStage> = [
  "serve",
  "processors",
  "register",
  "warmup",
  "connect",
] as const

/**
 * Construct a minimal Configuration shim for createCommandInvocation (D-82).
 *
 * The shim implements ONLY the methods createCommandInvocation invokes:
 * - hasComponent / getComponent for STATE_MANAGER, COMMAND_BUS, QUERY_BUS
 *   (the three keys seeded into ALS at command-invocation entry per D-82)
 * - hasComponent / getComponent for EVENT_STORE (read inside the
 *   onPrepareCommit closure when flushing buffered events)
 * - getOptionalComponent for TAG_RESOLVER (read inside onPrepareCommit when
 *   enriching events with tags)
 *
 * Everything else (decorators, modules, factories, getComponents, getParent)
 * throws or returns empty — the configurer-era surface is gone. Plan 04 will
 * delete the Configuration interface entirely; this shim is the bridge.
 */
function createConfigShim(
  built: { -readonly [K in SlotName]: KronosComponents[K] },
  stateManager: StateManager,
): MinimalConfiguration {
  // Inline string-literal keys mirror the kronos() framework defaults that
  // createCommandInvocation reads (STATE_MANAGER, COMMAND_BUS, QUERY_BUS,
  // EVENT_STORE, TAG_RESOLVER) plus the additional slot mirrors carried
  // forward for parity with the previous Configuration shim.
  const components: Record<string, unknown> = {
    stateManager,
    commandBus: built.commandBus,
    queryBus: built.queryBus,
    eventScheduler: built.eventScheduler,
    eventStore: built.eventStore,
    eventBus: built.eventBus,
    snapshotStore: built.snapshotStore,
    serializer: built.serializer,
    unitOfWorkFactory: built.unitOfWorkFactory,
    tagResolver: built.tagResolver,
    // Plan 09-01: shim mirrors of the two new typed slots so legacy
    // enhancers / probes that look them up via the Configuration shape
    // see the resolved instance.
    tokenStore: built.tokenStore,
    transactionManager: built.transactionManager,
  }
  const config: MinimalConfiguration = {
    hasComponent(type: string, _name?: string): boolean {
      return type in components
    },
    getComponent<T>(type: string, _name?: string): T {
      if (!(type in components)) {
        throw new Error(`[kronos] Configuration shim does not provide "${type}"`)
      }
      return components[type] as T
    },
    getOptionalComponent<T>(type: string, _name?: string): T | undefined {
      return components[type] as T | undefined
    },
  }
  return config
}
