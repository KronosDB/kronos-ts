import type { EntityModule } from "@kronos-ts/modelling"
import type {
  CommandHandlerDefinition,
  QueryHandlersDefinition,
  EventHandlersDefinition,
  EventProcessorModule,
  CommandGateway,
  QueryGateway,
} from "@kronos-ts/messaging"
import { subscribingProcessor } from "@kronos-ts/messaging"
import { EventSourcingConfigurer } from "@kronos-ts/eventsourcing"
import { ComponentKeys } from "@kronos-ts/common"
import { ALL_SLOTS, type KronosComponents, type SlotName } from "./components.js"
import { SlotRegistry, type SlotFactory, type SlotMeta } from "./slot-registry.js"
import { buildResolved } from "./resolved.js"
import type { WarningChannel } from "./warnings.js"

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

export interface App {
  entities(...modules: EntityModule[]): App
  commands(...handlers: CommandHandlerDefinition<any, any>[]): App
  queries(...handlers: QueryHandlersDefinition[]): App
  events(...handlers: EventHandlersDefinition[]): App
  processors(...modules: EventProcessorModule[]): App
  use(extension: Extension): App
  setDefault<K extends SlotName>(
    slot: K,
    factory: SlotFactory<K> | KronosComponents[K],
    meta?: SlotMeta,
  ): App
  set<K extends SlotName>(slot: K, factory: SlotFactory<K> | KronosComponents[K]): App
  forceSet<K extends SlotName>(slot: K, factory: SlotFactory<K> | KronosComponents[K]): App
  start(): Promise<RunningApp>
}

export interface RunningApp {
  readonly commandGateway: CommandGateway
  readonly queryGateway: QueryGateway
  stop(): Promise<void>
}

/** Internal accumulators populated by fluent methods; consumed by .start(). */
export interface AppState {
  readonly slotRegistry: SlotRegistry
  readonly entities: EntityModule[]
  readonly commandHandlers: CommandHandlerDefinition<any, any>[]
  readonly queryHandlerGroups: QueryHandlersDefinition[]
  readonly eventHandlerGroups: EventHandlersDefinition[]
  readonly processors: EventProcessorModule[]
  readonly extensions: Extension[]
  readonly warningChannel: WarningChannel
}

export interface AppImplOptions {
  warningChannel: WarningChannel
}

export class AppImpl implements App {
  readonly _state: AppState
  private _started = false

  constructor(options: AppImplOptions) {
    this._state = {
      slotRegistry: new SlotRegistry(),
      entities: [],
      commandHandlers: [],
      queryHandlerGroups: [],
      eventHandlerGroups: [],
      processors: [],
      extensions: [],
      warningChannel: options.warningChannel,
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

  entities(...modules: EntityModule[]): App {
    this.guard()
    this._state.entities.push(...modules)
    return this
  }

  commands(...handlers: CommandHandlerDefinition<any, any>[]): App {
    this.guard()
    this._state.commandHandlers.push(...handlers)
    return this
  }

  queries(...handlers: QueryHandlersDefinition[]): App {
    this.guard()
    this._state.queryHandlerGroups.push(...handlers)
    return this
  }

  events(...handlers: EventHandlersDefinition[]): App {
    this.guard()
    this._state.eventHandlerGroups.push(...handlers)
    return this
  }

  processors(...modules: EventProcessorModule[]): App {
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

    // 3. Build the lazy Resolved proxy and EAGERLY resolve all 8 slots up-front
    //    (Pitfall 1 — interleaving slot resolution with configurer registration creates stale-cache hazards).
    const resolved = buildResolved(this._state.slotRegistry)
    const built: KronosComponents = {
      eventStore: resolved.eventStore,
      snapshotStore: resolved.snapshotStore,
      commandBus: resolved.commandBus,
      queryBus: resolved.queryBus,
      eventBus: resolved.eventBus,
      serializer: resolved.serializer,
      unitOfWorkFactory: resolved.unitOfWorkFactory,
      tagResolver: resolved.tagResolver,
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

    // 5. Build the EventSourcingConfigurer chain (D-49). Order matters: register slot
    //    overrides BEFORE configurer.start() runs build() internally (Pitfall 1).
    const configurer = EventSourcingConfigurer.create()

    // Slot → configurer registration mapping (uses public methods that call reg.register, NOT registerIfAbsent):
    configurer.registerEventStore(() => built.eventStore)
    configurer.registerTagResolver(() => built.tagResolver)
    configurer.componentRegistry((reg) => {
      reg.register(ComponentKeys.SNAPSHOT_STORE, () => built.snapshotStore)
      reg.register(ComponentKeys.SERIALIZER, () => built.serializer)
      reg.register(ComponentKeys.EVENT_BUS, () => built.eventBus)
    })
    configurer.messaging((m) => {
      m.registerCommandBus(() => built.commandBus)
      m.registerQueryBus(() => built.queryBus)
      m.registerUnitOfWorkFactory(() => built.unitOfWorkFactory)
    })

    // 6. Translate domain registrations to configurer calls.
    for (const entity of this._state.entities) {
      configurer.registerEntity(entity)
    }
    for (const handler of this._state.commandHandlers) {
      configurer.registerCommandHandler(() => handler)
    }
    for (const queryGroup of this._state.queryHandlerGroups) {
      configurer.registerQueryHandlers(() => queryGroup)
    }
    for (const eventGroup of this._state.eventHandlerGroups) {
      // Translate .events() registrations to subscribing processors (mirrors legacy pattern)
      const builder = subscribingProcessor(eventGroup.name).registerEventHandler(eventGroup)
      configurer.registerEventProcessor(() => builder.build())
    }
    for (const processor of this._state.processors) {
      configurer.registerEventProcessor(() => processor)
    }

    // 7. Build & start.
    const kronosApp = await configurer.start()

    return {
      get commandGateway(): CommandGateway {
        return kronosApp.commandGateway
      },
      get queryGateway(): QueryGateway {
        return kronosApp.queryGateway
      },
      stop: () => kronosApp.stop(),
    }
  }
}
