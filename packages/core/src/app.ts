import type { EntityModule } from "@kronos-ts/modelling"
import type {
  CommandHandlerDefinition,
  QueryHandlersDefinition,
  EventHandlersDefinition,
  EventProcessorModule,
  CommandGateway,
  QueryGateway,
} from "@kronos-ts/messaging"
import type { KronosComponents, SlotName } from "./components.js"
import { SlotRegistry, type SlotFactory, type SlotMeta } from "./slot-registry.js"
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

  /** Stub — Task 2 implements this. Throws by default so tests catch missing wiring. */
  async start(): Promise<RunningApp> {
    throw new Error("AppImpl.start() not yet implemented — Task 2 wires the configurer bridge")
  }
}
