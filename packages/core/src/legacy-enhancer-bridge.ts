// transitional: Phase 9 deletes
//
// Bridge adapter: routes legacy ConfigurationEnhancer instances passed to App.use()
// through the typed App API. The ONLY surviving consumer of the legacy
// ConfigurationEnhancer / ComponentRegistry / ComponentKeys surface after Phase 8.
//
// D-73, D-74, D-81. Production extensions (KronosDB, Axon Server, OpenTelemetry) keep
// loading via this bridge until Phase 9 migrates them to (app: App) => void.
//
// Plan 08-04 relocation: the legacy ConfigurationEnhancer / ComponentRegistry /
// Configuration / Module / ComponentBuilder / ComponentDecorator / ComponentFactory /
// ComponentId / OverridePolicy / SearchScope / ComponentOverrideError type declarations
// AND the ComponentKeys constant are now owned by THIS file (formerly in
// @kronos-ts/common's configuration.ts + component-keys.ts, both deleted in Plan 04).
// Production extensions import these via `@kronos-ts/core/legacy-enhancer-bridge`.
//
// Exported as a SUBMODULE from packages/core via the package.json exports map
// `./legacy-enhancer-bridge` entry. NOT re-exported from packages/core/src/index.ts.

import type { App } from "./app.js"
import type { LifecycleStage } from "./lifecycle.js"
import type { KronosComponents, SlotName } from "./components.js"

// ─────────────────────────────────────────────────────────────────
// transitional: Phase 9 deletes
// The legacy ConfigurationEnhancer / ComponentRegistry / ComponentKeys
// surface is preserved here ONLY for the three production extensions
// (KronosDB, Axon Server, OpenTelemetry). Phase 9 migrates them to
// (app: App) => void and deletes this entire block.
// ─────────────────────────────────────────────────────────────────

/**
 * A component identifier — a type key with an optional name for disambiguation.
 * Allows multiple components of the same type (e.g., different TokenStores per processor).
 */
export type ComponentId = {
  readonly type: string
  readonly name?: string
}

/**
 * Controls what happens when a component is registered with a key
 * that already has a builder.
 *
 * CFG-03: deleted from public surface — bridge keeps the type alive
 * for ConfigurationEnhancer signature compatibility only.
 */
export type OverridePolicy = "allow" | "warn" | "reject"

/**
 * Controls where to search when resolving components in hierarchical configurations.
 *
 * CFG-03: deleted from public surface — bridge keeps the type alive
 * for ConfigurationEnhancer signature compatibility only.
 */
export type SearchScope = "all" | "current" | "ancestors"

/**
 * A factory that can create components on demand.
 * Only consulted when a component is NOT already registered.
 */
export interface ComponentFactory<T = unknown> {
  /** Whether this factory can create a component for the given key. */
  canCreate(type: string, name?: string): boolean
  /** Create the component. */
  create(config: Configuration, type: string, name?: string): T
}

/**
 * A type-keyed component registry for wiring framework infrastructure.
 *
 * Components are registered by type (and optional name) and built lazily on first access.
 * Decorators wrap components after they're built, applied in order (lower = first).
 */
export interface ComponentRegistry {
  /**
   * Register a component builder. Behavior on duplicate keys depends on
   * the override policy (default: "warn").
   */
  register<T>(type: string, builder: ComponentBuilder<T>): ComponentRegistry
  register<T>(type: string, name: string, builder: ComponentBuilder<T>): ComponentRegistry

  /**
   * Register a component builder only if no component exists for this key.
   */
  registerIfAbsent<T>(type: string, builder: ComponentBuilder<T>): ComponentRegistry
  registerIfAbsent<T>(type: string, name: string, builder: ComponentBuilder<T>): ComponentRegistry

  /**
   * Register a decorator that wraps a component after it's built.
   * Lower order values execute first (innermost decorator has highest order).
   */
  registerDecorator<T>(type: string, order: number, decorator: ComponentDecorator<T>): ComponentRegistry
  registerDecorator<T>(type: string, name: string, order: number, decorator: ComponentDecorator<T>): ComponentRegistry

  /**
   * Register a module — a self-contained group of components that registers
   * itself with the appropriate buses/registries during initialization.
   */
  registerModule(module: Module): ComponentRegistry

  /**
   * Register a component factory for on-demand component creation.
   */
  registerFactory(factory: ComponentFactory): ComponentRegistry

  /**
   * Set the override policy for this registry.
   */
  setOverridePolicy(policy: OverridePolicy): ComponentRegistry
}

/**
 * Resolved configuration — provides access to built components.
 */
export interface Configuration {
  /** Get a component by type (and optional name). */
  getComponent<T>(type: string): T
  getComponent<T>(type: string, name: string): T

  /** Get a component, returning undefined if not registered (instead of throwing). */
  getOptionalComponent<T>(type: string, name?: string): T | undefined

  /** Get all components of a given type as a map of name → component. */
  getComponents<T>(type: string): Map<string, T>

  /** Check if a component is registered. */
  hasComponent(type: string, name?: string): boolean

  /** Get all registered modules. */
  getModules(): ReadonlyArray<Module>

  /** Get the parent configuration (if this is a nested/child config). */
  getParent(): Configuration | undefined
}

/**
 * Builds a component, given access to the full configuration for resolving dependencies.
 */
export type ComponentBuilder<T> = (config: Configuration) => T

/**
 * Decorates/wraps a component. Receives the configuration, the component's
 * registered name (or the type key if unnamed), and the built delegate.
 */
export type ComponentDecorator<T> = (config: Configuration, name: string, delegate: T) => T

/**
 * A self-contained group of related handlers/components that can register
 * itself with the messaging infrastructure.
 */
export interface Module {
  readonly name: string
  /** Called after all components are built. */
  initialize(config: Configuration): void
}

/**
 * Enhances a component registry with additional components, decorators, etc.
 */
export interface ConfigurationEnhancer {
  enhance(registry: ComponentRegistry): void
  /** Lower values execute first. Defaults to 0. CFG-03: bridge IGNORES this field. */
  order?: number
  /** Called when the application starts. Receives the built configuration. */
  onStart?: (config: Configuration) => void | Promise<void>
  /** Called when the application stops. */
  onStop?: () => void | Promise<void>
}

/**
 * Thrown when a component registration is rejected by the override policy.
 */
export class ComponentOverrideError extends Error {
  constructor(key: string) {
    super(`Component override rejected for "${key}". Override policy is set to "reject".`)
    this.name = "ComponentOverrideError"
  }
}

/**
 * Well-known component keys used by the framework.
 * These are the string keys for looking up components in the legacy registry.
 *
 * Plan 08-04 relocation: formerly in @kronos-ts/common's component-keys.ts.
 * Phase 9 deletes this constant alongside the bridge.
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

// ─────────────────────────────────────────────────────────────────
// End relocated legacy surface
// ─────────────────────────────────────────────────────────────────

/**
 * Translation table from ComponentKeys string tokens to the typed
 * KronosComponents slot names. Tokens absent from this table fall back
 * to the bridge's per-app token store (still observable via Configuration
 * shim getComponent), but typed-slot resolution will not see them.
 */
const TOKEN_TO_SLOT: Record<string, SlotName> = {
  [ComponentKeys.COMMAND_BUS]: "commandBus",
  [ComponentKeys.QUERY_BUS]: "queryBus",
  [ComponentKeys.EVENT_STORE]: "eventStore",
  [ComponentKeys.SNAPSHOT_STORE]: "snapshotStore",
  [ComponentKeys.EVENT_BUS]: "eventBus",
  [ComponentKeys.SERIALIZER]: "serializer",
  [ComponentKeys.UNIT_OF_WORK_FACTORY]: "unitOfWorkFactory",
  [ComponentKeys.TAG_RESOLVER]: "tagResolver",
}

/**
 * D-81 fallback: numeric LifecyclePhase → typed LifecycleStage.
 *
 * No current production caller — KronosDB / Axon Server / OpenTelemetry
 * enhancers all use the standalone `enhancer.onStart(config)` callback
 * rather than `lifecycleRegistry.onStart(phase, fn)` from inside enhance()
 * (verified via RESEARCH §Production Enhancer Audit). The inverter exists
 * for completeness of D-81 in case a future enhancer reaches for the numeric
 * scale before Phase 9 deletes the configurer entirely.
 */
export function phaseToStage(phase: number): LifecycleStage {
  if (phase <= -10) return "connect"
  if (phase <= 999) return "register"
  if (phase <= 1500) return "processors"
  return "serve"
}

/**
 * Loud failure for any Configuration method the bridge shim does not implement.
 * The bridge surface is intentionally narrow — anything outside the audited
 * KronosDB / Axon Server / OpenTelemetry usage throws here rather than returning
 * undefined silently.
 */
export class UnsupportedConfigurationMethodError extends Error {
  constructor(method: string) {
    super(
      `[kronos] Bridge Configuration shim does not implement '${method}'. ` +
        `Enhancer is using a feature outside the minimal viable subset audited ` +
        `from KronosDB / Axon Server / OpenTelemetry. File a bug or migrate ` +
        `the enhancer to the (app: App) => void shape.`,
    )
    this.name = "UnsupportedConfigurationMethodError"
  }
}

/**
 * Apply a legacy ConfigurationEnhancer to an unstarted App. Called from
 * `AppImpl.use()` when the user passes an object (not a function) to `.use()`.
 *
 * Translation:
 * - enhancer.enhance(registry) — registry shim translates register / registerIfAbsent /
 *   registerDecorator to app.set / app.setDefault / app.decorate via TOKEN_TO_SLOT.
 *   Tokens not in TOKEN_TO_SLOT go to a per-app token store visible via the
 *   Configuration shim's getComponent only.
 * - enhancer.onStart(config) — registered as `app.onStart('connect', () => onStart(configShim))`.
 * - enhancer.onStop()        — registered as `app.onStop('connect',  () => onStop())`.
 * - enhancer.order is IGNORED (CFG-03 — registration order = .use() call order).
 */
export function applyEnhancerToApp(
  enhancer: ConfigurationEnhancer,
  app: App,
): void {
  const registryShim = createRegistryShim(app)
  enhancer.enhance(registryShim)

  if (enhancer.onStart) {
    app.onStart("connect", async () => {
      await enhancer.onStart!(getOrCreateBridgeConfigShim(app))
    })
  }
  if (enhancer.onStop) {
    app.onStop("connect", async () => {
      await enhancer.onStop!()
    })
  }
}

// ---------------------------------------------------------------------------
// Per-app bridge state (token store + lazy config shim)
// ---------------------------------------------------------------------------

interface BridgeState {
  tokenStore: Map<string, ComponentBuilder<unknown>>
  configShim?: Configuration
}

const BRIDGE_STATE = new WeakMap<App, BridgeState>()

function getBridgeState(app: App): BridgeState {
  let state = BRIDGE_STATE.get(app)
  if (!state) {
    state = { tokenStore: new Map() }
    BRIDGE_STATE.set(app, state)
  }
  return state
}

function createRegistryShim(app: App): ComponentRegistry {
  // The ComponentRegistry interface in packages/common/src/configuration.ts has
  // overloaded signatures (with/without `name` parameter). We implement the
  // unnamed-token forms used by KronosDB / Axon Server / OpenTelemetry.
  const shim: ComponentRegistry = {
    register<T>(...args: any[]): ComponentRegistry {
      // Two shapes: register(type, builder) | register(type, name, builder)
      const token = args[0] as string
      const builder = (typeof args[1] === "string" ? args[2] : args[1]) as
        | ComponentBuilder<T>
      const slot = TOKEN_TO_SLOT[token]
      if (slot) {
        // Bridge factory adapts SlotFactory(resolved => component) into
        // ComponentBuilder(config => component) by routing through the bridge's
        // Configuration shim. Production enhancers' builders read other typed
        // slots off `config.getComponent(...)`, which the shim resolves to the
        // post-decoration native components.
        ;(app as any).set(slot, () =>
          builder(getOrCreateBridgeConfigShim(app)),
        )
      } else {
        getBridgeState(app).tokenStore.set(
          token,
          builder as ComponentBuilder<unknown>,
        )
      }
      return shim
    },
    registerIfAbsent<T>(...args: any[]): ComponentRegistry {
      const token = args[0] as string
      const builder = (typeof args[1] === "string" ? args[2] : args[1]) as
        | ComponentBuilder<T>
      const slot = TOKEN_TO_SLOT[token]
      if (slot) {
        // setDefault on the slot registry is ifAbsent — first registration wins.
        ;(app as any).setDefault(slot, () =>
          builder(getOrCreateBridgeConfigShim(app)),
        )
      } else {
        const store = getBridgeState(app).tokenStore
        if (!store.has(token)) {
          store.set(token, builder as ComponentBuilder<unknown>)
        }
      }
      return shim
    },
    registerDecorator<T>(...args: any[]): ComponentRegistry {
      // Two shapes:
      //   registerDecorator(type, order, decorator)
      //   registerDecorator(type, name, order, decorator)
      // CFG-03: order is IGNORED.
      let token: string
      let decorator: ComponentDecorator<T>
      if (typeof args[1] === "string") {
        token = args[0] as string
        decorator = args[3] as ComponentDecorator<T>
      } else {
        token = args[0] as string
        decorator = args[2] as ComponentDecorator<T>
      }
      const slot = TOKEN_TO_SLOT[token]
      if (!slot) {
        // Unknown token — no typed-slot to decorate. Quietly drop with a warning.
        console.warn(
          `[kronos:bridge] registerDecorator on untyped token '${token}' has no effect. Decorator skipped.`,
        )
        return shim
      }
      ;(app as any).decorate(slot, (inner: any, _resolved: KronosComponents) =>
        decorator(getOrCreateBridgeConfigShim(app), token, inner),
      )
      return shim
    },
    registerModule(_module: Module): ComponentRegistry {
      // No production enhancer registers Modules from inside enhance() —
      // Modules are a configurer-era construct that Plan 04 deletes wholesale.
      // If a caller ever exercises this path, fail loudly.
      throw new UnsupportedConfigurationMethodError("registerModule")
    },
    registerFactory(_factory: ComponentFactory): ComponentRegistry {
      throw new UnsupportedConfigurationMethodError("registerFactory")
    },
    setOverridePolicy(_policy): ComponentRegistry {
      // Tolerated as a no-op — kronos() slot registry has its own override
      // semantics (setDefault/set/forceSet) and the policy is irrelevant here.
      return shim
    },
  }
  return shim
}

function getOrCreateBridgeConfigShim(app: App): Configuration {
  const state = getBridgeState(app)
  if (state.configShim) return state.configShim

  const tokenStore = state.tokenStore
  const shim: Configuration = {
    hasComponent(...args: any[]): boolean {
      const token = args[0] as string
      if (TOKEN_TO_SLOT[token]) return true
      return tokenStore.has(token)
    },
    getComponent<T>(...args: any[]): T {
      const token = args[0] as string
      const slot = TOKEN_TO_SLOT[token]
      if (slot) {
        // Read post-resolution from AppImpl's _resolvedSlot accessor (set
        // during start() after slot decoration). Pre-start, fall back to the
        // user-supplied factory if one was registered via the bridge.
        const value = (app as any)._resolvedSlot?.(slot)
        if (value !== undefined) return value as T
        // Pre-start: invoke the slot's factory through the slot registry's
        // entry. We don't have direct access here, so fall back to the
        // bridge's tokenStore if the same token was tracked there too;
        // otherwise throw — a pre-start getComponent is an enhancer-bug
        // signal we'd rather surface loudly.
        const factory = tokenStore.get(token) as ComponentBuilder<T> | undefined
        if (factory) return factory(shim) as T
        throw new UnsupportedConfigurationMethodError(
          `getComponent('${token}') — slot '${slot}' not yet resolved`,
        )
      }
      const factory = tokenStore.get(token)
      if (!factory) {
        throw new UnsupportedConfigurationMethodError(
          `getComponent('${token}')`,
        )
      }
      return factory(shim) as T
    },
    getOptionalComponent<T>(type: string, name?: string): T | undefined {
      try {
        return shim.getComponent<T>(type, name as string)
      } catch {
        return undefined
      }
    },
    getComponents<T>(_type: string): Map<string, T> {
      // No production enhancer enumerates a multi-named component group.
      return new Map<string, T>()
    },
    getModules() {
      // Modules are configurer-era; the bridge is a one-way translation INTO
      // the App API and never round-trips Modules back out.
      return []
    },
    getParent() {
      return undefined
    },
  }

  state.configShim = shim
  return shim
}
