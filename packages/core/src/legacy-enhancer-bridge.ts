// transitional: Phase 9 deletes
//
// Bridge adapter: routes legacy ConfigurationEnhancer instances passed to App.use()
// through the typed App API. The ONLY surviving consumer of the numeric LifecyclePhase
// scale + ComponentKeys string tokens after Phase 8.
//
// D-73, D-74, D-81. Production extensions (KronosDB, Axon Server, OpenTelemetry) keep
// loading via this bridge until Phase 9 migrates them to (app: App) => void.
//
// NOT exported from packages/core/src/index.ts. Internal only.

import type {
  ConfigurationEnhancer,
  ComponentRegistry,
  ComponentBuilder,
  ComponentDecorator,
  Configuration,
  Module,
  ComponentFactory,
} from "@kronos-ts/common"
import { ComponentKeys } from "@kronos-ts/common"
import type { App } from "./app.js"
import type { LifecycleStage } from "./lifecycle.js"
import type { KronosComponents, SlotName } from "./components.js"

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
