import type { LifecycleRegistry } from "./lifecycle.js"

/**
 * A component identifier — a type key with an optional name for disambiguation.
 * Allows multiple components of the same type (e.g., different TokenStores per processor).
 */
export type ComponentId = {
  readonly type: string
  readonly name?: string
}

function componentKey(typeOrId: string | ComponentId, name?: string): string {
  if (typeof typeOrId === "string") {
    return name ? `${typeOrId}:${name}` : typeOrId
  }
  return typeOrId.name ? `${typeOrId.type}:${typeOrId.name}` : typeOrId.type
}

// ---------------------------------------------------------------------------
// Override policy
// ---------------------------------------------------------------------------

/**
 * Controls what happens when a component is registered with a key
 * that already has a builder.
 */
export type OverridePolicy = "allow" | "warn" | "reject"

// ---------------------------------------------------------------------------
// Search scope (for hierarchical configs)
// ---------------------------------------------------------------------------

/**
 * Controls where to search when resolving components in hierarchical configurations.
 */
export type SearchScope = "all" | "current" | "ancestors"

// ---------------------------------------------------------------------------
// Component factory
// ---------------------------------------------------------------------------

/**
 * A factory that can create components on demand.
 * Only consulted when a component is NOT already registered.
 * Enables dynamic component creation for multi-tenancy, proxying, etc.
 */
export interface ComponentFactory<T = unknown> {
  /** Whether this factory can create a component for the given key. */
  canCreate(type: string, name?: string): boolean
  /** Create the component. */
  create(config: Configuration, type: string, name?: string): T
}

// ---------------------------------------------------------------------------
// Component registry
// ---------------------------------------------------------------------------

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
   * Factories are consulted when a component is requested but not registered.
   */
  registerFactory(factory: ComponentFactory): ComponentRegistry

  /**
   * Set the override policy for this registry.
   * - "allow": silently replace existing builders
   * - "warn": log a warning when overriding (default)
   * - "reject": throw an error on duplicate registrations
   */
  setOverridePolicy(policy: OverridePolicy): ComponentRegistry
}

/**
 * Resolved configuration — provides access to built components.
 */
export interface Configuration {
  /**
   * Get a component by type (and optional name).
   * Builds it lazily on first access, then caches.
   */
  getComponent<T>(type: string): T
  getComponent<T>(type: string, name: string): T

  /**
   * Get a component, returning undefined if not registered (instead of throwing).
   */
  getOptionalComponent<T>(type: string, name?: string): T | undefined

  /**
   * Get all components of a given type as a map of name → component.
   * Unnamed components use the type as the key.
   */
  getComponents<T>(type: string): Map<string, T>

  /**
   * Check if a component is registered.
   */
  hasComponent(type: string, name?: string): boolean

  /**
   * Get all registered modules.
   */
  getModules(): ReadonlyArray<Module>

  /**
   * Get the parent configuration (if this is a nested/child config).
   */
  getParent(): Configuration | undefined
}

/**
 * Builds a component, given access to the full configuration for resolving dependencies.
 */
export type ComponentBuilder<T> = (config: Configuration) => T

/**
 * Decorates/wraps a component. Receives the configuration, the component's
 * registered name (or the type key if unnamed), and the built delegate.
 *
 * Aligned with Java's `(config, name, delegate)` decorator signature.
 */
export type ComponentDecorator<T> = (config: Configuration, name: string, delegate: T) => T

/**
 * A self-contained group of related handlers/components that can register
 * itself with the messaging infrastructure.
 */
export interface Module {
  readonly name: string
  /**
   * Called after all components are built. The module subscribes its handlers
   * to the appropriate buses.
   */
  initialize(config: Configuration): void
}

/**
 * Base interface for all configurers. Provides access to the component
 * registry, lifecycle registry, and the build method.
 *
 * All configurers in the hierarchy implement this interface:
 * `MessagingConfigurer`, `ModellingConfigurer`, `EventSourcingConfigurer`.
 *
 * Aligned with AF5's `ApplicationConfigurer`.
 */
export interface ApplicationConfigurer {
  /** Direct access to the component registry. */
  componentRegistry(fn: (registry: ComponentRegistry) => void): ApplicationConfigurer
  /** Access the lifecycle registry for startup/shutdown phase ordering. */
  lifecycleRegistry(fn: (registry: LifecycleRegistry) => void): ApplicationConfigurer
}

/**
 * Enhances a component registry with additional components, decorators, etc.
 */
export interface ConfigurationEnhancer {
  enhance(registry: ComponentRegistry): void
  /** Lower values execute first. Defaults to 0. */
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
 * Creates a component registry and builds a configuration from it.
 *
 * @param parent Optional parent configuration for hierarchical resolution
 */
export function createComponentRegistry(
  parent?: Configuration,
): ComponentRegistry & { build(): Configuration } {
  const builders = new Map<string, ComponentBuilder<unknown>>()
  const decorators = new Map<string, Array<{ order: number; decorator: ComponentDecorator<unknown> }>>()
  const modules: Module[] = []
  const factories: ComponentFactory[] = []
  let overridePolicy: OverridePolicy = "warn"

  function applyOverridePolicy(key: string): void {
    if (!builders.has(key)) return
    switch (overridePolicy) {
      case "reject":
        throw new ComponentOverrideError(key)
      case "warn":
        console.warn(`Component "${key}" is being overridden. Set override policy to "allow" to suppress this warning.`)
        break
      case "allow":
        break
    }
  }

  const registry: ComponentRegistry = {
    register(...args: any[]) {
      const key = typeof args[1] === "string" ? componentKey(args[0], args[1]) : componentKey(args[0])
      const builder = typeof args[1] === "string" ? args[2] : args[1]
      applyOverridePolicy(key)
      builders.set(key, builder)
      return registry
    },

    registerIfAbsent(...args: any[]) {
      const key = typeof args[1] === "string"
        ? componentKey(args[0], args[1])
        : componentKey(args[0])
      const builder = typeof args[1] === "string" ? args[2] : args[1]
      if (!builders.has(key)) {
        builders.set(key, builder)
      }
      return registry
    },

    registerDecorator(...args: any[]) {
      let key: string, order: number, decorator: ComponentDecorator<unknown>
      if (typeof args[1] === "string") {
        key = componentKey(args[0], args[1])
        order = args[2]
        decorator = args[3]
      } else {
        key = componentKey(args[0])
        order = args[1]
        decorator = args[2]
      }
      if (!decorators.has(key)) {
        decorators.set(key, [])
      }
      decorators.get(key)!.push({ order, decorator })
      return registry
    },

    registerModule(module: Module) {
      modules.push(module)
      return registry
    },

    registerFactory(factory: ComponentFactory) {
      factories.push(factory)
      return registry
    },

    setOverridePolicy(policy: OverridePolicy) {
      overridePolicy = policy
      return registry
    },
  } as ComponentRegistry

  function build(): Configuration {
    const cache = new Map<string, unknown>()

    const config: Configuration = {
      getComponent<T>(...args: any[]): T {
        const key = args.length > 1 ? componentKey(args[0], args[1]) : componentKey(args[0])

        if (cache.has(key)) {
          return cache.get(key) as T
        }

        let builder = builders.get(key)

        // If no builder, try factories
        if (!builder) {
          const type = args[0] as string
          const name = args.length > 1 ? args[1] as string : undefined
          for (const factory of factories) {
            if (factory.canCreate(type, name)) {
              builder = (cfg) => factory.create(cfg, type, name)
              break
            }
          }
        }

        // If still no builder, try parent
        if (!builder && parent) {
          const result = parent.getOptionalComponent<T>(args[0], args[1])
          if (result !== undefined) {
            cache.set(key, result)
            return result
          }
        }

        if (!builder) {
          throw new Error(`No component registered for "${key}"`)
        }

        let component = builder(config)

        const decs = decorators.get(key)
        if (decs) {
          const sorted = [...decs].sort((a, b) => a.order - b.order)
          for (const { decorator } of sorted) {
            component = decorator(config, key, component)
          }
        }

        cache.set(key, component)
        return component as T
      },

      getOptionalComponent<T>(type: string, name?: string): T | undefined {
        const key = name ? componentKey(type, name) : componentKey(type)

        if (cache.has(key)) return cache.get(key) as T

        // Check if we have a builder or factory for this
        if (builders.has(key)) {
          return config.getComponent(type, name!)
        }

        for (const factory of factories) {
          if (factory.canCreate(type, name)) {
            return config.getComponent(type, name!)
          }
        }

        // Try parent
        if (parent) {
          return parent.getOptionalComponent<T>(type, name)
        }

        return undefined
      },

      getComponents<T>(type: string): Map<string, T> {
        const result = new Map<string, T>()
        const prefix = `${type}:`

        // Check local builders
        for (const key of builders.keys()) {
          if (key === type || key.startsWith(prefix)) {
            const name = key === type ? type : key.slice(prefix.length)
            result.set(name, config.getComponent<T>(type, key === type ? undefined! : name))
          }
        }

        // Check parent (merge, local takes precedence)
        if (parent) {
          const parentComponents = parent.getComponents<T>(type)
          for (const [name, component] of parentComponents) {
            if (!result.has(name)) {
              result.set(name, component)
            }
          }
        }

        return result
      },

      hasComponent(...args: any[]): boolean {
        const key = args.length > 1 && args[1] !== undefined
          ? componentKey(args[0], args[1])
          : componentKey(args[0])

        if (builders.has(key)) return true

        // Check factories
        const type = args[0] as string
        const name = args.length > 1 ? args[1] as string : undefined
        for (const factory of factories) {
          if (factory.canCreate(type, name)) return true
        }

        // Check parent
        if (parent) {
          return parent.hasComponent(args[0], args[1])
        }

        return false
      },

      getModules(): ReadonlyArray<Module> {
        return modules
      },

      getParent(): Configuration | undefined {
        return parent
      },
    } as Configuration

    return config
  }

  return Object.assign(registry, { build })
}
