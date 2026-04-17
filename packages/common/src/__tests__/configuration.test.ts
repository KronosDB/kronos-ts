import { describe, expect, it } from "bun:test"
import {
  createComponentRegistry,
  ComponentOverrideError,
  type ComponentFactory,
  type Module,
  type Configuration,
} from "../configuration.js"

describe("ComponentRegistry", () => {
  describe("register and retrieve", () => {
    it("registers a component by type and retrieves it", () => {
      const reg = createComponentRegistry()
      reg.register("EventBus", () => ({ publish: () => {} }))
      const config = reg.build()

      const bus = config.getComponent<{ publish: () => void }>("EventBus")

      expect(bus).toBeDefined()
      expect(bus.publish).toBeFunction()
    })

    it("named components are distinct from unnamed", () => {
      const reg = createComponentRegistry()
      reg.register("TokenStore", () => "default-store")
      reg.register("TokenStore", "tracking", () => "tracking-store")
      const config = reg.build()

      expect(config.getComponent("TokenStore")).toBe("default-store")
      expect(config.getComponent("TokenStore", "tracking")).toBe("tracking-store")
    })

    it("throws when requesting a component that does not exist", () => {
      const config = createComponentRegistry().build()

      expect(() => config.getComponent("NonExistent")).toThrow('No component registered for "NonExistent"')
    })
  })

  describe("registerIfAbsent", () => {
    it("registers when no existing builder", () => {
      const reg = createComponentRegistry()
      reg.registerIfAbsent("Store", () => "first")
      const config = reg.build()

      expect(config.getComponent("Store")).toBe("first")
    })

    it("does not overwrite an existing builder", () => {
      const reg = createComponentRegistry()
      reg.register("Store", () => "original")
      reg.registerIfAbsent("Store", () => "replacement")
      const config = reg.build()

      expect(config.getComponent("Store")).toBe("original")
    })

    it("works with named components", () => {
      const reg = createComponentRegistry()
      reg.register("Store", "primary", () => "original")
      reg.registerIfAbsent("Store", "primary", () => "replacement")
      const config = reg.build()

      expect(config.getComponent("Store", "primary")).toBe("original")
    })
  })

  describe("decorators", () => {
    it("wraps a component with a decorator", () => {
      const reg = createComponentRegistry()
      reg.register("Serializer", () => "base")
      reg.registerDecorator<string>("Serializer", 0, (_cfg, _name, delegate) => `decorated(${delegate})`)
      const config = reg.build()

      expect(config.getComponent("Serializer")).toBe("decorated(base)")
    })

    it("applies decorators in order: lower order = first (innermost)", () => {
      const reg = createComponentRegistry()
      reg.register("Handler", () => "core")
      reg.registerDecorator<string>("Handler", 10, (_cfg, _name, d) => `outer(${d})`)
      reg.registerDecorator<string>("Handler", 1, (_cfg, _name, d) => `inner(${d})`)
      const config = reg.build()

      // order 1 runs first (wraps core), then order 10 wraps that
      expect(config.getComponent("Handler")).toBe("outer(inner(core))")
    })

    it("chains multiple decorators correctly", () => {
      const reg = createComponentRegistry()
      reg.register("Bus", () => "bus")
      reg.registerDecorator<string>("Bus", 0, (_cfg, _name, d) => `logging(${d})`)
      reg.registerDecorator<string>("Bus", 1, (_cfg, _name, d) => `tracing(${d})`)
      reg.registerDecorator<string>("Bus", 2, (_cfg, _name, d) => `metrics(${d})`)
      const config = reg.build()

      expect(config.getComponent("Bus")).toBe("metrics(tracing(logging(bus)))")
    })

    it("decorates named components independently", () => {
      const reg = createComponentRegistry()
      reg.register("Store", "a", () => "store-a")
      reg.register("Store", "b", () => "store-b")
      reg.registerDecorator<string>("Store", "a", 0, (_cfg, _name, d) => `wrapped(${d})`)
      const config = reg.build()

      expect(config.getComponent("Store", "a")).toBe("wrapped(store-a)")
      expect(config.getComponent("Store", "b")).toBe("store-b")
    })
  })

  describe("modules", () => {
    it("registers and retrieves modules", () => {
      const reg = createComponentRegistry()
      const module: Module = {
        name: "OrderModule",
        initialize: () => {},
      }
      reg.registerModule(module)
      const config = reg.build()

      expect(config.getModules()).toHaveLength(1)
      expect(config.getModules()[0].name).toBe("OrderModule")
    })

    it("supports multiple modules", () => {
      const reg = createComponentRegistry()
      reg.registerModule({ name: "A", initialize: () => {} })
      reg.registerModule({ name: "B", initialize: () => {} })
      const config = reg.build()

      expect(config.getModules()).toHaveLength(2)
    })
  })

  describe("override policy", () => {
    it("reject: throws on duplicate registration", () => {
      const reg = createComponentRegistry()
      reg.setOverridePolicy("reject")
      reg.register("Bus", () => "first")

      expect(() => reg.register("Bus", () => "second")).toThrow(ComponentOverrideError)
    })

    it("warn: allows override (component uses latest builder)", () => {
      const reg = createComponentRegistry()
      reg.setOverridePolicy("warn")
      reg.register("Bus", () => "first")
      reg.register("Bus", () => "second")
      const config = reg.build()

      expect(config.getComponent("Bus")).toBe("second")
    })

    it("allow: silently replaces", () => {
      const reg = createComponentRegistry()
      reg.setOverridePolicy("allow")
      reg.register("Bus", () => "first")
      reg.register("Bus", () => "second")
      const config = reg.build()

      expect(config.getComponent("Bus")).toBe("second")
    })

    it("reject policy does not affect first registration", () => {
      const reg = createComponentRegistry()
      reg.setOverridePolicy("reject")
      reg.register("Bus", () => "only")
      const config = reg.build()

      expect(config.getComponent("Bus")).toBe("only")
    })
  })

  describe("ComponentFactory", () => {
    it("is consulted when no builder exists", () => {
      const factory: ComponentFactory<string> = {
        canCreate: (type) => type === "DynamicService",
        create: (_cfg, type) => `dynamic-${type}`,
      }
      const reg = createComponentRegistry()
      reg.registerFactory(factory)
      const config = reg.build()

      expect(config.getComponent("DynamicService")).toBe("dynamic-DynamicService")
    })

    it("is not consulted when a builder exists", () => {
      let factoryCalled = false
      const factory: ComponentFactory<string> = {
        canCreate: () => { factoryCalled = true; return true },
        create: () => "from-factory",
      }
      const reg = createComponentRegistry()
      reg.register("Service", () => "from-builder")
      reg.registerFactory(factory)
      const config = reg.build()

      expect(config.getComponent("Service")).toBe("from-builder")
      expect(factoryCalled).toBe(false)
    })

    it("hasComponent returns true when factory can create", () => {
      const factory: ComponentFactory = {
        canCreate: (type) => type === "Virtual",
        create: () => "virtual",
      }
      const reg = createComponentRegistry()
      reg.registerFactory(factory)
      const config = reg.build()

      expect(config.hasComponent("Virtual")).toBe(true)
      expect(config.hasComponent("Missing")).toBe(false)
    })
  })

  describe("hierarchical configuration", () => {
    it("child falls back to parent for missing components", () => {
      const parentReg = createComponentRegistry()
      parentReg.register("Serializer", () => "parent-serializer")
      const parentConfig = parentReg.build()

      const childReg = createComponentRegistry(parentConfig)
      childReg.register("Bus", () => "child-bus")
      const childConfig = childReg.build()

      expect(childConfig.getComponent("Serializer")).toBe("parent-serializer")
      expect(childConfig.getComponent("Bus")).toBe("child-bus")
    })

    it("child component overrides parent", () => {
      const parentReg = createComponentRegistry()
      parentReg.register("Serializer", () => "parent-serializer")
      const parentConfig = parentReg.build()

      const childReg = createComponentRegistry(parentConfig)
      childReg.register("Serializer", () => "child-serializer")
      const childConfig = childReg.build()

      expect(childConfig.getComponent("Serializer")).toBe("child-serializer")
    })

    it("getParent returns the parent configuration", () => {
      const parentConfig = createComponentRegistry().build()
      const childConfig = createComponentRegistry(parentConfig).build()

      expect(childConfig.getParent()).toBe(parentConfig)
    })

    it("root configuration has no parent", () => {
      const config = createComponentRegistry().build()

      expect(config.getParent()).toBeUndefined()
    })
  })

  describe("getComponents", () => {
    it("returns all components of a given type", () => {
      const reg = createComponentRegistry()
      reg.register("Store", () => "default")
      reg.register("Store", "tracking", () => "tracking-store")
      reg.register("Store", "saga", () => "saga-store")
      const config = reg.build()

      const stores = config.getComponents<string>("Store")

      expect(stores.size).toBe(3)
      expect(stores.get("Store")).toBe("default")
      expect(stores.get("tracking")).toBe("tracking-store")
      expect(stores.get("saga")).toBe("saga-store")
    })

    it("merges parent components into result", () => {
      const parentReg = createComponentRegistry()
      parentReg.register("Store", "parent-store", () => "from-parent")
      const parentConfig = parentReg.build()

      const childReg = createComponentRegistry(parentConfig)
      childReg.register("Store", "child-store", () => "from-child")
      const childConfig = childReg.build()

      const stores = childConfig.getComponents<string>("Store")

      expect(stores.size).toBe(2)
      expect(stores.get("child-store")).toBe("from-child")
      expect(stores.get("parent-store")).toBe("from-parent")
    })

    it("returns empty map for unknown type", () => {
      const config = createComponentRegistry().build()

      expect(config.getComponents("Unknown").size).toBe(0)
    })
  })

  describe("getOptionalComponent", () => {
    it("returns the component when registered", () => {
      const reg = createComponentRegistry()
      reg.register("Bus", () => "the-bus")
      const config = reg.build()

      expect(config.getOptionalComponent("Bus")).toBe("the-bus")
    })

    it("returns undefined for missing components", () => {
      const config = createComponentRegistry().build()

      expect(config.getOptionalComponent("Missing")).toBeUndefined()
    })

    it("falls back to parent", () => {
      const parentReg = createComponentRegistry()
      parentReg.register("Bus", () => "parent-bus")
      const parentConfig = parentReg.build()

      const childConfig = createComponentRegistry(parentConfig).build()

      expect(childConfig.getOptionalComponent("Bus")).toBe("parent-bus")
    })
  })

  describe("lazy building and caching", () => {
    it("builds component on first access", () => {
      let buildCount = 0
      const reg = createComponentRegistry()
      reg.register("Service", () => {
        buildCount++
        return "built"
      })
      const config = reg.build()

      expect(buildCount).toBe(0)
      config.getComponent("Service")
      expect(buildCount).toBe(1)
    })

    it("caches component after first build", () => {
      let buildCount = 0
      const reg = createComponentRegistry()
      reg.register("Service", () => {
        buildCount++
        return { id: buildCount }
      })
      const config = reg.build()

      const first = config.getComponent("Service")
      const second = config.getComponent("Service")

      expect(buildCount).toBe(1)
      expect(first).toBe(second) // same reference
    })

    it("builder receives configuration for resolving dependencies", () => {
      const reg = createComponentRegistry()
      reg.register("Dep", () => "dependency")
      reg.register("Service", (cfg: Configuration) => `uses-${cfg.getComponent("Dep")}`)
      const config = reg.build()

      expect(config.getComponent("Service")).toBe("uses-dependency")
    })
  })

  describe("hasComponent", () => {
    it("returns true for registered components", () => {
      const reg = createComponentRegistry()
      reg.register("Bus", () => "bus")
      const config = reg.build()

      expect(config.hasComponent("Bus")).toBe(true)
    })

    it("returns false for missing components", () => {
      const config = createComponentRegistry().build()

      expect(config.hasComponent("Missing")).toBe(false)
    })

    it("checks parent configuration", () => {
      const parentReg = createComponentRegistry()
      parentReg.register("Bus", () => "bus")
      const parentConfig = parentReg.build()
      const childConfig = createComponentRegistry(parentConfig).build()

      expect(childConfig.hasComponent("Bus")).toBe(true)
    })
  })

  describe("fluent API", () => {
    it("register returns the registry for chaining", () => {
      const reg = createComponentRegistry()
      const result = reg
        .register("A", () => "a")
        .register("B", () => "b")
        .registerIfAbsent("C", () => "c")
        .setOverridePolicy("allow")

      expect(result).toBeDefined()
    })
  })
})
