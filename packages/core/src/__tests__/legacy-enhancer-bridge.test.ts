/**
 * Plan 08-03b Wave 0 — RED tests for the legacy-enhancer bridge.
 *
 * These tests pin the contract for `applyEnhancerToApp` (D-73, D-74, D-81, CFG-03):
 *
 *   - App.use() detects function vs object and routes object args through the bridge.
 *   - registry.register / registerIfAbsent / registerDecorator on typed slots
 *     translate to app.set / setDefault / decorate via TOKEN_TO_SLOT.
 *   - enhancer.onStart(config) fires during the connect lifecycle stage.
 *   - enhancer.onStop() fires during the connect lifecycle stage in reverse order.
 *   - Configuration shim throws UnsupportedConfigurationMethodError on unknown tokens
 *     (loud failure — no silent undefined returns).
 *   - CFG-03: enhancer.order is IGNORED — .use() registration order wins.
 *   - D-81 fallback PHASE_TO_STAGE inverter exists for completeness (no current
 *     production caller — see RESEARCH §Production Enhancer Audit).
 *
 * Bridge file does NOT exist yet — these tests will fail at import / typecheck
 * until Task 2 implements `legacy-enhancer-bridge.ts` and overloads App.use().
 */
import { describe, it, expect } from "bun:test"
import {
  ComponentKeys,
  type ConfigurationEnhancer,
  type ComponentRegistry,
  type Configuration,
} from "../legacy-enhancer-bridge.js"
import { kronos } from "../kronos.js"
import { createSimpleCommandBus } from "@kronos-ts/messaging"
import type { CommandBus, CommandMessage } from "@kronos-ts/messaging"
// Direct relative import — bridge is private (D-74), NOT exported from index.
import {
  applyEnhancerToApp,
  UnsupportedConfigurationMethodError,
} from "../legacy-enhancer-bridge.js"

describe("legacy-enhancer-bridge (D-73, D-74, D-81, CFG-03)", () => {
  it("Test 1: App.use() routes function arg to extension; routes enhancer-object arg through bridge", async () => {
    let extCalled = false
    let enhanceCalled = false
    const enhancer: ConfigurationEnhancer = {
      enhance(_registry) {
        enhanceCalled = true
      },
    }
    const app = kronos({ quiet: true })
      .use((_a) => {
        extCalled = true
      })
      .use(enhancer)
    const running = await app.start()
    expect(extCalled).toBe(true)
    expect(enhanceCalled).toBe(true)
    await running.stop()
  })

  it("Test 2: registry.register('commandBus', factory) translates to app.set('commandBus', factory)", async () => {
    let dispatchedVia: "mock" | "default" | "none" = "none"
    const mockBus: CommandBus = {
      async dispatch(_msg: CommandMessage): Promise<unknown> {
        dispatchedVia = "mock"
        return undefined
      },
      subscribe(_name: string, _handler) {
        // accept subscriptions but record nothing
      },
    }
    const enhancer: ConfigurationEnhancer = {
      enhance(registry) {
        registry.register(ComponentKeys.COMMAND_BUS, () => mockBus)
      },
    }
    const app = kronos({ quiet: true }).use(enhancer)
    const running = await app.start()
    // Probe via the running gateway — its underlying bus must be our mock
    // (post-decoration, but our mock CommandBus is the inner the intercepting
    // default wraps, so dispatch ultimately reaches it).
    try {
      await running.commandGateway.send(
        // Construct a minimal command via the messaging gateway shape — any name works
        // because the mock bus accepts all dispatches without lookup.
        { name: { namespace: "x", name: "Y" }, payload: undefined } as any,
        undefined as any,
      )
    } catch {
      // Some send() overloads validate; the key check is whether mockBus.dispatch fired.
    }
    expect(dispatchedVia).toBe("mock")
    await running.stop()
  })

  it("Test 3: registry.registerDecorator('commandBus', order, decorator) translates to app.decorate (order IGNORED per CFG-03)", async () => {
    let decoratorApplied = false
    const enhancer: ConfigurationEnhancer = {
      enhance(registry) {
        registry.registerDecorator(
          ComponentKeys.COMMAND_BUS,
          -200, // CFG-03: this number is IGNORED
          (_config, _name, delegate) => {
            decoratorApplied = true
            return delegate
          },
        )
      },
    }
    const app = kronos({ quiet: true }).use(enhancer)
    const running = await app.start()
    expect(decoratorApplied).toBe(true)
    await running.stop()
  })

  it("Test 4: enhancer.onStart(config) fires during the connect stage", async () => {
    let startCalled = false
    let connectStageReached = false
    const enhancer: ConfigurationEnhancer = {
      enhance(_r) {
        /* no-op */
      },
      onStart(_config) {
        startCalled = true
      },
    }
    const app = kronos({ quiet: true })
      .onStart("connect", () => {
        connectStageReached = true
      })
      .use(enhancer)
    const running = await app.start()
    expect(startCalled).toBe(true)
    expect(connectStageReached).toBe(true)
    await running.stop()
  })

  it("Test 5: enhancer.onStop() fires during the connect stage on stop (reverse order)", async () => {
    let stopCalled = false
    const enhancer: ConfigurationEnhancer = {
      enhance(_r) {
        /* no-op */
      },
      onStop() {
        stopCalled = true
      },
    }
    const app = kronos({ quiet: true }).use(enhancer)
    const running = await app.start()
    expect(stopCalled).toBe(false) // onStop has not fired yet
    await running.stop()
    expect(stopCalled).toBe(true)
  })

  it("Test 6: Configuration shim throws UnsupportedConfigurationMethodError on unknown tokens", async () => {
    let caughtError: unknown
    const enhancer: ConfigurationEnhancer = {
      enhance(_r) {
        /* no-op */
      },
      onStart(config) {
        try {
          config.getComponent("nonExistentToken")
        } catch (e) {
          caughtError = e
        }
      },
    }
    const app = kronos({ quiet: true }).use(enhancer)
    const running = await app.start()
    expect(caughtError).toBeInstanceOf(UnsupportedConfigurationMethodError)
    await running.stop()
  })

  it("Test 7: applyEnhancerToApp invokes enhance with a registry shim — direct API path (D-81 audit fallback documented but no production caller)", async () => {
    // RESEARCH §Production Enhancer Audit: KronosDB / Axon Server / OpenTelemetry
    // do NOT invoke `lifecycleRegistry.onStart(numericPhase, fn)` from inside
    // enhance(). The D-81 phase→stage inverter exists for completeness but has
    // no live production caller. This test documents the direct enhance() path:
    // applyEnhancerToApp(enhancer, app) builds the registry shim and dispatches
    // to enhancer.enhance(registry) — proving the shim surface is wired up.
    let registryReceived: ComponentRegistry | undefined
    const enhancer: ConfigurationEnhancer = {
      enhance(registry) {
        registryReceived = registry
      },
    }
    const app = kronos({ quiet: true })
    applyEnhancerToApp(enhancer, app)
    expect(registryReceived).toBeDefined()
    expect(typeof registryReceived!.register).toBe("function")
    expect(typeof registryReceived!.registerIfAbsent).toBe("function")
    expect(typeof registryReceived!.registerDecorator).toBe("function")
    const running = await app.start()
    await running.stop()
  })

  it("Test 8: CFG-03 — enhancer.order is IGNORED; enhance() invocation follows .use() registration order", async () => {
    const callOrder: string[] = []
    const first: ConfigurationEnhancer = {
      order: 1000, // CFG-03: ignored
      enhance(_r) {
        callOrder.push("first")
      },
    }
    const second: ConfigurationEnhancer = {
      order: -1000, // CFG-03: ignored — would have run first under legacy ordering
      enhance(_r) {
        callOrder.push("second")
      },
    }
    const app = kronos({ quiet: true }).use(first).use(second)
    const running = await app.start()
    expect(callOrder).toEqual(["first", "second"])
    await running.stop()
  })
})
