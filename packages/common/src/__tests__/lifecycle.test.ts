import { describe, expect, it } from "bun:test"
import { createLifecycleRegistry, LifecyclePhase } from "../lifecycle.js"

describe("LifecycleRegistry", () => {
  describe("start", () => {
    it("executes start handlers in ascending phase order", async () => {
      // given
      const order: string[] = []
      const registry = createLifecycleRegistry()

      registry.onStart(LifecyclePhase.INBOUND_EVENT_CONNECTORS, () => { order.push("events") })
      registry.onStart(LifecyclePhase.EXTERNAL_CONNECTIONS, () => { order.push("connections") })
      registry.onStart(LifecyclePhase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS, () => { order.push("handlers") })

      // when
      await registry.start()

      // then — ascending: connections (-1000), handlers (0), events (1000)
      expect(order).toEqual(["connections", "handlers", "events"])
    })

    it("executes handlers in same phase concurrently", async () => {
      // given
      const started: string[] = []
      const registry = createLifecycleRegistry()

      registry.onStart(0, async () => {
        started.push("a-start")
        await new Promise(r => setTimeout(r, 10))
        started.push("a-end")
      })
      registry.onStart(0, async () => {
        started.push("b-start")
        await new Promise(r => setTimeout(r, 10))
        started.push("b-end")
      })

      // when
      await registry.start()

      // then — both started before either ended (concurrent)
      expect(started[0]).toBe("a-start")
      expect(started[1]).toBe("b-start")
    })

    it("passes config parameter to start handlers", async () => {
      // given
      const registry = createLifecycleRegistry()
      let receivedConfig: any

      registry.onStart(0, (config) => {
        receivedConfig = config
      })

      const testConfig = { name: "test-config" }

      // when
      await registry.start(testConfig)

      // then
      expect(receivedConfig).toBe(testConfig)
    })
  })

  describe("shutdown", () => {
    it("executes shutdown handlers in descending phase order", async () => {
      // given
      const order: string[] = []
      const registry = createLifecycleRegistry()

      registry.onShutdown(LifecyclePhase.EXTERNAL_CONNECTIONS, () => { order.push("connections") })
      registry.onShutdown(LifecyclePhase.INBOUND_EVENT_CONNECTORS, () => { order.push("events") })
      registry.onShutdown(LifecyclePhase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS, () => { order.push("handlers") })

      // when
      await registry.shutdown()

      // then — descending: events (1000), handlers (0), connections (-1000)
      expect(order).toEqual(["events", "handlers", "connections"])
    })
  })

  describe("phase timeout", () => {
    it("logs a warning and continues when a phase exceeds the timeout", async () => {
      // given
      const order: string[] = []
      const warnings: string[] = []
      const originalWarn = console.warn
      console.warn = (msg: string) => warnings.push(msg)

      try {
        const registry = createLifecycleRegistry({ phaseTimeoutMs: 50 })

        registry.onStart(0, async () => {
          await new Promise(r => setTimeout(r, 100))
          order.push("slow-phase")
        })
        registry.onStart(1000, () => {
          order.push("next-phase")
        })

        // when
        await registry.start()

        // then — both phases complete, warning logged for phase 0
        expect(warnings.length).toBeGreaterThanOrEqual(1)
        expect(warnings[0]).toContain("exceeded timeout")
        expect(order).toContain("next-phase")
      } finally {
        console.warn = originalWarn
      }
    })
  })

  describe("empty registry", () => {
    it("start and shutdown succeed with no handlers", async () => {
      // given
      const registry = createLifecycleRegistry()

      // when / then — no errors
      await registry.start()
      await registry.shutdown()
    })
  })
})
