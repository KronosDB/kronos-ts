import { describe, expect, it } from "bun:test"
import { kronosDbControlPlane, type ManagedEventProcessor } from "../control-plane.js"
import type {
  InstructionHandler,
  PlatformConnection,
  PlatformInstruction,
} from "../platform-service.js"
import type { ProcessorStatusSupplier } from "../event-processor-info.js"

/**
 * A PlatformConnection double that records the ORDER of lifecycle calls, so the
 * ordering invariant can be asserted rather than reviewed.
 */
function fakePlatform() {
  const calls: string[] = []
  const handlers: InstructionHandler[] = []
  const suppliers: ProcessorStatusSupplier[] = []
  let connected = false

  const platform: PlatformConnection & {
    calls: string[]
    push(instruction: PlatformInstruction): Promise<void>
    status(): ReturnType<ProcessorStatusSupplier>
  } = {
    async start() {
      calls.push("start")
      connected = true
    },
    stop() {
      calls.push("stop")
      connected = false
    },
    onInstruction(handler) {
      calls.push("onInstruction")
      handlers.push(handler)
    },
    registerProcessorStatusSupplier(supplier) {
      calls.push("registerProcessorStatusSupplier")
      suppliers.push(supplier)
    },
    get connected() {
      return connected
    },
    async subscriptionsAcked() {
      return connected
    },
    calls,
    async push(instruction) {
      for (const handler of handlers) await handler(instruction)
    },
    status() {
      return suppliers.flatMap((s) => s())
    },
  }

  return platform
}

function fakeProcessor(name: string) {
  const events: string[] = []
  const proc: ManagedEventProcessor & { events: string[] } = {
    name,
    running: true,
    events,
    start() {
      events.push("start")
    },
    stop() {
      events.push("stop")
    },
  }
  return proc
}

describe("kronosDbControlPlane", () => {
  describe("ordering invariant (regression lock)", () => {
    it("calls platform.start() only AFTER the instruction handler and status supplier are registered", () => {
      const platform = fakePlatform()
      kronosDbControlPlane({ platform }, [fakeProcessor("p")])

      expect(platform.calls).toEqual([
        "onInstruction",
        "registerProcessorStatusSupplier",
        "start",
      ])
    })

    it("start() is last, so no instruction can arrive before a handler exists", () => {
      const platform = fakePlatform()
      kronosDbControlPlane({ platform }, [fakeProcessor("p")])

      expect(platform.calls.indexOf("start")).toBe(platform.calls.length - 1)
      expect(platform.calls.indexOf("onInstruction")).toBeLessThan(
        platform.calls.indexOf("start"),
      )
      expect(platform.calls.indexOf("registerProcessorStatusSupplier")).toBeLessThan(
        platform.calls.indexOf("start"),
      )
    })
  })

  describe("instruction routing", () => {
    it("routes each instruction kind to the named processor", async () => {
      const platform = fakePlatform()
      const proc = fakeProcessor("proc-a")
      kronosDbControlPlane({ platform }, [proc])

      await platform.push({ kind: "pause-processor", processorName: "proc-a" })
      await platform.push({ kind: "start-processor", processorName: "proc-a" })
      await platform.push({ kind: "split-segment", processorName: "proc-a", segmentId: 2 })

      // split/merge/release are ignored: one lane per processor
      expect(proc.events).toEqual(["stop", "start"])
    })

    it("ignores an instruction for an unknown processor without throwing", async () => {
      const platform = fakePlatform()
      const proc = fakeProcessor("proc-a")
      kronosDbControlPlane({ platform }, [proc])

      await platform.push({ kind: "pause-processor", processorName: "nope" })
      expect(proc.events).toEqual([])
    })

    it("skips an instruction a processor does not implement", async () => {
      const platform = fakePlatform()
      const bare: ManagedEventProcessor = { name: "bare" }
      kronosDbControlPlane({ platform }, [bare])

      await platform.push({ kind: "split-segment", processorName: "bare", segmentId: 1 })
      // no throw is the assertion
      expect(platform.connected).toBe(true)
    })

    it("ignores reconnect-request (not a processor instruction)", async () => {
      const platform = fakePlatform()
      const proc = fakeProcessor("proc-a")
      kronosDbControlPlane({ platform }, [proc])

      await platform.push({ kind: "reconnect-request" })
      expect(proc.events).toEqual([])
    })
  })

  describe("processor source", () => {
    it("accepts a name-keyed map, which is what app.processors is", () => {
      const platform = fakePlatform()
      const proc = fakeProcessor("proc-a")
      const appProcessors: ReadonlyMap<string, unknown> = new Map([["proc-a", proc]])

      kronosDbControlPlane({ platform }, appProcessors)

      expect(platform.status().map((s) => s.name)).toEqual(["proc-a"])
    })

    it("ignores map entries that are not processor-shaped (values are typed unknown)", () => {
      const platform = fakePlatform()
      const appProcessors: ReadonlyMap<string, unknown> = new Map<string, unknown>([
        ["proc-a", fakeProcessor("proc-a")],
        ["junk", 42],
        ["nullish", null],
      ])

      kronosDbControlPlane({ platform }, appProcessors)

      expect(platform.status().map((s) => s.name)).toEqual(["proc-a"])
    })

    it("re-reads the source, so processors registered later are addressable", async () => {
      const platform = fakePlatform()
      const live = new Map<string, unknown>()
      kronosDbControlPlane({ platform }, live)

      expect(platform.status()).toEqual([])

      const late = fakeProcessor("late")
      live.set("late", late)

      await platform.push({ kind: "pause-processor", processorName: "late" })
      expect(late.events).toEqual(["stop"])
      expect(platform.status().map((s) => s.name)).toEqual(["late"])
    })
  })

  describe("status reporting", () => {
    it("reports name, running and a default position when the processor exposes no status()", () => {
      const platform = fakePlatform()
      kronosDbControlPlane({ platform }, [fakeProcessor("proc-a")])

      const [status] = platform.status()
      expect(status).toEqual({ name: "proc-a", running: true, caughtUp: true, replaying: false, position: 0n, error: undefined })
    })

    it("reads status() when the processor has one", () => {
      const platform = fakePlatform()
      const proc: ManagedEventProcessor = {
        name: "proc-a",
        running: true,
        status: () => ({ caughtUp: false, replaying: true, position: 4n, error: new Error("boom") }),
      }
      kronosDbControlPlane({ platform }, [proc])

      const [status] = platform.status()
      expect(status).toEqual({ name: "proc-a", running: true, caughtUp: false, replaying: true, position: 4n, error: "boom" })
    })
  })

  describe("teardown", () => {
    it("close() stops the platform stream", async () => {
      const platform = fakePlatform()
      const control = kronosDbControlPlane({ platform }, [fakeProcessor("p")])

      await control.close()

      expect(platform.calls.at(-1)).toBe("stop")
      expect(platform.connected).toBe(false)
    })

    it("exposes the platform it was handed", () => {
      const platform = fakePlatform()
      const control = kronosDbControlPlane({ platform }, [])
      expect(control.platform).toBe(platform)
    })
  })
})
