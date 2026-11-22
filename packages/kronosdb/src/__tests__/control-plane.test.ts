import { afterEach, describe, expect, it } from "bun:test"
import {
  concurrent,
  event,
  eventHandler,
  eventProcessor,
  inMemoryEventStore,
  inMemoryTokenStore,
  kronos,
  localCommandBus,
  localQueryBus,
  qn,
  segmented,
  unitOfWork,
} from "@kronos-ts/core"
import { z } from "zod"
import { kronosDbControlPlane, type ManagedEventProcessor } from "../control-plane.js"
import {
  platformConnection,
  type InstructionHandler,
  type PlatformConnection,
  type PlatformInstruction,
} from "../platform-service.js"
import { ProcessorAction } from "../protocol/kronosdb-protocol.js"
import { rig, until, type Rig } from "./bus-test-kit.js"
import { toEventProcessorInfo, type ProcessorStatusSupplier } from "../event-processor-info.js"

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

      expect(proc.events).toEqual(["stop", "start"])
    })

    it("routes split, merge and release to the named processor's segment", async () => {
      const platform = fakePlatform()
      const calls: string[] = []
      const proc: ManagedEventProcessor = {
        name: "proc-a",
        splitSegment: async (id) => void calls.push(`split ${id}`),
        mergeSegment: async (id) => void calls.push(`merge ${id}`),
        releaseSegment: async (id) => void calls.push(`release ${id}`),
      }
      kronosDbControlPlane({ platform }, [proc])

      await platform.push({ kind: "split-segment", processorName: "proc-a", segmentId: 2 })
      await platform.push({ kind: "merge-segment", processorName: "proc-a", segmentId: 6 })
      await platform.push({ kind: "release-segment", processorName: "proc-a", segmentId: 1 })

      expect(calls).toEqual(["split 2", "merge 6", "release 1"])
    })

    it("fails the instruction when the processor refuses it, so the platform stream reports why", async () => {
      const platform = fakePlatform()
      const proc: ManagedEventProcessor = {
        name: "proc-a",
        splitSegment: async () => {
          throw new Error("wrap it in segmented(p, count, sequenceOf)")
        },
      }
      kronosDbControlPlane({ platform }, [proc])

      await expect(platform.push({ kind: "split-segment", processorName: "proc-a", segmentId: 0 })).rejects.toThrow(
        "segmented(p, count, sequenceOf)",
      )
    })

    it("waits for a pause to finish stopping the processor", async () => {
      const platform = fakePlatform()
      let stopped = false
      const proc: ManagedEventProcessor = {
        name: "proc-a",
        stop: async () => {
          await new Promise((resolve) => setTimeout(resolve, 5))
          stopped = true
        },
      }
      kronosDbControlPlane({ platform }, [proc])

      await platform.push({ kind: "pause-processor", processorName: "proc-a" })
      expect(stopped).toBe(true)
    })

    it("fails an instruction for an unknown processor, naming it", async () => {
      const platform = fakePlatform()
      const proc = fakeProcessor("proc-a")
      kronosDbControlPlane({ platform }, [proc])

      const instructions: PlatformInstruction[] = [
        { kind: "pause-processor", processorName: "nope" },
        { kind: "start-processor", processorName: "nope" },
        { kind: "release-segment", processorName: "nope", segmentId: 0 },
        { kind: "split-segment", processorName: "nope", segmentId: 0 },
        { kind: "merge-segment", processorName: "nope", segmentId: 0 },
      ]
      for (const instruction of instructions) {
        await expect(platform.push(instruction)).rejects.toThrow('No event processor named "nope" in this application')
      }
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

    it("reports the segments this node holds, each as one part of the whole", () => {
      const platform = fakePlatform()
      const proc: ManagedEventProcessor = {
        name: "proc-a",
        running: true,
        segments: () => [
          { segment: 0, mask: 1, position: 10n, caughtUp: true, replaying: false },
          { segment: 3, mask: 3, position: 8n, caughtUp: false, replaying: true, error: new Error("boom") },
        ],
      }
      kronosDbControlPlane({ platform }, [proc])

      const [status] = platform.status()
      expect(status!.segments).toEqual([
        { segment: 0, onePartOf: 2, position: 10n, caughtUp: true, replaying: false, error: undefined },
        { segment: 3, onePartOf: 4, position: 8n, caughtUp: false, replaying: true, error: "boom" },
      ])
    })
  })

  describe("wire mapping", () => {
    it("puts each held segment on the wire, one active thread per segment", () => {
      const info = toEventProcessorInfo({
        name: "proc-a",
        running: true,
        caughtUp: false,
        replaying: false,
        position: 8n,
        segments: [
          { segment: 0, onePartOf: 2, position: 10n, caughtUp: true, replaying: false },
          { segment: 3, onePartOf: 4, position: 8n, caughtUp: false, replaying: true, error: "boom" },
        ],
      })
      expect(info.activeThreads).toBe(2)
      expect(info.segments).toEqual([
        { segmentId: 0, caughtUp: true, replaying: false, onePartOf: 2, tokenPosition: 10n, errorState: "" },
        { segmentId: 3, caughtUp: false, replaying: true, onePartOf: 4, tokenPosition: 8n, errorState: "boom" },
      ])
    })

    it("reports no segments for a node that holds none", () => {
      const info = toEventProcessorInfo({ name: "p", running: true, caughtUp: false, replaying: false, position: 0n, segments: [] })
      expect(info.activeThreads).toBe(0)
      expect(info.segments).toEqual([])
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

describe("kronosDbControlPlane on a platform stream", () => {
  let r: Rig | undefined
  afterEach(async () => {
    await r?.close()
    r = undefined
  })

  it("reports an instruction for an unknown processor to the server as failed, naming it", async () => {
    r = await rig()
    const server = r.server
    const proc = fakeProcessor("orders")
    const platform = platformConnection(r.client.connection, { processorsNotificationInitialDelayMs: 60_000 })
    const control = kronosDbControlPlane({ platform }, [proc])
    const results = () =>
      (server.platforms[0]?.connection.received ?? []).flatMap((f) => (f.frame.name === "InstructionResult" ? [f.frame.body] : []))
    const pause = (instructionId: string, processorName: string) =>
      server.toPlatform(0, { name: "ProcessorInstruction", body: { instructionId, action: ProcessorAction.pause, processorName, segment: 0 } })

    const originalError = console.error
    console.error = () => {}
    try {
      await until(() => server.platforms.length === 1, "the platform stream")
      pause("i-1", "nope")
      pause("i-2", "orders")
      await until(() => results().length === 2, "both results")
    } finally {
      console.error = originalError
      await control.close()
    }

    expect(results()[0]).toMatchObject({
      instructionId: "i-1",
      success: false,
      error: { message: 'No event processor named "nope" in this application' },
    })
    expect(results()[1]).toMatchObject({ instructionId: "i-2", success: true })
    expect(proc.events).toEqual(["stop"])
  })
})

describe("kronosDbControlPlane with a real segmented processor", () => {
  it("splits, merges and releases the processor's segments and reports each one", async () => {
    const Opened = event({ name: qn("test", "Opened"), payload: z.object({ id: z.string() }), tags: { id: (p) => p.id } })
    const byId = (e: { payload: unknown }) => (e.payload as { id: string }).id
    const eventStore = inMemoryEventStore()
    const processor = segmented(
      concurrent(eventProcessor("projection", eventStore, inMemoryTokenStore(), unitOfWork), byId),
      2,
      byId,
    )
    const app = kronos({
      eventHandlers: [
        {
          ...eventHandler(Opened, () => {}),
          commandBus: localCommandBus(unitOfWork),
          queryBus: localQueryBus(unitOfWork),
          processor,
        },
      ],
    })
    const platform = fakePlatform()
    kronosDbControlPlane({ platform }, app.processors)
    const held = () => platform.status()[0]!.segments!.map((s) => `${s.segment}/${s.onePartOf}`)

    try {
      // kronos starts the processor; its first claim round takes both segments.
      const deadline = Date.now() + 2000
      while (held().length < 2 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5))
      expect(held()).toEqual(["0/2", "1/2"])

      await platform.push({ kind: "split-segment", processorName: "projection", segmentId: 1 })
      expect(held()).toEqual(["0/2", "1/4", "3/4"])

      await platform.push({ kind: "merge-segment", processorName: "projection", segmentId: 3 })
      expect(held()).toEqual(["0/2", "1/2"])

      await platform.push({ kind: "release-segment", processorName: "projection", segmentId: 0 })
      expect(held()).toEqual(["1/2"])

      await expect(platform.push({ kind: "merge-segment", processorName: "projection", segmentId: 7 })).rejects.toThrow(
        "no segment 7",
      )
    } finally {
      await app.stop()
    }
  })
})
