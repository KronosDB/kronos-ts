/**
 * Unit tests for the extracted platform control plane.
 *
 * These run against a fake `PlatformConnection` — no container, no gRPC. The
 * control plane takes the shared CONNECTION and reads `.platform` off it, so a
 * bare `{ platform }` record is the whole dependency a test has to supply.
 * What they pin down is the contract the extraction has to preserve:
 *
 *   1. ORDERING: `onInstruction` and `registerProcessorStatusSupplier` are both
 *      called BEFORE `platform.start()`. An instruction arriving in that gap
 *      would be dropped; an early status request would find no supplier.
 *   2. instruction → processor-method routing, by name, skipping absent methods
 *   3. the ProcessorStatus mapping (streaming vs subscribing, per-segment vs
 *      the synthetic single segment)
 *   4. the processor source is read on every instruction and status report;
 *      a one-shot iterator (`Map.values()`) is copied and still reports on
 *      every tick
 */
import { describe, it, expect } from "bun:test"
import type {
  PlatformConnection,
  InstructionHandler,
  PlatformInstruction,
} from "../platform-service.js"
import { toEventProcessorInfo, type ProcessorStatusSupplier } from "../event-processor-info.js"
import { axonServerControlPlane, type ManagedEventProcessor } from "../control-plane.js"

type FakePlatform = PlatformConnection & {
  /** Every call recorded in order, for the ordering assertion. */
  readonly calls: string[]
  emit(instruction: PlatformInstruction): Promise<void>
  statuses(): ReturnType<ProcessorStatusSupplier>
}

function fakePlatform(): FakePlatform {
  const calls: string[] = []
  const handlers: InstructionHandler[] = []
  const suppliers: ProcessorStatusSupplier[] = []
  let connected = false

  return {
    calls,
    async armConnectionMonitoring() {
      // The DATA path's half. The control plane must never call it — if it
      // shows up in `calls`, the split has leaked back into this file.
      calls.push("armConnectionMonitoring")
      connected = true
    },
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
    async emit(instruction) {
      for (const handler of handlers) await handler(instruction)
    },
    statuses() {
      return suppliers.flatMap((s) => s())
    },
  }
}

/** A processor that records which control methods were invoked. */
function spyProcessor(name: string, overrides: Partial<ManagedEventProcessor> = {}) {
  const calls: string[] = []
  const proc: ManagedEventProcessor = {
    name,
    stop: () => void calls.push("stop"),
    start: async () => void calls.push("start"),
    ...overrides,
  }
  return { proc, calls }
}

describe("axonServerControlPlane — handler ordering", () => {
  it("registers the instruction handler and status supplier BEFORE platform.start()", async () => {
    const platform = fakePlatform()
    await axonServerControlPlane({ platform }, [])

    expect(platform.calls).toEqual(["onInstruction", "registerProcessorStatusSupplier", "start"])
    // The load-bearing property, stated independently of the exact call list:
    // nothing is registered after the stream is live.
    expect(platform.calls.indexOf("start")).toBe(platform.calls.length - 1)
  })

  it("starts the platform stream itself", async () => {
    const platform = fakePlatform()
    expect(platform.connected).toBe(false)
    await axonServerControlPlane({ platform }, [])
    expect(platform.connected).toBe(true)
  })

  it("arms nothing on the data path — that is the backend's half", async () => {
    // `platform.start()` is idempotent about the stream, so the control plane
    // works whether or not `axon.start()` already armed connection monitoring.
    // What it must NOT do is reach for the data path's entry point itself.
    const platform = fakePlatform()
    await axonServerControlPlane({ platform }, [])
    expect(platform.calls).not.toContain("armConnectionMonitoring")
  })

  it("close() stops the platform stream", async () => {
    const platform = fakePlatform()
    const control = await axonServerControlPlane({ platform }, [])
    await control.close()
    expect(platform.connected).toBe(false)
    expect(platform.calls.at(-1)).toBe("stop")
  })
})

describe("axonServerControlPlane — instruction routing", () => {
  it("routes each instruction kind to the named processor", async () => {
    const platform = fakePlatform()
    const a = spyProcessor("proc-a")
    const b = spyProcessor("proc-b")
    await axonServerControlPlane({ platform }, [a.proc, b.proc])

    await platform.emit({ kind: "pause-processor", processorName: "proc-a" })
    await platform.emit({ kind: "start-processor", processorName: "proc-a" })

    expect(a.calls).toEqual(["stop", "start"])
    expect(b.calls).toEqual([])
  })

  it("routes split, merge and release to the named processor's segment", async () => {
    const platform = fakePlatform()
    const calls: string[] = []
    const { proc } = spyProcessor("proc-a", {
      splitSegment: async (id) => void calls.push(`split ${id}`),
      mergeSegment: async (id) => void calls.push(`merge ${id}`),
      releaseSegment: async (id) => void calls.push(`release ${id}`),
    })
    await axonServerControlPlane({ platform }, [proc])

    await platform.emit({ kind: "split-segment", processorName: "proc-a", segmentId: 4 })
    await platform.emit({ kind: "merge-segment", processorName: "proc-a", segmentId: 6 })
    await platform.emit({ kind: "release-segment", processorName: "proc-a", segmentId: 1 })

    expect(calls).toEqual(["split 4", "merge 6", "release 1"])
  })

  it("does not hold the instruction up while a paused processor finishes stopping", async () => {
    const platform = fakePlatform()
    let finish: () => void = () => {}
    const { proc } = spyProcessor("proc-a", { stop: () => new Promise<void>((resolve) => (finish = resolve)) })
    await axonServerControlPlane({ platform }, [proc])

    // Resolves although stop() has not: instructions run in the stream's read loop.
    await platform.emit({ kind: "pause-processor", processorName: "proc-a" })
    finish()
  })

  it("fails an instruction for an unknown processor, naming it", async () => {
    const platform = fakePlatform()
    const a = spyProcessor("proc-a")
    await axonServerControlPlane({ platform }, [a.proc])

    const instructions: PlatformInstruction[] = [
      { kind: "pause-processor", processorName: "nope" },
      { kind: "start-processor", processorName: "nope" },
      { kind: "release-segment", processorName: "nope", segmentId: 0 },
      { kind: "split-segment", processorName: "nope", segmentId: 0 },
      { kind: "merge-segment", processorName: "nope", segmentId: 0 },
    ]
    for (const instruction of instructions) {
      await expect(platform.emit(instruction)).rejects.toThrow('No event processor named "nope" in this application')
    }
    expect(a.calls).toEqual([])
  })

  it("skips control methods a processor does not implement", async () => {
    const platform = fakePlatform()
    // A subscribing processor: no segments, so no split/merge/release.
    const minimal: ManagedEventProcessor = { name: "minimal" }
    await axonServerControlPlane({ platform }, [minimal])

    await platform.emit({ kind: "split-segment", processorName: "minimal", segmentId: 1 })
    await platform.emit({ kind: "pause-processor", processorName: "minimal" })
    // No throw is the assertion.
    expect(platform.connected).toBe(true)
  })

  it("ignores non-processor instructions (topology, reconnect)", async () => {
    const platform = fakePlatform()
    const a = spyProcessor("proc-a")
    await axonServerControlPlane({ platform }, [a.proc])

    await platform.emit({ kind: "reconnect-request" })
    await platform.emit({ kind: "command-handler-added", componentName: "c", commandName: "x" })
    expect(a.calls).toEqual([])
  })
})

describe("axonServerControlPlane — status reporting", () => {
  it("reports name, running and position for a processor with no status()", async () => {
    const platform = fakePlatform()
    const proc: ManagedEventProcessor = { name: "p", running: true, position: 17n }
    await axonServerControlPlane({ platform }, [proc])

    const [status] = platform.statuses()
    expect(status).toEqual({ name: "p", running: true, caughtUp: true, replaying: false, position: 17n, error: undefined })
  })

  it("reads status() when the processor has one", async () => {
    const platform = fakePlatform()
    const proc: ManagedEventProcessor = {
      name: "p",
      running: true,
      status: () => ({ position: 9n, caughtUp: false, replaying: true, error: new Error("boom") }),
    }
    await axonServerControlPlane({ platform }, [proc])

    const [status] = platform.statuses()
    expect(status).toEqual({ name: "p", running: true, caughtUp: false, replaying: true, position: 9n, error: "boom" })
  })

  it("reports the segments this node holds, each as one part of the whole", async () => {
    const platform = fakePlatform()
    const proc: ManagedEventProcessor = {
      name: "p",
      running: true,
      segments: () => [
        { segment: 1, mask: 1, position: 5n, caughtUp: true, replaying: false },
        { segment: 2, mask: 3, position: 3n, caughtUp: false, replaying: false, error: new Error("boom") },
      ],
    }
    await axonServerControlPlane({ platform }, [proc])

    const [status] = platform.statuses()
    expect(status!.segments).toEqual([
      { segment: 1, onePartOf: 2, position: 5n, caughtUp: true, replaying: false, error: undefined },
      { segment: 2, onePartOf: 4, position: 3n, caughtUp: false, replaying: false, error: "boom" },
    ])
    const info = toEventProcessorInfo(status!)
    expect(info.activeThreads).toBe(2)
    expect(info.segmentStatus).toEqual([
      { segmentId: 1, caughtUp: true, replaying: false, onePartOf: 2, tokenPosition: 5n, errorState: "" },
      { segmentId: 2, caughtUp: false, replaying: false, onePartOf: 4, tokenPosition: 3n, errorState: "boom" },
    ])
  })

  it("reads LIVE processor state on every report", async () => {
    const platform = fakePlatform()
    let running = false
    const proc: ManagedEventProcessor = {
      name: "p",
      get running() {
        return running
      },
    }
    await axonServerControlPlane({ platform }, [proc])

    expect(platform.statuses()[0]!.running).toBe(false)
    running = true
    expect(platform.statuses()[0]!.running).toBe(true)
  })

  it("exposes the processors by name", async () => {
    const platform = fakePlatform()
    const control = await axonServerControlPlane({ platform }, [{ name: "a" }, { name: "b" }])
    expect([...control.processors.keys()]).toEqual(["a", "b"])
  })
})

describe("axonServerControlPlane — processor source", () => {
  it("accepts a name-keyed map, which is what app.processors is", async () => {
    const platform = fakePlatform()
    const a = spyProcessor("proc-a")
    const appProcessors: ReadonlyMap<string, unknown> = new Map([["proc-a", a.proc]])
    const control = await axonServerControlPlane({ platform }, appProcessors)

    expect(platform.statuses().map((s) => s.name)).toEqual(["proc-a"])
    await platform.emit({ kind: "start-processor", processorName: "proc-a" })
    expect(a.calls).toEqual(["start"])
    expect(control.processors.get("proc-a")).toBe(a.proc)
  })

  it("ignores map entries that are not processor-shaped", async () => {
    const platform = fakePlatform()
    const appProcessors: ReadonlyMap<string, unknown> = new Map<string, unknown>([
      ["proc-a", spyProcessor("proc-a").proc],
      ["junk", 42],
      ["nullish", null],
    ])
    await axonServerControlPlane({ platform }, appProcessors)

    expect(platform.statuses().map((s) => s.name)).toEqual(["proc-a"])
  })

  it("reports and controls a processor added to the map after the control plane was created", async () => {
    const platform = fakePlatform()
    const live = new Map<string, unknown>()
    const control = await axonServerControlPlane({ platform }, live)

    expect(platform.statuses()).toEqual([])

    const late = spyProcessor("late")
    live.set("late", late.proc)

    expect(platform.statuses().map((s) => s.name)).toEqual(["late"])
    await platform.emit({ kind: "start-processor", processorName: "late" })
    expect(late.calls).toEqual(["start"])
    expect([...control.processors.keys()]).toEqual(["late"])
  })

  it("stops reporting a processor removed from the map", async () => {
    const platform = fakePlatform()
    const live = new Map<string, unknown>([["p", { name: "p" }]])
    await axonServerControlPlane({ platform }, live)

    expect(platform.statuses()).toHaveLength(1)
    live.delete("p")
    expect(platform.statuses()).toEqual([])
  })

  it("reads an array again on every report", async () => {
    const platform = fakePlatform()
    const live: ManagedEventProcessor[] = []
    await axonServerControlPlane({ platform }, live)

    expect(platform.statuses()).toEqual([])

    const late = spyProcessor("late")
    live.push(late.proc)

    expect(platform.statuses().map((s) => s.name)).toEqual(["late"])
    await platform.emit({ kind: "start-processor", processorName: "late" })
    expect(late.calls).toEqual(["start"])
  })

  it("copies a one-shot iterator source (app.processors.values()), which can be read only once", async () => {
    const platform = fakePlatform()
    const live = new Map<string, ManagedEventProcessor>([["p", { name: "p" }]])
    await axonServerControlPlane({ platform }, live.values())

    expect(platform.statuses()).toHaveLength(1)
    expect(platform.statuses()).toHaveLength(1)

    // The iterator was copied: the map it came from is not read again.
    live.set("late", { name: "late" })
    expect(platform.statuses().map((s) => s.name)).toEqual(["p"])
  })
})
