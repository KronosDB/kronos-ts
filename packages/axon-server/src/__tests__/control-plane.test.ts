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
 *   4. a one-shot iterator (`Map.values()`) still reports on every tick
 */
import { describe, it, expect } from "bun:test"
import type {
  PlatformConnection,
  InstructionHandler,
  PlatformInstruction,
} from "../platform-service.js"
import type { ProcessorStatusSupplier } from "../event-processor-info.js"
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
    async subscriptionsAcked() {
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
    releaseSegment: (id: number) => void calls.push(`release:${id}`),
    splitSegment: (id: number) => void calls.push(`split:${id}`),
    mergeSegment: (id: number) => void calls.push(`merge:${id}`),
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
    await platform.emit({ kind: "release-segment", processorName: "proc-a", segmentId: 3 })
    await platform.emit({ kind: "split-segment", processorName: "proc-a", segmentId: 4 })
    await platform.emit({ kind: "merge-segment", processorName: "proc-a", segmentId: 5 })

    expect(a.calls).toEqual(["stop", "start", "release:3", "split:4", "merge:5"])
    expect(b.calls).toEqual([])
  })

  it("ignores instructions for unknown processor names", async () => {
    const platform = fakePlatform()
    const a = spyProcessor("proc-a")
    await axonServerControlPlane({ platform }, [a.proc])

    await platform.emit({ kind: "pause-processor", processorName: "nope" })
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
  it("reports a synthetic single segment for a processor with no per-segment status", async () => {
    const platform = fakePlatform()
    const proc: ManagedEventProcessor = { name: "p", running: true, position: 17n }
    await axonServerControlPlane({ platform }, [proc])

    const [status] = platform.statuses()
    expect(status!.name).toBe("p")
    expect(status!.running).toBe(true)
    expect(status!.mode).toBe("Tracking")
    expect(status!.isStreamingProcessor).toBe(true)
    expect(status!.activeThreads).toBe(1)
    expect(status!.segments).toEqual([
      {
        segmentId: 0,
        caughtUp: true,
        replaying: false,
        onePartOf: 1,
        tokenPosition: 17n,
        errorState: "",
      },
    ])
  })

  it("maps per-segment processing status when the processor exposes it", async () => {
    const platform = fakePlatform()
    const proc: ManagedEventProcessor = {
      name: "p",
      running: true,
      processingStatus: () =>
        new Map([
          [0, { position: 5n, caughtUp: true, replaying: false }],
          [1, { position: 9n, caughtUp: false, replaying: true, error: new Error("boom") }],
        ]),
    }
    await axonServerControlPlane({ platform }, [proc])

    const [status] = platform.statuses()
    expect(status!.segments).toEqual([
      {
        segmentId: 0,
        caughtUp: true,
        replaying: false,
        onePartOf: 1,
        tokenPosition: 5n,
        errorState: "",
      },
      {
        segmentId: 1,
        caughtUp: false,
        replaying: true,
        onePartOf: 1,
        tokenPosition: 9n,
        errorState: "boom",
      },
    ])
  })

  it("reports a non-resettable processor as Subscribing", async () => {
    const platform = fakePlatform()
    const proc: ManagedEventProcessor = { name: "sub", supportsReset: () => false }
    await axonServerControlPlane({ platform }, [proc])

    const [status] = platform.statuses()
    expect(status!.mode).toBe("Subscribing")
    expect(status!.isStreamingProcessor).toBe(false)
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

  it("survives a one-shot iterator source (app.processors.values())", async () => {
    const platform = fakePlatform()
    const live = new Map<string, ManagedEventProcessor>([["p", { name: "p" }]])
    // Map.values() is single-use — the control plane must snapshot it, or the
    // second status tick would report no processors at all.
    await axonServerControlPlane({ platform }, live.values())

    expect(platform.statuses()).toHaveLength(1)
    expect(platform.statuses()).toHaveLength(1)
  })

  it("exposes the snapshotted processors by name", async () => {
    const platform = fakePlatform()
    const control = await axonServerControlPlane({ platform }, [{ name: "a" }, { name: "b" }])
    expect([...control.processors.keys()]).toEqual(["a", "b"])
  })
})
