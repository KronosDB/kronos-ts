import { afterEach, describe, expect, it } from "bun:test"
import { platformConnection, parseInstruction, type PlatformConnection, type PlatformInstruction } from "../platform-service.js"
import { ProcessorAction, type Frame } from "../protocol/kronosdb-protocol.js"
import { until, rig, type Rig } from "./bus-test-kit.js"

describe("parseInstruction", () => {
  const frame = (action: number, processorName = "p", segment = 0) => ({ action, processorName, segment })

  it("decodes pause and start by processor name", () => {
    expect(parseInstruction(frame(ProcessorAction.pause, "proc-x"))).toEqual({ kind: "pause-processor", processorName: "proc-x" })
    expect(parseInstruction(frame(ProcessorAction.start, "proc-y"))).toEqual({ kind: "start-processor", processorName: "proc-y" })
  })

  it("decodes the segment actions with their segment", () => {
    expect(parseInstruction(frame(ProcessorAction.release_segment, "p", 7))).toEqual({ kind: "release-segment", processorName: "p", segmentId: 7 })
    expect(parseInstruction(frame(ProcessorAction.split_segment, "p", 8))).toEqual({ kind: "split-segment", processorName: "p", segmentId: 8 })
    expect(parseInstruction(frame(ProcessorAction.merge_segment, "p", 9))).toEqual({ kind: "merge-segment", processorName: "p", segmentId: 9 })
  })

  it("has nothing for report_status (the connection answers it) or an action it does not know", () => {
    expect(parseInstruction(frame(ProcessorAction.report_status))).toBeNull()
    expect(parseInstruction(frame(99))).toBeNull()
  })
})

describe("the platform stream", () => {
  let r: Rig
  let platform: PlatformConnection | undefined
  afterEach(async () => {
    platform?.stop()
    platform = undefined
    await r?.close()
  })

  const open = async (options?: Parameters<typeof platformConnection>[1], config?: Parameters<typeof rig>[0]) => {
    r = await rig(config)
    platform = platformConnection(r.client.connection, { processorsNotificationInitialDelayMs: 60_000, ...options })
    return platform
  }
  const results = () =>
    (r.server.platforms[0]?.connection.received ?? [])
      .filter((f) => f.stream === r.server.platforms[0]!.stream && f.frame.name === "InstructionResult")
      .map((f) => (f.frame as Extract<Frame, { name: "InstructionResult" }>).body)
  const statuses = () =>
    (r.server.platforms[0]?.connection.received ?? []).flatMap((f) => (f.frame.name === "ProcessorStatus" ? [f.frame.body] : []))
  const instruct = (action: number, instructionId: string, processorName = "orders", segment = 0) =>
    r.server.toPlatform(0, { name: "ProcessorInstruction", body: { instructionId, action, processorName, segment } })

  it("registers this client, and is acknowledged once the server's NodeInfo arrives", async () => {
    const p = await open()
    expect(await p.subscriptionsAcked()).toBe(false)
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    expect(r.server.platforms[0]!.clientId).toBe(r.client.connection.config.clientId)
    let acked = false
    for (let i = 0; i < 100 && !acked; i++) {
      acked = await p.subscriptionsAcked()
      if (!acked) await new Promise((resolve) => setTimeout(resolve, 5))
    }
    expect(acked).toBe(true)
    expect(p.connected).toBe(true)
  })

  it("answers the server's heartbeat at once", async () => {
    const p = await open({ heartbeatIntervalMs: 60_000 })
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    r.server.toPlatform(0, { name: "ServerHeartbeat", body: {} })
    const heartbeats = () => r.server.platforms[0]!.connection.received.filter((f) => f.frame.name === "PlatformHeartbeat")
    await until(() => heartbeats().length === 1, "the answer")
  })

  it("sends its own heartbeats too", async () => {
    const p = await open({ heartbeatIntervalMs: 10 })
    await p.start()
    await until(() => r.server.platforms[0]?.connection.received.filter((f) => f.frame.name === "PlatformHeartbeat").length! >= 2, "two heartbeats")
  })

  it("runs a pause instruction by name and reports that it went well", async () => {
    const p = await open()
    const seen: PlatformInstruction[] = []
    p.onInstruction((instruction) => void seen.push(instruction))
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    instruct(ProcessorAction.pause, "i-1")
    await until(() => results().length === 1, "the result")
    expect(seen).toEqual([{ kind: "pause-processor", processorName: "orders" }])
    expect(results()[0]).toEqual({ instructionId: "i-1", success: true, error: undefined })
  })

  it("reports a failing handler as a failed instruction, and carries on", async () => {
    const p = await open()
    p.onInstruction((instruction) => {
      if (instruction.kind === "pause-processor") throw new Error("cannot pause now")
    })
    const originalError = console.error
    console.error = () => {}
    try {
      await p.start()
      await until(() => r.server.platforms.length === 1, "the platform stream")
      instruct(ProcessorAction.pause, "i-1")
      instruct(ProcessorAction.start, "i-2")
      await until(() => results().length === 2, "both results")
    } finally {
      console.error = originalError
    }
    expect(results()[0]).toMatchObject({ instructionId: "i-1", success: false, error: { message: "cannot pause now" } })
    expect(results()[1]).toMatchObject({ instructionId: "i-2", success: true })
  })

  it("runs instructions one after the other, in the order the server sent them", async () => {
    const p = await open()
    const order: string[] = []
    p.onInstruction(async (instruction) => {
      const name = instruction.kind === "pause-processor" ? "pause" : "start"
      order.push(`${name}:begin`)
      await new Promise((resolve) => setTimeout(resolve, name === "pause" ? 30 : 0))
      order.push(`${name}:end`)
    })
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    instruct(ProcessorAction.pause, "i-1")
    instruct(ProcessorAction.start, "i-2")
    await until(() => results().length === 2, "both results")
    expect(order).toEqual(["pause:begin", "pause:end", "start:begin", "start:end"])
    expect(results().map((x) => x.instructionId)).toEqual(["i-1", "i-2"])
  })

  it("reports a result on the stream the instruction came on, not on a replacement", async () => {
    const p = await open()
    const release = Promise.withResolvers<void>()
    p.onInstruction(() => release.promise)
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    instruct(ProcessorAction.pause, "slow-1")
    await new Promise((resolve) => setTimeout(resolve, 20))
    // The connection is reset while the handler is still working; the stream is opened again.
    await r.client.connection.reconnect()
    await until(() => r.server.platforms.length === 2, "the replacement stream")
    release.resolve()
    await new Promise((resolve) => setTimeout(resolve, 30))
    const onReplacement = r.server.platforms[1]!.connection.received.filter((f) => f.stream === r.server.platforms[1]!.stream && f.frame.name === "InstructionResult")
    expect(onReplacement).toHaveLength(0)
  })

  it("holds an instruction that arrives before any handler, and runs it when one registers", async () => {
    const p = await open()
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    instruct(ProcessorAction.pause, "early-1")
    instruct(ProcessorAction.start, "early-2")
    await new Promise((resolve) => setTimeout(resolve, 30))
    expect(results()).toHaveLength(0)
    const seen: string[] = []
    p.onInstruction((instruction) => void seen.push(instruction.kind))
    await until(() => results().length === 2, "both results")
    expect(seen).toEqual(["pause-processor", "start-processor"])
  })

  it("refuses an action it does not know, saying so", async () => {
    const p = await open()
    p.onInstruction(() => {})
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    instruct(99, "odd")
    await until(() => results().length === 1, "the refusal")
    expect(results()[0]).toMatchObject({ instructionId: "odd", success: false, error: { message: expect.stringContaining("99") } })
  })

  it("reports processor status when asked to, and on a timer", async () => {
    const p = await open({ processorsNotificationInitialDelayMs: 10, processorsNotificationRateMs: 1000 })
    p.registerProcessorStatusSupplier(() => [
      { name: "OrderProjection", running: true, caughtUp: true, replaying: false, position: 42n },
      { name: "Broken", running: false, caughtUp: false, replaying: true, position: 7n, error: "boom" },
    ])
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    await until(() => statuses().length === 2, "the first report")
    expect(statuses()[0]).toEqual({
      processorName: "OrderProjection",
      mode: "Tracking",
      activeThreads: 1,
      availableThreads: 0,
      running: true,
      error: false,
      streaming: true,
      tokenStoreIdentifier: "",
      segments: [{ segmentId: 0, caughtUp: true, replaying: false, onePartOf: 1, tokenPosition: 42n, errorState: "" }],
    })
    expect(statuses()[1]).toMatchObject({ processorName: "Broken", error: true, running: false, segments: [{ errorState: "boom", tokenPosition: 7n, replaying: true }] })

    instruct(ProcessorAction.report_status, "report-1")
    await until(() => statuses().length === 4 && results().length === 1, "the asked-for report")
    expect(results()[0]).toEqual({ instructionId: "report-1", success: true, error: undefined })
  })

  it("keeps reporting after a supplier throws", async () => {
    const p = await open({ processorsNotificationInitialDelayMs: 10, processorsNotificationRateMs: 20 })
    const warn = console.warn
    console.warn = () => {}
    try {
      p.registerProcessorStatusSupplier(() => {
        throw new Error("supplier broke")
      })
      p.registerProcessorStatusSupplier(() => [{ name: "Fine", running: true, caughtUp: true, replaying: false, position: 1n }])
      await p.start()
      await until(() => statuses().length >= 2, "reports from the working supplier")
    } finally {
      console.warn = warn
    }
  })

  it("asks the connection to move when the server asks it to reconnect, and opens the stream again", async () => {
    const p = await open()
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    let reconnected = 0
    r.client.connection.onReconnect(() => void reconnected++)
    r.server.toPlatform(0, { name: "RequestReconnect", body: {} })
    await until(() => reconnected === 1 && r.server.platforms.length === 2, "the reconnect")
    expect(p.connected).toBe(true)
  })

  it("recovers when the server ends the stream, and stops recovering once stopped", async () => {
    const p = await open()
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    let reconnected = 0
    r.client.connection.onReconnect(() => void reconnected++)

    const first = r.server.platforms[0]!
    const error = console.error
    console.error = () => {}
    first.connection.reply(first.stream, 1, { name: "RequestFailed", body: { code: 1, message: "replaced" } })
    await until(() => reconnected === 1 && r.server.platforms.length === 2, "recovery")
    console.error = error
    expect(p.connected).toBe(true)

    p.stop()
    const second = r.server.platforms[1]!
    second.connection.reply(second.stream, 1, { name: "RequestFailed", body: { code: 1, message: "gone" } })
    await new Promise((resolve) => setTimeout(resolve, 40))
    expect(reconnected).toBe(1)
    expect(p.connected).toBe(false)
  })

  it("treats a server that goes silent as lost, ends the socket and reconnects", async () => {
    const p = await open({ heartbeatIntervalMs: 10, heartbeatTimeoutMs: 40 })
    const warn = console.warn
    console.warn = () => {}
    try {
      await p.start()
      await until(() => r.server.platforms.length === 1, "the platform stream")
      let reconnected = 0
      r.client.connection.onReconnect(() => void reconnected++)
      await until(() => reconnected >= 1 && r.server.platforms.length >= 2, "reconnect after silence")
      expect(r.server.connections.length).toBeGreaterThanOrEqual(2)
    } finally {
      console.warn = warn
    }
  })

  it("cancels its stream when stopped", async () => {
    const p = await open()
    await p.start()
    await until(() => r.server.platforms.length === 1, "the platform stream")
    p.stop()
    await until(() => r.server.platforms[0]!.connection.received.some((f) => f.frame.name === "Cancel" && f.stream === r.server.platforms[0]!.stream), "the Cancel")
  })
})
