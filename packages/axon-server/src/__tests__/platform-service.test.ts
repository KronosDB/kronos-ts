/**
 * The DATA-PATH / CONTROL-PLANE split on the Axon Server platform stream.
 *
 * The defect: `platform.start()` armed the heartbeat that drives
 * `connection.reconnect()` on timeout, and `platform.start()` only ever runs if
 * the caller builds `axonServerControlPlane(...)`. After the control-plane
 * extraction, a service that never opted into remote administration therefore
 * had NO reconnect detection on its data path — the buses hook
 * `connection.onReconnect(...)`, but nothing was ever going to fire it.
 *
 * The split: `armConnectionMonitoring()` is the data path's half (stream +
 * heartbeat, no processor status reporting), `start()` is the control plane's
 * (everything, plus status reporting). Both are idempotent so either can run
 * first.
 */
import { afterEach, describe, expect, it } from "bun:test"
import { platformConnection, type PlatformConnection } from "../platform-service.js"
import type { AxonServerConnection } from "../connection.js"

function controllableInbound() {
  let push: ((msg: any) => void) | null = null
  let close: (() => void) | null = null
  const iterable: AsyncIterable<any> = {
    [Symbol.asyncIterator]() {
      const queue: any[] = []
      let pending: ((r: IteratorResult<any>) => void) | null = null
      let done = false
      push = (msg) => {
        if (pending) {
          const r = pending
          pending = null
          r({ value: msg, done: false })
        } else queue.push(msg)
      }
      close = () => {
        done = true
        if (pending) {
          const r = pending
          pending = null
          r({ value: undefined, done: true })
        }
      }
      return {
        next(): Promise<IteratorResult<any>> {
          if (queue.length) return Promise.resolve({ value: queue.shift(), done: false })
          if (done) return Promise.resolve({ value: undefined, done: true })
          return new Promise((r) => {
            pending = r
          })
        },
      }
    },
  }
  return {
    iterable,
    push(msg: any) {
      push?.(msg)
    },
    close() {
      close?.()
    },
  }
}

function fakeConnection() {
  const outboundFrames: any[] = []
  const inbound = controllableInbound()
  let openStreamCalls = 0
  let reconnects = 0

  const connection = {
    platform: {
      openStream(outboundIterable: AsyncIterable<any>) {
        openStreamCalls++
        void (async () => {
          try {
            for await (const frame of outboundIterable) outboundFrames.push(frame)
          } catch {
            /* closed */
          }
        })()
        return inbound.iterable
      },
    } as any,
    commands: {} as any,
    queries: {} as any,
    events: {} as any,
    channel: {} as any,
    config: {
      host: "localhost",
      port: 8124,
      context: "default",
      componentName: "platform-test",
      clientId: "platform-client",
      token: "",
    },
    state: "connected",
    onReconnect() {},
    onDisconnect() {},
    close() {},
    async reconnect() {
      reconnects++
    },
  } as unknown as AxonServerConnection

  return {
    connection,
    inbound,
    outboundFrames,
    get openStreamCalls() {
      return openStreamCalls
    },
    get reconnects() {
      return reconnects
    },
  }
}

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms))

/**
 * Fast timers so the heartbeat/status behaviour is observable in a unit test.
 *
 * `heartbeatTimeoutMs` is deliberately LONG here: nothing answers the fake
 * inbound stream, so a short timeout would mark the connection lost mid-test and
 * silence status reporting — which would make the "arms NO status reporting"
 * assertion pass for entirely the wrong reason.
 */
const fastOptions = {
  heartbeatIntervalMs: 5,
  heartbeatTimeoutMs: 10_000,
  processorsNotificationInitialDelayMs: 5,
  processorsNotificationRateMs: 5,
}

/** Same, but with a heartbeat window short enough to lapse during the test. */
const lapsingOptions = { ...fastOptions, heartbeatTimeoutMs: 10 }

let platform: PlatformConnection | undefined

afterEach(() => {
  platform?.stop()
  platform = undefined
})

describe("platformConnection — armConnectionMonitoring (data path)", () => {
  it("opens the stream, registers, and sends heartbeats without any control plane", async () => {
    const fake = fakeConnection()
    platform = platformConnection(fake.connection, fastOptions)

    await platform.armConnectionMonitoring()

    expect(platform.connected).toBe(true)
    expect(fake.openStreamCalls).toBe(1)
    expect(fake.outboundFrames.some((f) => f.register)).toBe(true)

    await wait(20)
    expect(fake.outboundFrames.some((f) => f.heartbeat)).toBe(true)
  })

  it("drives connection.reconnect() on heartbeat timeout — the defect", async () => {
    const fake = fakeConnection()
    platform = platformConnection(fake.connection, lapsingOptions)

    // No axonServerControlPlane anywhere: this is a service nobody administers.
    await platform.armConnectionMonitoring()

    // Nothing answers on the inbound stream, so the heartbeat window lapses.
    await wait(60)

    expect(fake.reconnects).toBeGreaterThan(0)
    expect(platform.connected).toBe(false)
  })

  it("arms NO processor status reporting", async () => {
    const fake = fakeConnection()
    platform = platformConnection(fake.connection, fastOptions)
    platform.registerProcessorStatusSupplier(() => [
      {
        name: "p",
        running: true,
        mode: "Tracking",
        isStreamingProcessor: true,
        activeThreads: 1,
        availableThreads: 0,
        error: false,
        tokenStoreIdentifier: "",
        segments: [],
      },
    ])

    await platform.armConnectionMonitoring()
    await wait(40)

    expect(fake.outboundFrames.some((f) => f.eventProcessorInfo)).toBe(false)
  })
})

describe("platformConnection — start (control plane) over an already-armed stream", () => {
  it("adds status reporting without re-registering on a second stream", async () => {
    const fake = fakeConnection()
    platform = platformConnection(fake.connection, fastOptions)

    await platform.armConnectionMonitoring()
    const registersAfterArming = fake.outboundFrames.filter((f) => f.register).length

    platform.registerProcessorStatusSupplier(() => [
      {
        name: "p",
        running: true,
        mode: "Tracking",
        isStreamingProcessor: true,
        activeThreads: 1,
        availableThreads: 0,
        error: false,
        tokenStoreIdentifier: "",
        segments: [],
      },
    ])
    await platform.start()
    await wait(30)

    // One stream, one registration — start() found it already up.
    expect(fake.openStreamCalls).toBe(1)
    expect(fake.outboundFrames.filter((f) => f.register).length).toBe(registersAfterArming)
    // …and status reporting is now running on it.
    expect(fake.outboundFrames.some((f) => f.eventProcessorInfo)).toBe(true)
  })

  it("is idempotent — a second start() does not double the status reports", async () => {
    const fake = fakeConnection()
    platform = platformConnection(fake.connection, fastOptions)
    platform.registerProcessorStatusSupplier(() => [
      {
        name: "p",
        running: true,
        mode: "Tracking",
        isStreamingProcessor: true,
        activeThreads: 1,
        availableThreads: 0,
        error: false,
        tokenStoreIdentifier: "",
        segments: [],
      },
    ])

    await platform.start()
    await platform.start()
    await platform.start()
    await wait(40)
    const reports = fake.outboundFrames.filter((f) => f.eventProcessorInfo).length

    platform.stop()
    await wait(20)
    // Every timer really stopped: nothing kept reporting after stop().
    expect(fake.outboundFrames.filter((f) => f.eventProcessorInfo).length).toBe(reports)

    // Three start() calls did not install three reporters. Allow generous slack
    // for timer jitter, but 3x would blow well past this.
    const singleReporterCeiling = 40 / fastOptions.processorsNotificationRateMs + 4
    expect(reports).toBeLessThanOrEqual(singleReporterCeiling)
  })

  it("works when start() runs FIRST and the data path arms afterwards", async () => {
    const fake = fakeConnection()
    platform = platformConnection(fake.connection, fastOptions)

    await platform.start()
    await platform.armConnectionMonitoring()

    expect(fake.openStreamCalls).toBe(1)
    expect(platform.connected).toBe(true)
  })
})

describe("platformConnection — instruction buffering", () => {
  it("holds instructions that arrive before a handler is registered and drains them in order", async () => {
    const fake = fakeConnection()
    platform = platformConnection(fake.connection, fastOptions)

    // Data path opens the stream first; no control plane exists yet.
    await platform.armConnectionMonitoring()

    fake.inbound.push({ eventProcessorControl: { processorName: "a", pauseEventProcessor: {} } })
    fake.inbound.push({ eventProcessorControl: { processorName: "b", startEventProcessor: {} } })
    await wait(10)

    const seen: string[] = []
    platform.onInstruction((instruction) => {
      if ("processorName" in instruction) seen.push(`${instruction.kind}:${instruction.processorName}`)
    })
    await wait(10)

    expect(seen).toEqual(["pause-processor:a", "start-processor:b"])
  })

  it("does not replay a stopped stream's backlog", async () => {
    const fake = fakeConnection()
    platform = platformConnection(fake.connection, fastOptions)
    await platform.armConnectionMonitoring()

    fake.inbound.push({ eventProcessorControl: { processorName: "a", pauseEventProcessor: {} } })
    await wait(10)
    platform.stop()

    const seen: string[] = []
    platform.onInstruction((instruction) => {
      seen.push(instruction.kind)
    })
    await wait(10)

    expect(seen).toEqual([])
  })
})
