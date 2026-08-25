/**
 * CREDIT-BASED FLOW CONTROL ON A SUBSCRIPTION QUERY.
 *
 * `subscribe` grants a window of permits; the server sends at most that many
 * updates and then waits for the subscriber to top it back up. A client that
 * grants a window and never refills works until exactly `window` updates have
 * flowed and then stalls with no error anywhere — the failure this pins.
 */
import { describe, expect, it } from "bun:test"
import {
  localQueryBus,
  qn,
  unitOfWork,
  type QueryMessage,
  type SerializedObject,
  type Serializer,
} from "@kronos-ts/core"

const jsonSerializer: Serializer = {
  serialize(value, type, revision = ""): SerializedObject {
    return { type, revision, data: new TextEncoder().encode(JSON.stringify(value)) }
  },
  deserialize<T>({ data }: SerializedObject): T {
    return JSON.parse(new TextDecoder().decode(data)) as T
  },
  canConvert() { return true },
}
import { kronosDbQueryBus } from "../kronosdb.js"
import { shutdownLatch } from "../shutdown-latch.js"
import type { KronosDbConnection } from "../connection.js"

/** A stream we can push server frames into, and close. */
function controllable<T>() {
  let push!: (value: T) => void
  let close!: () => void
  const queue: T[] = []
  let waiting: ((r: IteratorResult<T>) => void) | null = null
  let done = false
  const iterable: AsyncIterable<T> = {
    [Symbol.asyncIterator]: () => ({
      next: () =>
        new Promise<IteratorResult<T>>((resolve) => {
          if (queue.length > 0) return resolve({ value: queue.shift()!, done: false })
          if (done) return resolve({ value: undefined as never, done: true })
          waiting = resolve
        }),
    }),
  }
  push = (value: T) => {
    if (waiting) { const w = waiting; waiting = null; w({ value, done: false }) }
    else queue.push(value)
  }
  close = () => {
    done = true
    if (waiting) { const w = waiting; waiting = null; w({ value: undefined as never, done: true }) }
  }
  return { iterable, push, close }
}

function fakeConnection(captured: { outbound: any[] }, server: AsyncIterable<any>): KronosDbConnection {
  const never = (async function* () { await new Promise(() => {}) })()
  return {
    channel: {} as any,
    platform: {} as any,
    eventStore: {} as any,
    commands: { openStream: () => never } as any,
    queries: {
      openStream: () => never,
      // The subscriber leg: drain what the client sends, hand back server frames.
      subscription(outbound: AsyncIterable<any>) {
        void (async () => {
          try { for await (const msg of outbound) captured.outbound.push(msg) } catch { /* closed */ }
        })()
        return server
      },
    } as any,
    config: {
      host: "localhost", port: 50051, context: "default",
      componentName: "test", clientId: "test-client", token: "",
      reconnectIntervalMs: 0, maxReconnectAttempts: 0,
      keepAliveTimeMs: 0, keepAliveTimeoutMs: 0, keepAlivePermitWithoutCalls: false,
    },
    state: "connected",
    onReconnect() {}, onDisconnect() {}, close() {}, reconnect: async () => {},
  } as unknown as KronosDbConnection
}

const message = (): QueryMessage => ({
  identifier: "q-1",
  kind: "query",
  name: qn("kronos.test", "WatchValue"),
  payload: { id: "x" },
  metadata: {},
  timestamp: 1,
})

const updateFrame = (n: number) => ({
  update: { payload: jsonSerializer.serialize(n, "kronos.test.Update", "") },
})

async function flush(ms = 10) { await new Promise((r) => setTimeout(r, ms)) }

describe("kronosDbQueryBus — subscription query flow control", () => {
  function setup(bufferSize: number) {
    const captured: { outbound: any[] } = { outbound: [] }
    const server = controllable<any>()
    let latch = shutdownLatch()
    const bus = kronosDbQueryBus(
      localQueryBus(unitOfWork),
      {
        connection: fakeConnection(captured, server.iterable),
        serializer: jsonSerializer,
        registerShutdownLatch: (l) => { latch = l },
      },
    )
    const result = bus.subscriptionQuery(message(), bufferSize)
    return { captured, server, result, stop: () => latch.initiateShutdown() }
  }

  const flowControls = (outbound: any[]) => outbound.filter((m) => m.flowControl)

  it("grants the buffer size as the initial window", async () => {
    const { captured, server, stop } = setup(8)
    await flush()
    expect(captured.outbound[0]?.subscribe?.numberOfPermits).toBe(8n)
    server.close(); stop()
  })

  it("never asks for more credit than the server will grant", async () => {
    // The server clamps the initial grant to [1, 1024]. Asking for 5000 and
    // then pacing refills against a 5000-wide window would let 1250 updates
    // pass before topping up — 226 of them beyond the credit the server
    // actually holds, and silently dropped. So the window we track is the one
    // the server gives.
    const { captured, server, result, stop } = setup(5000)
    await flush()
    expect(captured.outbound[0]?.subscribe?.numberOfPermits).toBe(1024n)

    // …and the refill cadence follows the granted window (1024 / 4 = 256),
    // not the requested one.
    for (let i = 0; i < 255; i++) server.push(updateFrame(i))
    await flush()
    expect(flowControls(captured.outbound)).toHaveLength(0)
    server.push(updateFrame(255))
    await flush()
    expect(flowControls(captured.outbound).map((m) => m.flowControl.numberOfPermits)).toEqual([256n])

    result.close(); server.close(); stop()
  })

  it("asks for at least one credit — zero is not unlimited", async () => {
    // `clamp(1, 1024)` server-side: a 0 grant becomes exactly 1, so a client
    // that meant "unlimited" would get one update and stall.
    const { captured, server, result, stop } = setup(0)
    await flush()
    expect(captured.outbound[0]?.subscribe?.numberOfPermits).toBe(256n)
    result.close(); server.close(); stop()
  })

  it("refills a quarter-window at a time as updates are consumed", async () => {
    const { captured, server, result, stop } = setup(8)   // quarter window = 2
    await flush()

    for (let i = 0; i < 2; i++) server.push(updateFrame(i))
    await flush()
    expect(flowControls(captured.outbound).map((m) => m.flowControl.numberOfPermits)).toEqual([2n])

    for (let i = 0; i < 2; i++) server.push(updateFrame(i))
    await flush()
    expect(flowControls(captured.outbound).map((m) => m.flowControl.numberOfPermits)).toEqual([2n, 2n])

    // What was granted back equals what was taken — the window never shrinks.
    const refilled = flowControls(captured.outbound).reduce((sum, m) => sum + Number(m.flowControl.numberOfPermits), 0)
    expect(refilled).toBe(4)

    result.close(); server.close(); stop()
  })

  it("does not refill before a quarter-window has accrued", async () => {
    const { captured, server, result, stop } = setup(8)
    await flush()
    server.push(updateFrame(1))
    await flush()
    expect(flowControls(captured.outbound)).toHaveLength(0)
    result.close(); server.close(); stop()
  })

  it("addresses every refill to the subscription it belongs to", async () => {
    const { captured, server, result, stop } = setup(4)   // quarter window = 1
    await flush()
    const subscriptionId = captured.outbound[0]?.subscribe?.subscriptionIdentifier
    expect(subscriptionId).toBeTruthy()

    server.push(updateFrame(1))
    await flush()
    expect(flowControls(captured.outbound)[0]?.flowControl.subscriptionIdentifier).toBe(subscriptionId)

    result.close(); server.close(); stop()
  })

  it("stops granting credit once the subscription is closed", async () => {
    const { captured, server, result, stop } = setup(4)
    await flush()
    result.close()
    const after = flowControls(captured.outbound).length
    server.push(updateFrame(1))
    server.push(updateFrame(2))
    await flush()
    expect(flowControls(captured.outbound)).toHaveLength(after)
    server.close(); stop()
  })
})
