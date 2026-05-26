import { afterEach, describe, expect, it } from "bun:test"
import { type Serializer, type SerializedObject } from "@kronos-ts/common"
import { payloadEquals } from "@kronos-ts/messaging"
import { createDistributedQueryBus } from "../axon-server.js"
import { createShutdownLatch } from "../shutdown-latch.js"
import type { AxonServerConnection } from "../connection.js"

const jsonSerializer: Serializer = {
  serialize(value, type, revision = ""): SerializedObject {
    return { type, revision, data: new TextEncoder().encode(JSON.stringify(value)) }
  },
  deserialize<T>({ data }: SerializedObject): T {
    return JSON.parse(new TextDecoder().decode(data)) as T
  },
  canConvert() { return true },
}

function controllableInbound() {
  let push: ((msg: any) => void) | null = null
  let close: (() => void) | null = null
  const iterable: AsyncIterable<any> = {
    [Symbol.asyncIterator]() {
      const queue: any[] = []
      let pending: ((r: IteratorResult<any>) => void) | null = null
      let done = false
      push = (msg) => {
        if (pending) { const r = pending; pending = null; r({ value: msg, done: false }) }
        else queue.push(msg)
      }
      close = () => {
        done = true
        if (pending) { const r = pending; pending = null; r({ value: undefined, done: true }) }
      }
      return {
        next(): Promise<IteratorResult<any>> {
          if (queue.length) return Promise.resolve({ value: queue.shift(), done: false })
          if (done) return Promise.resolve({ value: undefined, done: true })
          return new Promise((r) => { pending = r })
        },
      }
    },
  }
  return {
    iterable,
    push(msg: any) { push!(msg) },
    close() { close!() },
  }
}

function fakeConnection(
  captured: { outbound: any[]; outboundIter?: AsyncIterable<any> },
  inbound: AsyncIterable<any>,
): AxonServerConnection {
  return {
    channel: {} as any,
    platform: {} as any,
    commands: {} as any,
    eventStore: {} as any,
    snapshotStore: {} as any,
    queries: {
      openStream(outboundIter: AsyncIterable<any>) {
        captured.outboundIter = outboundIter
        ;(async () => {
          try {
            for await (const msg of outboundIter) {
              captured.outbound.push(msg)
            }
          } catch { /* stream closed */ }
        })()
        return inbound
      },
    } as any,
    config: {
      host: "localhost",
      port: 8124,
      context: "default",
      componentName: "test-component",
      clientId: "test-client",
      token: "",
      reconnectIntervalMs: 0,
      maxReconnectAttempts: 0,
      keepAliveTimeMs: 0,
      keepAliveTimeoutMs: 0,
      keepAlivePermitWithoutCalls: false,
    },
    state: "connected",
    onReconnect() {},
    onDisconnect() {},
    close() {},
    reconnect: async () => {},
  }
}

function makeSerializedQueryPayload(payload: unknown) {
  return jsonSerializer.serialize(payload, "kronos.test.WatchValue", "")
}

async function flush(ms = 10) { await new Promise((r) => setTimeout(r, ms)) }

describe("axon-server distributed query bus — subscription queries", () => {
  let inbound: ReturnType<typeof controllableInbound>
  let captured: { outbound: any[]; outboundIter?: AsyncIterable<any> }
  let latch: ReturnType<typeof createShutdownLatch>

  afterEach(() => {
    inbound?.close()
    latch?.initiateShutdown()
  })

  function setupBus(opts?: { handlerResult?: unknown; handlerError?: Error }) {
    inbound = controllableInbound()
    captured = { outbound: [] }
    latch = createShutdownLatch()

    const bus = createDistributedQueryBus(
      fakeConnection(captured, inbound.iterable),
      async (_metadata, run) => run(),
      latch,
      jsonSerializer,
    )

    bus.subscribe("kronos.test.WatchValue", async () => {
      if (opts?.handlerError) throw opts.handlerError
      return opts?.handlerResult ?? "initial-result"
    })

    return bus
  }

  it("handles inbound subscribe — sends initialResult back over the outbound stream", async () => {
    setupBus({ handlerResult: "hello" })
    await flush()

    inbound.push({
      subscriptionQueryRequest: {
        subscribe: {
          subscriptionIdentifier: "sub-1",
          numberOfPermits: 256n,
          queryRequest: {
            messageIdentifier: "msg-1",
            query: "kronos.test.WatchValue",
            timestamp: 0n,
            payload: makeSerializedQueryPayload({ id: "x" }),
            metaData: {},
            processingInstructions: [],
            clientId: "subscriber",
            componentName: "subscriber-comp",
          },
        },
      },
      instructionId: "",
    })

    await flush(30)

    const response = captured.outbound.find((o) => o.subscriptionQueryResponse?.initialResult)
    expect(response).toBeDefined()
    expect(response.subscriptionQueryResponse.subscriptionIdentifier).toBe("sub-1")
    expect(response.subscriptionQueryResponse.initialResult.errorCode).toBe("")
    expect(jsonSerializer.deserialize(response.subscriptionQueryResponse.initialResult.payload)).toBe("hello")
  })

  it("emitUpdate dispatches a SubscriptionQueryResponse.update to each matching tracked subscriber", async () => {
    const bus = setupBus()
    await flush()

    inbound.push({
      subscriptionQueryRequest: {
        subscribe: {
          subscriptionIdentifier: "sub-x",
          numberOfPermits: 256n,
          queryRequest: {
            messageIdentifier: "msg-x",
            query: "kronos.test.WatchValue",
            timestamp: 0n,
            payload: makeSerializedQueryPayload({ id: "x" }),
            metaData: {},
            processingInstructions: [],
            clientId: "s1",
            componentName: "s1c",
          },
        },
      },
      instructionId: "",
    })
    inbound.push({
      subscriptionQueryRequest: {
        subscribe: {
          subscriptionIdentifier: "sub-y",
          numberOfPermits: 256n,
          queryRequest: {
            messageIdentifier: "msg-y",
            query: "kronos.test.WatchValue",
            timestamp: 0n,
            payload: makeSerializedQueryPayload({ id: "y" }),
            metaData: {},
            processingInstructions: [],
            clientId: "s2",
            componentName: "s2c",
          },
        },
      },
      instructionId: "",
    })

    await flush(30)
    captured.outbound.length = 0

    await bus.emitUpdate("kronos.test.WatchValue", payloadEquals({ id: "x" }), "value-v1")
    await flush()

    const updates = captured.outbound.filter((o) => o.subscriptionQueryResponse?.update)
    expect(updates).toHaveLength(1)
    expect(updates[0].subscriptionQueryResponse.subscriptionIdentifier).toBe("sub-x")
    expect(jsonSerializer.deserialize(updates[0].subscriptionQueryResponse.update.payload)).toBe("value-v1")
  })

  it("emitUpdate with function filter is evaluated locally against tracked subscriber payloads", async () => {
    const bus = setupBus()
    await flush()

    for (const [id, payload] of [["sub-a", { value: 1 }], ["sub-b", { value: 2 }], ["sub-c", { value: 3 }]] as const) {
      inbound.push({
        subscriptionQueryRequest: {
          subscribe: {
            subscriptionIdentifier: id,
            numberOfPermits: 256n,
            queryRequest: {
              messageIdentifier: id,
              query: "kronos.test.WatchValue",
              timestamp: 0n,
              payload: makeSerializedQueryPayload(payload),
              metaData: {},
              processingInstructions: [],
              clientId: "sub",
              componentName: "sub",
            },
          },
        },
        instructionId: "",
      })
    }
    await flush(30)
    captured.outbound.length = 0

    await bus.emitUpdate(
      "kronos.test.WatchValue",
      (p) => (p as { value: number }).value > 1,
      "filtered",
    )
    await flush()

    const updates = captured.outbound
      .filter((o) => o.subscriptionQueryResponse?.update)
      .map((o) => o.subscriptionQueryResponse.subscriptionIdentifier)
      .sort()
    expect(updates).toEqual(["sub-b", "sub-c"])
  })

  it("unsubscribe removes the tracked subscriber so subsequent emits skip it", async () => {
    const bus = setupBus()
    await flush()

    inbound.push({
      subscriptionQueryRequest: {
        subscribe: {
          subscriptionIdentifier: "sub-1",
          numberOfPermits: 256n,
          queryRequest: {
            messageIdentifier: "msg-1",
            query: "kronos.test.WatchValue",
            timestamp: 0n,
            payload: makeSerializedQueryPayload({ id: "x" }),
            metaData: {},
            processingInstructions: [],
            clientId: "s",
            componentName: "sc",
          },
        },
      },
      instructionId: "",
    })
    await flush(30)
    inbound.push({
      subscriptionQueryRequest: {
        unsubscribe: { subscriptionIdentifier: "sub-1" },
      },
      instructionId: "",
    })
    await flush(30)
    captured.outbound.length = 0

    await bus.emitUpdate("kronos.test.WatchValue", payloadEquals({ id: "x" }), "should-not-fire")
    await flush()

    const updates = captured.outbound.filter((o) => o.subscriptionQueryResponse?.update)
    expect(updates).toHaveLength(0)
  })

  it("completeSubscription dispatches a complete response and stops further emits", async () => {
    const bus = setupBus()
    await flush()

    inbound.push({
      subscriptionQueryRequest: {
        subscribe: {
          subscriptionIdentifier: "sub-1",
          numberOfPermits: 256n,
          queryRequest: {
            messageIdentifier: "msg-1",
            query: "kronos.test.WatchValue",
            timestamp: 0n,
            payload: makeSerializedQueryPayload({ id: "x" }),
            metaData: {},
            processingInstructions: [],
            clientId: "s",
            componentName: "sc",
          },
        },
      },
      instructionId: "",
    })
    await flush(30)
    captured.outbound.length = 0

    await bus.completeSubscription("kronos.test.WatchValue")
    await flush()

    const completes = captured.outbound.filter((o) => o.subscriptionQueryResponse?.complete)
    expect(completes).toHaveLength(1)
    expect(completes[0].subscriptionQueryResponse.subscriptionIdentifier).toBe("sub-1")

    captured.outbound.length = 0
    await bus.emitUpdate("kronos.test.WatchValue", payloadEquals({ id: "x" }), "should-not-fire")
    await flush()
    expect(captured.outbound.filter((o) => o.subscriptionQueryResponse?.update)).toHaveLength(0)
  })

  it("completeSubscriptionExceptionally dispatches a completeExceptionally response", async () => {
    const bus = setupBus()
    await flush()

    inbound.push({
      subscriptionQueryRequest: {
        subscribe: {
          subscriptionIdentifier: "sub-err",
          numberOfPermits: 256n,
          queryRequest: {
            messageIdentifier: "msg-err",
            query: "kronos.test.WatchValue",
            timestamp: 0n,
            payload: makeSerializedQueryPayload({ id: "x" }),
            metaData: {},
            processingInstructions: [],
            clientId: "s",
            componentName: "sc",
          },
        },
      },
      instructionId: "",
    })
    await flush(30)
    captured.outbound.length = 0

    await bus.completeSubscriptionExceptionally("kronos.test.WatchValue", new Error("boom"))
    await flush()

    const ex = captured.outbound.filter((o) => o.subscriptionQueryResponse?.completeExceptionally)
    expect(ex).toHaveLength(1)
    expect(ex[0].subscriptionQueryResponse.completeExceptionally.errorMessage.message).toBe("boom")
  })
})
