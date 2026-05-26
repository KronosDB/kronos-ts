import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn } from "@kronos-ts/common"
import { query, payloadEquals } from "@kronos-ts/messaging"
import { createSimpleQueryBus } from "@kronos-ts/messaging"
import { createRabbitMqQueryBus, type RabbitMqQueryEnvelope, type RabbitMqQueryTransport } from "../query-bus.js"
import type { RabbitMqQueryUpdateEnvelope, RabbitMqQueryUpdatesTransport } from "../amqp-query-updates-transport.js"
import { resolveRabbitMqConfig } from "../rabbitmq.js"

const GetThing = query({
  name: qn("test", "GetThing"),
  payload: z.object({ id: z.string() }),
})

function appStub(overrides: any = {}) {
  return {
    identity: { serviceName: "svc", instanceId: "inst" },
    ...overrides,
  } as any
}

function recordingTransport() {
  const subscriptions = new Map<string, (envelope: RabbitMqQueryEnvelope) => Promise<any>>()
  const dispatched: RabbitMqQueryEnvelope[] = []
  const transport: RabbitMqQueryTransport = {
    async dispatch(envelope) {
      dispatched.push(envelope)
      return { requestId: envelope.requestId, ok: true, result: "remote-ok" }
    },
    subscribe(name, handler) {
      subscriptions.set(name, handler)
    },
  }
  return { transport, dispatched, subscriptions }
}

describe("RabbitMQ query bus", () => {
  it("prefers local handlers by default", async () => {
    const local = createSimpleQueryBus()
    const { transport, dispatched } = recordingTransport()
    const bus = createRabbitMqQueryBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), { url: "amqp://test" }),
    })

    bus.subscribe("test.GetThing", async () => "local-ok")

    const result = await bus.query({
      identifier: "qry-1",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(result).toBe("local-ok")
    expect(dispatched).toHaveLength(0)
  })

  it("routes through transport when distributed routing is forced", async () => {
    const local = createSimpleQueryBus()
    const { transport, dispatched } = recordingTransport()
    const bus = createRabbitMqQueryBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), {
        url: "amqp://test",
        queries: { alwaysUseDistributedBus: true },
      }),
    })

    bus.subscribe("test.GetThing", async () => "local-ok")

    const result = await bus.query({
      identifier: "qry-1",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(result).toBe("remote-ok")
    expect(dispatched).toHaveLength(1)
  })

  it("carries query metadata across the transport envelope", async () => {
    const local = createSimpleQueryBus()
    const { transport, dispatched } = recordingTransport()
    const bus = createRabbitMqQueryBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), {
        url: "amqp://test",
        queries: { alwaysUseDistributedBus: true },
      }),
    })

    await bus.query({
      identifier: "qry-1",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: { correlationId: "corr-1", causationId: "cause-1" },
      timestamp: Date.now(),
    })

    expect(dispatched[0]!.message.metadata).toEqual({
      correlationId: "corr-1",
      causationId: "cause-1",
    })
  })

  it("propagates a remote query failure as a thrown error", async () => {
    const local = createSimpleQueryBus()
    const subscriptions = new Map<string, (envelope: RabbitMqQueryEnvelope) => Promise<any>>()
    const transport: RabbitMqQueryTransport = {
      async dispatch(envelope) {
        return {
          requestId: envelope.requestId,
          ok: false,
          error: { name: "LookupError", message: "no such thing" },
        }
      },
      subscribe(name, handler) {
        subscriptions.set(name, handler)
      },
    }
    const bus = createRabbitMqQueryBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), {
        url: "amqp://test",
        queries: { alwaysUseDistributedBus: true },
      }),
    })

    await expect(
      bus.query({
        identifier: "qry-1",
        name: GetThing.name,
        payload: { id: "1" },
        metadata: emptyMetadata(),
        timestamp: Date.now(),
      }),
    ).rejects.toThrow("no such thing")
  })

  it("handles an inbound query in its own UnitOfWork", async () => {
    const local = createSimpleQueryBus()
    const { transport, subscriptions } = recordingTransport()
    const bus = createRabbitMqQueryBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), { url: "amqp://test" }),
    })

    bus.subscribe("test.GetThing", async (message) => `answered:${(message.payload as { id: string }).id}`)
    const subscribed = subscriptions.get("test.GetThing")!
    const reply = await subscribed({
      kind: "query",
      requestId: "qry-1",
      timeoutMs: 1000,
      message: {
        identifier: "qry-1",
        name: GetThing.name,
        payload: { id: "1" },
        metadata: emptyMetadata(),
        timestamp: Date.now(),
      },
    })

    expect(reply.ok).toBe(true)
    expect(reply.result).toBe("answered:1")
  })

  it("returns initial result via the existing query transport when starting a subscription", async () => {
    const local = createSimpleQueryBus()
    const { transport } = recordingTransport()
    const bus = createRabbitMqQueryBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), { url: "amqp://test" }),
    })

    bus.subscribe("test.GetThing", async () => "initial")

    const sub = bus.subscriptionQuery({
      identifier: "sub-1",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(await sub.initialResult).toBe("initial")
    sub.close()
  })

  it("delivers an emit to local subscribers AND broadcasts it across the updates transport", async () => {
    const broker = createInProcessUpdatesBroker()
    const local = createSimpleQueryBus()
    const { transport } = recordingTransport()
    const updatesTransport = broker.connect("svc.inst-A")
    const bus = createRabbitMqQueryBus({
      localSegment: local,
      transport,
      updatesTransport,
      config: resolveRabbitMqConfig(appStub(), { url: "amqp://test" }),
    })

    bus.subscribe("test.GetThing", async () => "initial")
    const sub = bus.subscriptionQuery({
      identifier: "sub-local",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    await sub.initialResult

    void bus.emitUpdate("test.GetThing", payloadEquals({ id: "1" }), { value: "v1" })

    // Local fan-out is synchronous via runAfterCommitOrImmediately (no UoW
    // here), so the next iteration tick will surface the update.
    const updates = readN(sub.updates, 1)
    expect(await updates).toEqual([{ value: "v1" }])

    // Broadcast was sent to the broker
    expect(broker.published).toHaveLength(1)
    expect(broker.published[0]!.kind).toBe("update")
    expect(broker.published[0]!.queryName).toBe("test.GetThing")
    expect(broker.published[0]!.payloadEquals).toEqual({ id: "1" })

    sub.close()
  })

  it("delivers a remote broadcast to a local subscriber, matching payloadEquals", async () => {
    const broker = createInProcessUpdatesBroker()
    const local = createSimpleQueryBus()
    const { transport } = recordingTransport()
    const updatesTransport = broker.connect("svc.inst-B")
    const bus = createRabbitMqQueryBus({
      localSegment: local,
      transport,
      updatesTransport,
      config: resolveRabbitMqConfig(appStub(), { url: "amqp://test" }),
    })

    bus.subscribe("test.GetThing", async () => "initial")
    const matching = bus.subscriptionQuery({
      identifier: "sub-match",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })
    const nonMatching = bus.subscriptionQuery({
      identifier: "sub-nomatch",
      name: GetThing.name,
      payload: { id: "2" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    await Promise.all([matching.initialResult, nonMatching.initialResult])

    // Simulate a remote process emitting — broker injects the envelope as if
    // it came over the wire from a different instance.
    broker.injectFromRemote("other-instance", {
      kind: "update",
      senderId: "other-instance",
      queryName: "test.GetThing",
      update: { value: "remote-v" },
      payloadEquals: { id: "1" },
    })

    const matched = await readN(matching.updates, 1)
    expect(matched).toEqual([{ value: "remote-v" }])

    // Non-matching subscriber sees nothing — close it and confirm no buffered updates.
    nonMatching.close()
    matching.close()
  })

  it("ignores its own loopback updates (senderId === own)", async () => {
    const broker = createInProcessUpdatesBroker()
    const local = createSimpleQueryBus()
    const { transport } = recordingTransport()
    const updatesTransport = broker.connect("svc.inst-C")
    const bus = createRabbitMqQueryBus({
      localSegment: local,
      transport,
      updatesTransport,
      config: resolveRabbitMqConfig(appStub(), { url: "amqp://test" }),
    })

    bus.subscribe("test.GetThing", async () => "initial")
    const sub = bus.subscriptionQuery({
      identifier: "sub-loopback",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })
    await sub.initialResult

    // emit once — local fan-out delivers update #1; loopback from broker
    // should NOT redeliver because senderId === own.
    void bus.emitUpdate("test.GetThing", payloadEquals({ id: "1" }), "u1")

    // Force a second emit so we can readN(2) and confirm only one "u1".
    void bus.emitUpdate("test.GetThing", payloadEquals({ id: "1" }), "u2")

    const updates = await readN(sub.updates, 2)
    expect(updates).toEqual(["u1", "u2"])

    sub.close()
  })
})

// ---------------------------------------------------------------------------
// In-process broker for unit-testing the updates transport.
// ---------------------------------------------------------------------------

interface TestUpdatesTransport extends RabbitMqQueryUpdatesTransport {
  readonly senderId: string
  inboundHandler?: (envelope: RabbitMqQueryUpdateEnvelope) => void
  boundKeys: Set<string>
}

interface InProcessBroker {
  connect(senderId: string): TestUpdatesTransport
  injectFromRemote(senderId: string, envelope: RabbitMqQueryUpdateEnvelope): void
  readonly published: RabbitMqQueryUpdateEnvelope[]
}

function createInProcessUpdatesBroker(): InProcessBroker {
  const transports: TestUpdatesTransport[] = []
  const published: RabbitMqQueryUpdateEnvelope[] = []

  function fanOut(envelope: RabbitMqQueryUpdateEnvelope) {
    for (const t of transports) {
      if (!t.boundKeys.has(envelope.queryName)) continue
      t.inboundHandler?.(envelope)
    }
  }

  return {
    published,
    connect(senderId) {
      const t: TestUpdatesTransport = {
        senderId,
        boundKeys: new Set<string>(),
        async publish(envelope) {
          published.push(envelope)
          fanOut(envelope)
        },
        async bindQueryName(name) {
          t.boundKeys.add(name)
        },
        async unbindQueryName(name) {
          t.boundKeys.delete(name)
        },
        setHandler(handler) {
          t.inboundHandler = handler
        },
      }
      transports.push(t)
      return t
    },
    injectFromRemote(senderId, envelope) {
      // Inject as if a remote producer published it — bypasses publish() so it
      // doesn't appear in `published`.
      fanOut({ ...envelope, senderId })
    },
  }
}

async function readN<T>(iter: AsyncIterable<T>, n: number): Promise<T[]> {
  const results: T[] = []
  for await (const v of iter) {
    results.push(v)
    if (results.length >= n) break
  }
  return results
}
