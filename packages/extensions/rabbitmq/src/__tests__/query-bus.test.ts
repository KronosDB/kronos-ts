import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn } from "@kronos-ts/common"
import { query } from "@kronos-ts/messaging"
import { createSimpleQueryBus } from "@kronos-ts/messaging"
import { createRabbitMqQueryBus, type RabbitMqQueryEnvelope, type RabbitMqQueryTransport } from "../query-bus.js"
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

  it("delegates subscription queries to the local segment", async () => {
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
})
