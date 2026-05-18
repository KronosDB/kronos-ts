import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn } from "@kronos-ts/common"
import { command } from "@kronos-ts/messaging"
import { createSimpleCommandBus } from "@kronos-ts/messaging"
import { createRabbitMqCommandBus, type RabbitMqCommandEnvelope, type RabbitMqCommandTransport } from "../command-bus.js"
import { resolveRabbitMqConfig } from "../rabbitmq.js"

const DoThing = command({
  name: qn("test", "DoThing"),
  payload: z.object({ id: z.string() }),
})

function appStub(overrides: any = {}) {
  return {
    identity: { serviceName: "svc", instanceId: "inst" },
    ...overrides,
  } as any
}

function recordingTransport() {
  const subscriptions = new Map<string, (envelope: RabbitMqCommandEnvelope) => Promise<any>>()
  const dispatched: RabbitMqCommandEnvelope[] = []
  const transport: RabbitMqCommandTransport = {
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

describe("RabbitMQ command bus", () => {
  it("prefers local handlers by default", async () => {
    const local = createSimpleCommandBus()
    const { transport, dispatched } = recordingTransport()
    const bus = createRabbitMqCommandBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), { url: "amqp://test" }),
    })

    bus.subscribe("test.DoThing", async () => "local-ok")

    const result = await bus.dispatch({
      identifier: "cmd-1",
      name: DoThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(result).toBe("local-ok")
    expect(dispatched).toHaveLength(0)
  })

  it("routes through transport when distributed routing is forced", async () => {
    const local = createSimpleCommandBus()
    const { transport, dispatched } = recordingTransport()
    const bus = createRabbitMqCommandBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), {
        url: "amqp://test",
        commands: { alwaysUseDistributedBus: true },
      }),
    })

    bus.subscribe("test.DoThing", async () => "local-ok")

    const result = await bus.dispatch({
      identifier: "cmd-1",
      name: DoThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(result).toBe("remote-ok")
    expect(dispatched).toHaveLength(1)
  })

  it("carries command metadata across the transport envelope", async () => {
    const local = createSimpleCommandBus()
    const { transport, dispatched } = recordingTransport()
    const bus = createRabbitMqCommandBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), {
        url: "amqp://test",
        commands: { alwaysUseDistributedBus: true },
      }),
    })

    await bus.dispatch({
      identifier: "cmd-1",
      name: DoThing.name,
      payload: { id: "1" },
      metadata: { correlationId: "corr-1", causationId: "cause-1" },
      timestamp: Date.now(),
    })

    // AF5 model: correlation/causation lineage rides on message metadata,
    // not a separate processing-context snapshot.
    expect(dispatched[0]!.message.metadata).toEqual({
      correlationId: "corr-1",
      causationId: "cause-1",
    })
  })

  it("handles an inbound command in its own UnitOfWork", async () => {
    const local = createSimpleCommandBus()
    const { transport, subscriptions } = recordingTransport()
    const bus = createRabbitMqCommandBus({
      localSegment: local,
      transport,
      config: resolveRabbitMqConfig(appStub(), { url: "amqp://test" }),
    })

    bus.subscribe("test.DoThing", async (message) => `handled:${(message.payload as { id: string }).id}`)
    const subscribed = subscriptions.get("test.DoThing")!
    const reply = await subscribed({
      kind: "command",
      requestId: "cmd-1",
      expectsReply: true,
      timeoutMs: 1000,
      message: {
        identifier: "cmd-1",
        name: DoThing.name,
        payload: { id: "1" },
        metadata: emptyMetadata(),
        timestamp: Date.now(),
      },
    })

    expect(reply.ok).toBe(true)
    expect(reply.result).toBe("handled:1")
  })
})
