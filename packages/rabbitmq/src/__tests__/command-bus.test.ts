import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn } from "@kronos-ts/core"
import { command, unitOfWork } from "@kronos-ts/core"
import {
  correlation,
  interceptingCommandBus,
  localCommandBus,
  type CommandBus,
} from "@kronos-ts/core"
import {
  rabbitMqCommandBus,
  type RabbitMqBusOptions,
  type RabbitMqCommandEnvelope,
  type RabbitMqCommandTransport,
} from "../command-bus.js"
import { resolveRabbitMqConfig, type RabbitMqConfig } from "../rabbitmq.js"
import type { RabbitMqIdentity } from "../topology.js"

const DoThing = command({
  name: qn("test", "DoThing"),
  payload: z.object({ id: z.string() }),
})

const DEFAULT_IDENTITY: RabbitMqIdentity = { serviceName: "svc", instanceId: "inst" }

function rabbitConfig(
  config: Omit<RabbitMqConfig, "identity">,
  identity: RabbitMqIdentity = DEFAULT_IDENTITY,
) {
  return resolveRabbitMqConfig({ identity, ...config })
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

/**
 * The composition under test: the transport owns its own client-side routing,
 * over the caller's local segment, with correlation OUTSIDE the fork.
 */
function busOver(
  transport: RabbitMqCommandTransport,
  localSegment: CommandBus,
  options?: RabbitMqBusOptions,
  config = rabbitConfig({ url: "amqp://test" }),
): CommandBus {
  return interceptingCommandBus(
    rabbitMqCommandBus(localSegment, { config, commandTransport: transport }, options),
    correlation,
  )
}

describe("RabbitMQ command bus", () => {
  it("prefers local handlers by default", async () => {
    const { transport, dispatched } = recordingTransport()
    const bus = busOver(transport, localCommandBus(unitOfWork))

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
    const { transport, dispatched } = recordingTransport()
    const bus = busOver(transport, localCommandBus(unitOfWork), { preferLocal: false })

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
    const { transport, dispatched } = recordingTransport()
    const bus = busOver(transport, localCommandBus(unitOfWork), { preferLocal: false })

    await bus.dispatch({
      identifier: "cmd-1",
      name: DoThing.name,
      payload: { id: "1" },
      metadata: { correlationId: "corr-1", causationId: "cause-1" },
      timestamp: Date.now(),
    })

    // Correlation/causation correlation rides on message metadata, not a separate
    // processing-context snapshot. `correlation` SEEDS both fields and clobbers
    // neither: a message that already carries a cause was caused by something,
    // so "cause-1" survives the hop.
    expect(dispatched[0]!.message.metadata).toEqual({
      correlationId: "corr-1",
      causationId: "cause-1",
    })
  })

  it("seeds correlation on a root message that carries none", async () => {
    const { transport, dispatched } = recordingTransport()
    const bus = busOver(transport, localCommandBus(unitOfWork), { preferLocal: false })

    await bus.dispatch({
      identifier: "cmd-1",
      name: DoThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    // No inbound correlation — this message IS the root, so both fields start at
    // its own identifier.
    expect(dispatched[0]!.message.metadata).toEqual({
      correlationId: "cmd-1",
      causationId: "cmd-1",
    })
  })

  it("stamps the connection's timeout on the envelope when routing names none", async () => {
    const { transport, dispatched } = recordingTransport()
    const bus = busOver(transport, localCommandBus(unitOfWork), { preferLocal: false })

    await bus.dispatch({
      identifier: "cmd-1",
      name: DoThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(dispatched[0]!.timeoutMs).toBe(30_000)
  })

  it("lets the routing layer override the transport's timeout default", async () => {
    const { transport, dispatched } = recordingTransport()
    const bus = busOver(transport, localCommandBus(unitOfWork), {
      preferLocal: false,
      timeoutMs: 1_500,
    })

    await bus.dispatch({
      identifier: "cmd-1",
      name: DoThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(dispatched[0]!.timeoutMs).toBe(1_500)
  })

  it("handles an inbound command in its own UnitOfWork", async () => {
    const { transport, subscriptions } = recordingTransport()
    const bus = busOver(transport, localCommandBus(unitOfWork))

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

  it("reports a handler failure as an ok:false reply rather than a rejection", async () => {
    // The transport nacks (and therefore dead-letters) on a rejection; a handler
    // that ran and threw is an answered message, so it must ack with ok:false.
    const { transport, subscriptions } = recordingTransport()
    const bus = busOver(transport, localCommandBus(unitOfWork))

    bus.subscribe("test.DoThing", async () => {
      throw new Error("handler blew up")
    })
    const reply = await subscriptions.get("test.DoThing")!({
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

    expect(reply.ok).toBe(false)
    expect(reply.error?.message).toBe("handler blew up")
  })

  it("propagates a remote command failure as a thrown error", async () => {
    const transport: RabbitMqCommandTransport = {
      async dispatch(envelope) {
        return {
          requestId: envelope.requestId,
          ok: false,
          error: { name: "RemoteBoom", message: "far side failed" },
        }
      },
      subscribe() {},
    }
    const bus = busOver(transport, localCommandBus(unitOfWork), { preferLocal: false })

    await expect(
      bus.dispatch({
        identifier: "cmd-1",
        name: DoThing.name,
        payload: { id: "1" },
        metadata: emptyMetadata(),
        timestamp: Date.now(),
      }),
    ).rejects.toThrow("far side failed")
  })
})
