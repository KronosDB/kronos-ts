import { describe, expect, it } from "bun:test"
import { qn } from "@kronos-ts/core"
import { amqpRabbitMqQueryTransport } from "../amqp-query-transport.js"
import { amqpChannelSource } from "../connection.js"
import { resolveRabbitMqConfig } from "../rabbitmq.js"
import type { RabbitMqQueryEnvelope } from "../query-bus.js"

function fakeAmqp() {
  const consumers = new Map<string, (msg: any) => void>()
  const published: Array<{ exchange: string; routingKey: string; body: Buffer; options: any }> = []
  const sentReplies: Array<{ queue: string; body: Buffer; options: any }> = []
  const acks: any[] = []
  const nacks: any[] = []
  const exchanges: Array<{ name: string; type: string; options: any }> = []
  const queues: Array<{ name: string; options: any }> = []
  const bindings: Array<{ queue: string; exchange: string; routingKey: string }> = []
  let deliveryTag = 0

  const channel: any = {
    assertExchange: async (name: string, type: string, options: any) => {
      exchanges.push({ name, type, options })
      return {}
    },
    assertQueue: async (name: string, options: any) => {
      queues.push({ name, options })
      return { queue: name }
    },
    bindQueue: async (queue: string, exchange: string, routingKey: string) => {
      bindings.push({ queue, exchange, routingKey })
    },
    prefetch: async () => {},
    consume: async (queue: string, cb: (msg: any) => void) => {
      consumers.set(queue, cb)
      return { consumerTag: `consumer-${queue}` }
    },
    publish: (_exchange: string, routingKey: string, body: Buffer, options: any) => {
      published.push({ exchange: _exchange, routingKey, body, options })
      return true
    },
    sendToQueue: (queue: string, body: Buffer, options: any) => {
      sentReplies.push({ queue, body, options })
      return true
    },
    ack: (msg: any) => { acks.push(msg) },
    nack: (msg: any, allUpTo?: boolean, requeue?: boolean) => { nacks.push({ msg, allUpTo, requeue }) },
    close: async () => {},
  }

  const connection: any = {
    createChannel: async () => channel,
    close: async () => {},
  }

  function deliver(queue: string, body: unknown, properties: any = {}) {
    const cb = consumers.get(queue)
    if (!cb) throw new Error(`No consumer for ${queue}`)
    cb({
      content: Buffer.from(JSON.stringify(body)),
      properties,
      fields: { deliveryTag: ++deliveryTag },
    })
  }

  return { connection, channel, consumers, published, sentReplies, acks, nacks, exchanges, queues, bindings, deliver }
}

function config() {
  return resolveRabbitMqConfig({
    identity: { serviceName: "faculty-service", instanceId: "pod-1" },
    url: "amqp://test",
  })
}

function envelope(): RabbitMqQueryEnvelope {
  return {
    kind: "query",
    requestId: "qry-1",
    timeoutMs: 1000,
    message: {
      identifier: "qry-1",
      name: qn("faculty", "GetThing"),
      payload: { id: "1" },
      metadata: {},
      timestamp: Date.now(),
    },
  }
}

describe("AMQP RabbitMQ query transport", () => {
  it("declares the queries and dead-letter exchanges", async () => {
    const fake = fakeAmqp()
    const transport = amqpRabbitMqQueryTransport(config(), amqpChannelSource("amqp://test", async () => fake.connection))
    await transport.connect()

    expect(fake.exchanges).toContainEqual({
      name: "kronos.queries",
      type: "topic",
      options: { durable: true },
    })
    expect(fake.exchanges).toContainEqual({
      name: "kronos.dlx",
      type: "topic",
      options: { durable: true },
    })
  })

  it("publishes queries and resolves correlated replies", async () => {
    const fake = fakeAmqp()
    const transport = amqpRabbitMqQueryTransport(config(), amqpChannelSource("amqp://test", async () => fake.connection))
    await transport.connect()

    const resultPromise = transport.dispatch(envelope())
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(fake.published).toHaveLength(1)
    expect(fake.published[0]!.exchange).toBe("kronos.queries")
    expect(fake.published[0]!.routingKey).toBe("faculty.GetThing")
    expect(fake.published[0]!.options.replyTo).toBe("kronos.query-replies.faculty-service.pod-1")

    fake.deliver(
      "kronos.query-replies.faculty-service.pod-1",
      { requestId: "qry-1", ok: true, result: "ok" },
      { correlationId: "qry-1" },
    )

    await expect(resultPromise).resolves.toEqual({ requestId: "qry-1", ok: true, result: "ok" })
  })

  it("times out pending query dispatches", async () => {
    const fake = fakeAmqp()
    const transport = amqpRabbitMqQueryTransport(config(), amqpChannelSource("amqp://test", async () => fake.connection))
    await transport.connect()

    const resultPromise = transport.dispatch({ ...envelope(), timeoutMs: 1 })

    await expect(resultPromise).rejects.toThrow(/timed out/)
  })

  it("declares query queues and sends replies for consumed queries", async () => {
    const fake = fakeAmqp()
    const transport = amqpRabbitMqQueryTransport(config(), amqpChannelSource("amqp://test", async () => fake.connection))
    transport.subscribe("faculty.GetThing", async (incoming) => ({
      requestId: incoming.requestId,
      ok: true,
      result: incoming.message.payload,
    }))
    await transport.connect()

    expect(fake.bindings).toContainEqual({
      queue: "kronos.queries.faculty-service.faculty.GetThing",
      exchange: "kronos.queries",
      routingKey: "faculty.GetThing",
    })
    expect(fake.queues).toContainEqual({
      name: "kronos.queries.faculty-service.faculty.GetThing",
      options: {
        durable: true,
        exclusive: false,
        autoDelete: false,
        arguments: {
          "x-dead-letter-exchange": "kronos.dlx",
          "x-dead-letter-routing-key": "faculty.GetThing",
        },
      },
    })

    fake.deliver(
      "kronos.queries.faculty-service.faculty.GetThing",
      envelope(),
      { replyTo: "reply-q", correlationId: "qry-1" },
    )

    await Promise.resolve()
    expect(fake.sentReplies).toHaveLength(1)
    expect(JSON.parse(fake.sentReplies[0]!.body.toString("utf8"))).toEqual({
      requestId: "qry-1",
      ok: true,
      result: { id: "1" },
    })
    expect(fake.acks).toHaveLength(1)
  })

  it("sends an error reply and nacks when consumed query handling fails", async () => {
    const fake = fakeAmqp()
    const transport = amqpRabbitMqQueryTransport(config(), amqpChannelSource("amqp://test", async () => fake.connection))
    transport.subscribe("faculty.GetThing", async () => {
      throw new Error("boom")
    })
    await transport.connect()

    fake.deliver(
      "kronos.queries.faculty-service.faculty.GetThing",
      envelope(),
      { replyTo: "reply-q", correlationId: "qry-1" },
    )

    await Promise.resolve()
    expect(fake.sentReplies).toHaveLength(1)
    expect(JSON.parse(fake.sentReplies[0]!.body.toString("utf8"))).toMatchObject({
      requestId: "qry-1",
      ok: false,
      error: { name: "Error", message: "boom" },
    })
    expect(fake.nacks).toHaveLength(1)
    expect(fake.nacks[0]!.requeue).toBe(false)
  })

  it("does not bind the same query handler more than once", async () => {
    const fake = fakeAmqp()
    const transport = amqpRabbitMqQueryTransport(config(), amqpChannelSource("amqp://test", async () => fake.connection))
    transport.subscribe("faculty.GetThing", async (incoming) => ({ requestId: incoming.requestId, ok: true }))
    await transport.connect()
    transport.subscribe("faculty.GetThing", async (incoming) => ({ requestId: incoming.requestId, ok: true }))
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(fake.bindings.filter((b) => b.routingKey === "faculty.GetThing")).toHaveLength(1)
  })
})
