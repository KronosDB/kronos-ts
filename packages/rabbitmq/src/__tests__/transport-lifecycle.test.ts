import { EventEmitter } from "node:events"
import { describe, expect, it } from "bun:test"
import { qn, type MessagingLimits } from "@kronos-ts/core"
import { amqpRabbitMqCommandTransport } from "../amqp-command-transport.js"
import { amqpRabbitMqQueryTransport } from "../amqp-query-transport.js"
import { resolveRabbitMqConfig } from "../rabbitmq.js"

for (const kind of ["command", "query"] as const) {
  describe(`AMQP ${kind} lifecycle`, () => {
    function setup(limits?: MessagingLimits, shutdownTimeoutMs = 100) {
      const consumers = new Map<string, (message: any) => void>()
      const config = resolveRabbitMqConfig({
        url: "amqp://test",
        identity: { serviceName: "qa", instanceId: "one" },
        limits,
        shutdownTimeoutMs,
      })
      const events = new EventEmitter()
      const replies: any[] = []
      let channelClosed = false
      const channel: any = {
        on: events.on.bind(events),
        cancel: async () => {},
        ack: () => {},
        nack: () => {},
        sendToQueue: (_queue: string, body: Buffer) => {
          replies.push(JSON.parse(body.toString()))
          return true
        },
        assertExchange: async () => {},
        assertQueue: async (queue: string) => ({ queue }),
        bindQueue: async () => {},
        prefetch: async () => {},
        consume: async (queue: string, fn: any) => {
          consumers.set(queue, fn)
          return {}
        },
        publish: () => true,
        close: async () => {
          channelClosed = true
          events.emit("close")
        },
      }
      const create = kind === "command" ? amqpRabbitMqCommandTransport : amqpRabbitMqQueryTransport
      const transport = create(config, { channel: async () => channel, close: async () => {} })
      const envelope: any = {
        kind,
        requestId: "same-id",
        expectsReply: true,
        timeoutMs: 100,
        message: {
          identifier: "same-id",
          name: qn("qa", "Work"),
          payload: {},
          metadata: {},
        },
      }
      const replyQueue =
        kind === "command" ? config.topology.commandReplyQueue() : config.topology.queryReplyQueue()
      return {
        channel,
        transport,
        envelope,
        events,
        replies,
        get channelClosed() {
          return channelClosed
        },
        deliver(id = "incoming") {
          const queue =
            kind === "command"
              ? config.topology.commandQueue("qa.Work")
              : config.topology.queryQueue("qa.Work")
          consumers.get(queue)!({
            properties: { correlationId: id, replyTo: "caller" },
            content: Buffer.from(JSON.stringify({ ...envelope, requestId: id })),
          })
        },
        reply(result: unknown) {
          consumers.get(replyQueue)!({
            properties: { correlationId: "same-id" },
            content: Buffer.from(JSON.stringify({ requestId: "same-id", ok: true, result })),
          })
        },
      }
    }
    it("bounds handlers and replies with overload while existing work awaits", async () => {
      const h = setup({ maxConcurrentHandlers: 1 })
      let release!: () => void
      const blocked = new Promise<void>((r) => {
        release = r
      })
      h.transport.subscribe("qa.Work", async (m) => {
        await blocked
        return { requestId: m.requestId, ok: true }
      })
      await h.transport.ready()
      h.deliver("parent")
      h.deliver("child")
      await new Promise((r) => setTimeout(r, 0))
      expect(h.replies[0].requestId).toBe("child")
      expect(h.replies[0].error.message).toContain("overloaded")
      release()
      await h.transport.close()
      expect(h.replies.find((r) => r.requestId === "parent")?.ok).toBe(true)
    })
    it("waits for running handlers before closing their channel", async () => {
      const h = setup()
      let release!: () => void
      const blocked = new Promise<void>((r) => {
        release = r
      })
      h.transport.subscribe("qa.Work", async (m) => {
        await blocked
        return { requestId: m.requestId, ok: true }
      })
      await h.transport.ready()
      h.deliver()
      const close = h.transport.close()
      await new Promise((r) => setTimeout(r, 0))
      expect(h.channelClosed).toBe(false)
      release()
      await close
      expect(h.channelClosed).toBe(true)
      expect(h.replies[0]?.ok).toBe(true)
    })
    it("bounds shutdown even if a handler never settles", async () => {
      const h = setup(undefined, 10)
      let release!: () => void
      h.transport.subscribe("qa.Work", async () => {
        await new Promise<void>((r) => {
          release = r
        })
        return { requestId: "incoming", ok: true }
      })
      await h.transport.ready()
      h.deliver()
      try {
        await expect(h.transport.close()).rejects.toThrow(/shutdown timed out/)
      } finally {
        release()
      }
      expect(h.channelClosed).toBe(true)
    })
    it("rejects pending work on channel loss and refuses silent reuse", async () => {
      const h = setup()
      const pending = h.transport.dispatch(h.envelope)
      await new Promise((r) => setTimeout(r, 0))
      h.events.emit("close")
      await expect(pending).rejects.toThrow(/outcome is unknown/)
      await expect(h.transport.dispatch(h.envelope)).rejects.toThrow(/outcome is unknown/)
      await h.transport.close()
    })
    it("fails unroutable requests immediately using broker returns", async () => {
      const h = setup()
      const pending = h.transport.dispatch(h.envelope)
      await new Promise((r) => setTimeout(r, 0))
      h.events.emit("return", { properties: { correlationId: h.envelope.requestId } })
      await expect(pending).rejects.toThrow(/No RabbitMQ queue/)
      await h.transport.close()
    })
    it("honors socket backpressure without republishing an accepted message", async () => {
      const h = setup()
      let publishes = 0
      h.channel.publish = () => {
        publishes++
        return false
      }
      const first = h.transport.dispatch(h.envelope)
      await new Promise((r) => setTimeout(r, 0))
      await expect(h.transport.dispatch({ ...h.envelope, requestId: "second" })).rejects.toThrow(
        /buffer is full/,
      )
      expect(publishes).toBe(1)
      h.reply(1)
      await first
      h.events.emit("drain")
      h.channel.publish = () => true
      const next = h.transport.dispatch(h.envelope)
      await new Promise((r) => setTimeout(r, 0))
      h.reply(2)
      expect((await next).result).toBe(2)
      await h.transport.close()
    })
    it("ready reports a background bind failure even after the promise settles", async () => {
      const { channel, transport } = setup()
      await transport.connect()
      channel.bindQueue = async () => {
        throw new Error("bind failed")
      }
      transport.subscribe("qa.Work", async () => ({ requestId: "x", ok: true }))
      await new Promise((r) => setTimeout(r, 0))
      await expect(transport.ready()).rejects.toThrow("bind failed")
      await transport.close()
    })
    it("rejects duplicate in-flight identifiers without stealing the first reply", async () => {
      const { transport, envelope, reply } = setup()
      await transport.connect()
      const first = transport.dispatch(envelope as never)
      void first.catch(() => {})
      try {
        await expect(transport.dispatch(envelope as never)).rejects.toThrow(/already.*pending/i)
        await new Promise((r) => setTimeout(r, 0))
        reply(42)
        expect((await first).result).toBe(42)
      } finally {
        await transport.close()
      }
    })
    it("cleans pending state when publish throws, so the identifier can be retried", async () => {
      const { channel, transport, envelope, reply } = setup()
      await transport.connect()
      channel.publish = () => {
        throw new Error("publish failed")
      }
      await expect(transport.dispatch(envelope as never)).rejects.toThrow("publish failed")
      channel.publish = () => true
      const retried = transport.dispatch(envelope as never)
      await new Promise((r) => setTimeout(r, 0))
      reply(42)
      expect((await retried).result).toBe(42)
      await transport.close()
    })
  })
}

import { amqpChannelSource, rabbitMqConnection } from "../connection.js"

it("a failed AMQP connection constructor closes sibling channels and the socket", async () => {
  const closed = new Set<number>()
  let created = 0,
    socketClosed = false
  await expect(
    rabbitMqConnection("amqp://test", {
      serviceName: "qa",
      instanceId: "partial",
      amqpConnect: async () =>
        ({
          async createChannel() {
            const id = created++
            return {
              async assertExchange() {
                if (id === 0) throw new Error("declaration failed")
              },
              async assertQueue(queue: string) {
                return { queue }
              },
              async bindQueue() {},
              async prefetch() {},
              async consume() {
                return {}
              },
              publish: () => true,
              async close() {
                closed.add(id)
              },
            } as any
          },
          async close() {
            socketClosed = true
          },
        }) as any,
    }),
  ).rejects.toThrow("declaration failed")
  expect(closed.size).toBe(created)
  expect(socketClosed).toBe(true)
})

it("closing a source whose dial failed is safe and permanently closes it", async () => {
  const source = amqpChannelSource("amqp://test", async () => {
    throw new Error("dial failed")
  })
  await expect(source.channel()).rejects.toThrow("dial failed")
  await source.close()
  await expect(source.channel()).rejects.toThrow(/closed/)
})
