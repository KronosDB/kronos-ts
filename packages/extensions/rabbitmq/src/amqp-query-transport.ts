import type { Channel, ConsumeMessage } from "amqplib"
import type {
  RabbitMqQueryEnvelope,
  RabbitMqQueryReplyEnvelope,
  RabbitMqQueryTransport,
} from "./query-bus.js"
import type { AmqpConnection } from "./connection.js"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

interface PendingRequest {
  resolve(reply: RabbitMqQueryReplyEnvelope): void
  reject(error: Error): void
  timer: ReturnType<typeof setTimeout>
}

/**
 * AMQP request/reply transport for distributed queries.
 *
 * Mirrors {@link AmqpRabbitMqCommandTransport}: a per-process exclusive reply
 * queue, durable per-query handler queues bound to the shared queries exchange,
 * and correlation-id matched replies. Takes its own channel off the shared
 * {@link AmqpConnection}.
 */
export class AmqpRabbitMqQueryTransport implements RabbitMqQueryTransport {
  private channel: Channel | undefined
  private replyQueue: string | undefined
  private readonly handlers = new Map<string, (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>>()
  private readonly boundHandlers = new Set<string>()
  private readonly pending = new Map<string, PendingRequest>()
  private connectPromise: Promise<void> | undefined
  private closed = false

  constructor(
    private readonly config: RabbitMqResolvedConfig,
    private readonly connection: AmqpConnection,
  ) {}

  async connect(): Promise<void> {
    if (this.connectPromise) return this.connectPromise
    this.connectPromise = this.doConnect()
    return this.connectPromise
  }

  private async doConnect(): Promise<void> {
    this.channel = await this.connection.channel()

    await this.channel.assertExchange(this.config.topology.queriesExchange, "topic", { durable: true })
    if (this.config.retry.deadLetter) {
      await this.channel.assertExchange(this.config.retry.deadLetterExchange, "topic", { durable: true })
    }

    const reply = await this.channel.assertQueue(this.config.topology.queryReplyQueue(), {
      durable: false,
      exclusive: true,
      autoDelete: true,
    })
    this.replyQueue = reply.queue
    await this.channel.consume(reply.queue, (msg) => this.handleReply(msg), { noAck: true })

    this.boundHandlers.clear()
    for (const [queryName, handler] of this.handlers) {
      await this.bindQueryHandler(queryName, handler)
    }
  }

  /**
   * Close this transport's channel and fail any in-flight requests. The shared
   * connection is owned by its creator (see {@link createAmqpConnection}) and is
   * not closed here.
   */
  async close(): Promise<void> {
    this.closed = true
    for (const [requestId, pending] of this.pending) {
      clearTimeout(pending.timer)
      pending.reject(new Error(`RabbitMQ query transport closed before reply for ${requestId}`))
    }
    this.pending.clear()
    await this.channel?.close().catch(() => {})
  }

  async dispatch(envelope: RabbitMqQueryEnvelope): Promise<RabbitMqQueryReplyEnvelope> {
    await this.connect()
    if (this.closed) throw new Error("RabbitMQ query transport is closed")
    const channel = this.requireChannel()
    const replyQueue = this.replyQueue
    if (!replyQueue) throw new Error("RabbitMQ reply queue is not initialized")

    const body = Buffer.from(JSON.stringify(envelope))
    const routingKey = this.config.topology.queryRoutingKey(envelope.message.name)

    return new Promise<RabbitMqQueryReplyEnvelope>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(envelope.requestId)
        reject(new Error(`Query ${routingKey} timed out after ${envelope.timeoutMs}ms`))
      }, envelope.timeoutMs)
      this.pending.set(envelope.requestId, { resolve, reject, timer })

      channel.publish(
        this.config.topology.queriesExchange,
        routingKey,
        body,
        {
          contentType: "application/json",
          correlationId: envelope.requestId,
          replyTo: replyQueue,
          persistent: true,
        },
      )
    })
  }

  subscribe(
    queryName: string,
    handler: (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>,
  ): void {
    this.handlers.set(queryName, handler)
    if (this.channel) {
      void this.bindQueryHandler(queryName, handler)
    }
  }

  private async bindQueryHandler(
    queryName: string,
    handler: (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>,
  ): Promise<void> {
    if (this.boundHandlers.has(queryName)) return
    const channel = this.requireChannel()
    const queue = this.config.topology.queryQueue(queryName)
    const routingKey = this.config.topology.queryRoutingKey(queryName)

    await channel.assertQueue(queue, {
      durable: true,
      exclusive: false,
      autoDelete: false,
      arguments: this.config.retry.deadLetter
        ? {
            "x-dead-letter-exchange": this.config.retry.deadLetterExchange,
            "x-dead-letter-routing-key": routingKey,
          }
        : undefined,
    })
    await channel.bindQueue(queue, this.config.topology.queriesExchange, routingKey)
    await channel.prefetch(1)
    await channel.consume(queue, (msg) => this.handleQuery(msg, handler), { noAck: false })
    this.boundHandlers.add(queryName)
  }

  private async handleQuery(
    msg: ConsumeMessage | null,
    handler: (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>,
  ): Promise<void> {
    if (!msg) return
    const channel = this.requireChannel()
    try {
      const envelope = JSON.parse(msg.content.toString("utf8")) as RabbitMqQueryEnvelope
      const reply = await handler(envelope)
      if (msg.properties.replyTo) {
        channel.sendToQueue(
          msg.properties.replyTo,
          Buffer.from(JSON.stringify(reply)),
          {
            contentType: "application/json",
            correlationId: msg.properties.correlationId,
          },
        )
      }
      channel.ack(msg)
    } catch (error) {
      const requestId = msg.properties.correlationId
      if (msg.properties.replyTo && requestId) {
        const reply: RabbitMqQueryReplyEnvelope = {
          requestId,
          ok: false,
          error: serializeError(error),
        }
        channel.sendToQueue(
          msg.properties.replyTo,
          Buffer.from(JSON.stringify(reply)),
          {
            contentType: "application/json",
            correlationId: requestId,
          },
        )
      }
      channel.nack(msg, false, false)
    }
  }

  private handleReply(msg: ConsumeMessage | null): void {
    if (!msg) return
    const requestId = msg.properties.correlationId
    if (!requestId) return
    const pending = this.pending.get(requestId)
    if (!pending) return
    this.pending.delete(requestId)
    clearTimeout(pending.timer)
    try {
      pending.resolve(JSON.parse(msg.content.toString("utf8")) as RabbitMqQueryReplyEnvelope)
    } catch (error) {
      pending.reject(error instanceof Error ? error : new Error(String(error)))
    }
  }

  private requireChannel(): Channel {
    if (!this.channel) throw new Error("RabbitMQ query transport is not connected")
    return this.channel
  }
}

function serializeError(error: unknown): RabbitMqQueryReplyEnvelope["error"] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack }
  }
  return { message: String(error) }
}
