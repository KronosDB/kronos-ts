import type { Channel, ConsumeMessage } from "amqplib"
import type {
  RabbitMqCommandEnvelope,
  RabbitMqCommandReplyEnvelope,
  RabbitMqCommandTransport,
} from "./command-bus.js"
import type { AmqpConnection } from "./connection.js"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

interface PendingRequest {
  resolve(reply: RabbitMqCommandReplyEnvelope): void
  reject(error: Error): void
  timer: ReturnType<typeof setTimeout>
}

export class AmqpRabbitMqCommandTransport implements RabbitMqCommandTransport {
  private channel: Channel | undefined
  private replyQueue: string | undefined
  private readonly handlers = new Map<string, (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>>()
  private readonly boundHandlers = new Set<string>()
  private readonly pending = new Map<string, PendingRequest>()
  private readonly pendingBinds = new Set<Promise<void>>()
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

    await this.channel.assertExchange(this.config.topology.commandsExchange, "topic", { durable: true })
    if (this.config.retry.deadLetter) {
      await this.channel.assertExchange(this.config.retry.deadLetterExchange, "topic", { durable: true })
    }

    const reply = await this.channel.assertQueue(this.config.topology.commandReplyQueue(), {
      durable: false,
      exclusive: true,
      autoDelete: true,
    })
    this.replyQueue = reply.queue
    await this.channel.consume(reply.queue, (msg) => this.handleReply(msg), { noAck: true })

    this.boundHandlers.clear()
    for (const [commandName, handler] of this.handlers) {
      await this.bindCommandHandler(commandName, handler)
    }
  }

  /**
   * Resolve once the connection is up and every handler subscribed so far is
   * bound to its queue and consuming. `subscribe` is synchronous (the bus API
   * gives it nowhere to await), so binding runs in the background; this is the
   * join point for a caller that wants to know the process is really listening.
   */
  async ready(): Promise<void> {
    await this.connect()
    while (this.pendingBinds.size > 0) {
      await Promise.all([...this.pendingBinds])
    }
  }

  /**
   * Close this transport's channel and fail any in-flight requests. The shared
   * connection is owned by its creator (see {@link amqpConnection}) and is
   * not closed here.
   */
  async close(): Promise<void> {
    this.closed = true
    for (const [requestId, pending] of this.pending) {
      clearTimeout(pending.timer)
      pending.reject(new Error(`RabbitMQ command transport closed before reply for ${requestId}`))
    }
    this.pending.clear()
    await this.channel?.close().catch(() => {})
  }

  async dispatch(envelope: RabbitMqCommandEnvelope): Promise<RabbitMqCommandReplyEnvelope> {
    await this.connect()
    if (this.closed) throw new Error("RabbitMQ command transport is closed")
    const channel = this.requireChannel()
    const replyQueue = this.replyQueue
    if (!replyQueue) throw new Error("RabbitMQ reply queue is not initialized")

    const body = Buffer.from(JSON.stringify(envelope))
    const routingKey = this.config.topology.commandRoutingKey(envelope.message.name)

    return new Promise<RabbitMqCommandReplyEnvelope>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(envelope.requestId)
        reject(new Error(`Command ${routingKey} timed out after ${envelope.timeoutMs}ms`))
      }, envelope.timeoutMs)
      this.pending.set(envelope.requestId, { resolve, reject, timer })

      const published = channel.publish(
        this.config.topology.commandsExchange,
        routingKey,
        body,
        {
          contentType: "application/json",
          correlationId: envelope.requestId,
          replyTo: replyQueue,
          persistent: true,
        },
      )

      if (!published) {
        // Backpressure is acceptable here; amqplib queued the write. Publisher
        // confirms are a later hardening step.
      }
    })
  }

  subscribe(
    commandName: string,
    handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
  ): void {
    this.handlers.set(commandName, handler)
    if (this.channel) {
      this.trackBind(this.bindCommandHandler(commandName, handler))
    }
  }

  /** Keep a background bind awaitable by {@link ready} without leaving it unhandled. */
  private trackBind(bind: Promise<void>): void {
    this.pendingBinds.add(bind)
    const forget = () => {
      this.pendingBinds.delete(bind)
    }
    bind.then(forget, forget)
  }

  private async bindCommandHandler(
    commandName: string,
    handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
  ): Promise<void> {
    if (this.boundHandlers.has(commandName)) return
    const channel = this.requireChannel()
    const queue = this.config.topology.commandQueue(commandName)
    const routingKey = this.config.topology.commandRoutingKey(commandName)

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
    await channel.bindQueue(queue, this.config.topology.commandsExchange, routingKey)
    await channel.prefetch(1)
    await channel.consume(queue, (msg) => this.handleCommand(msg, handler), { noAck: false })
    this.boundHandlers.add(commandName)
  }

  private async handleCommand(
    msg: ConsumeMessage | null,
    handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
  ): Promise<void> {
    if (!msg) return
    const channel = this.requireChannel()
    try {
      const envelope = JSON.parse(msg.content.toString("utf8")) as RabbitMqCommandEnvelope
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
        const reply: RabbitMqCommandReplyEnvelope = {
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
      pending.resolve(JSON.parse(msg.content.toString("utf8")) as RabbitMqCommandReplyEnvelope)
    } catch (error) {
      pending.reject(error instanceof Error ? error : new Error(String(error)))
    }
  }

  private requireChannel(): Channel {
    if (!this.channel) throw new Error("RabbitMQ command transport is not connected")
    return this.channel
  }
}

function serializeError(error: unknown): RabbitMqCommandReplyEnvelope["error"] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack }
  }
  return { message: String(error) }
}
