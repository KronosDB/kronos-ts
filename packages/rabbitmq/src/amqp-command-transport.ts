import type { Channel, ConsumeMessage } from "amqplib"
import type {
  RabbitMqCommandEnvelope,
  RabbitMqCommandReplyEnvelope,
  RabbitMqCommandTransport,
} from "./command-bus.js"
import type { AmqpChannelSource } from "./connection.js"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

type PendingRequest = {
  resolve(reply: RabbitMqCommandReplyEnvelope): void
  reject(error: Error): void
  timer: ReturnType<typeof setTimeout>
}

/**
 * The transport plus the lifecycle its owner drives. The bus seam
 * ({@link RabbitMqCommandTransport}) is only `dispatch`/`subscribe`; the
 * connection that minted the channel is what connects, joins and closes it.
 */
export type AmqpRabbitMqCommandTransport = RabbitMqCommandTransport & {
  connect(): Promise<void>
  /**
   * Resolve once the connection is up and every handler subscribed so far is
   * bound to its queue and consuming. `subscribe` is synchronous (the bus API
   * gives it nowhere to await), so binding runs in the background; this is the
   * join point for a caller that wants to know the process is really listening.
   */
  ready(): Promise<void>
  /**
   * Close this transport's channel and fail any in-flight requests. The shared
   * connection is owned by its creator (see {@link amqpChannelSource}) and is
   * not closed here.
   */
  close(): Promise<void>
}

export function amqpRabbitMqCommandTransport(
  config: RabbitMqResolvedConfig,
  connection: AmqpChannelSource,
): AmqpRabbitMqCommandTransport {
  let channel: Channel | undefined
  let replyQueue: string | undefined
  const handlers = new Map<string, (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>>()
  const boundHandlers = new Set<string>()
  const pending = new Map<string, PendingRequest>()
  const pendingBinds = new Set<Promise<void>>()
  let connectPromise: Promise<void> | undefined
  let closed = false

  const requireChannel = (): Channel => {
    if (!channel) throw new Error("RabbitMQ command transport is not connected")
    return channel
  }

  const handleReply = (msg: ConsumeMessage | null): void => {
    if (!msg) return
    const requestId = msg.properties.correlationId
    if (!requestId) return
    const request = pending.get(requestId)
    if (!request) return
    pending.delete(requestId)
    clearTimeout(request.timer)
    try {
      request.resolve(JSON.parse(msg.content.toString("utf8")) as RabbitMqCommandReplyEnvelope)
    } catch (error) {
      request.reject(error instanceof Error ? error : new Error(String(error)))
    }
  }

  const handleCommand = async (
    msg: ConsumeMessage | null,
    handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
  ): Promise<void> => {
    if (!msg) return
    const ch = requireChannel()
    try {
      const envelope = JSON.parse(msg.content.toString("utf8")) as RabbitMqCommandEnvelope
      const reply = await handler(envelope)
      if (msg.properties.replyTo) {
        ch.sendToQueue(
          msg.properties.replyTo,
          Buffer.from(JSON.stringify(reply)),
          {
            contentType: "application/json",
            correlationId: msg.properties.correlationId,
          },
        )
      }
      ch.ack(msg)
    } catch (error) {
      const requestId = msg.properties.correlationId
      if (msg.properties.replyTo && requestId) {
        const reply: RabbitMqCommandReplyEnvelope = {
          requestId,
          ok: false,
          error: serializeError(error),
        }
        ch.sendToQueue(
          msg.properties.replyTo,
          Buffer.from(JSON.stringify(reply)),
          {
            contentType: "application/json",
            correlationId: requestId,
          },
        )
      }
      ch.nack(msg, false, false)
    }
  }

  const bindCommandHandler = async (
    commandName: string,
    handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
  ): Promise<void> => {
    if (boundHandlers.has(commandName)) return
    const ch = requireChannel()
    const queue = config.topology.commandQueue(commandName)
    const routingKey = config.topology.commandRoutingKey(commandName)

    await ch.assertQueue(queue, {
      durable: true,
      exclusive: false,
      autoDelete: false,
      arguments: config.retry.deadLetter
        ? {
            "x-dead-letter-exchange": config.retry.deadLetterExchange,
            "x-dead-letter-routing-key": routingKey,
          }
        : undefined,
    })
    await ch.bindQueue(queue, config.topology.commandsExchange, routingKey)
    await ch.prefetch(1)
    await ch.consume(queue, (msg) => handleCommand(msg, handler), { noAck: false })
    boundHandlers.add(commandName)
  }

  /** Keep a background bind awaitable by `ready` without leaving it unhandled. */
  const trackBind = (bind: Promise<void>): void => {
    pendingBinds.add(bind)
    const forget = () => {
      pendingBinds.delete(bind)
    }
    bind.then(forget, forget)
  }

  const doConnect = async (): Promise<void> => {
    channel = await connection.channel()

    await channel.assertExchange(config.topology.commandsExchange, "topic", { durable: true })
    if (config.retry.deadLetter) {
      await channel.assertExchange(config.retry.deadLetterExchange, "topic", { durable: true })
    }

    const reply = await channel.assertQueue(config.topology.commandReplyQueue(), {
      durable: false,
      exclusive: true,
      autoDelete: true,
    })
    replyQueue = reply.queue
    await channel.consume(reply.queue, (msg) => handleReply(msg), { noAck: true })

    boundHandlers.clear()
    for (const [commandName, handler] of handlers) {
      await bindCommandHandler(commandName, handler)
    }
  }

  const connect = async (): Promise<void> => {
    if (connectPromise) return connectPromise
    connectPromise = doConnect()
    return connectPromise
  }

  return {
    connect,

    async ready() {
      await connect()
      while (pendingBinds.size > 0) {
        await Promise.all([...pendingBinds])
      }
    },

    async close() {
      closed = true
      for (const [requestId, request] of pending) {
        clearTimeout(request.timer)
        request.reject(new Error(`RabbitMQ command transport closed before reply for ${requestId}`))
      }
      pending.clear()
      await channel?.close().catch(() => {})
    },

    async dispatch(envelope: RabbitMqCommandEnvelope): Promise<RabbitMqCommandReplyEnvelope> {
      await connect()
      if (closed) throw new Error("RabbitMQ command transport is closed")
      const ch = requireChannel()
      const replyTo = replyQueue
      if (!replyTo) throw new Error("RabbitMQ reply queue is not initialized")

      const body = Buffer.from(JSON.stringify(envelope))
      const routingKey = config.topology.commandRoutingKey(envelope.message.name)

      return new Promise<RabbitMqCommandReplyEnvelope>((resolve, reject) => {
        const timer = setTimeout(() => {
          pending.delete(envelope.requestId)
          reject(new Error(`Command ${routingKey} timed out after ${envelope.timeoutMs}ms`))
        }, envelope.timeoutMs)
        pending.set(envelope.requestId, { resolve, reject, timer })

        const published = ch.publish(
          config.topology.commandsExchange,
          routingKey,
          body,
          {
            contentType: "application/json",
            correlationId: envelope.requestId,
            replyTo,
            persistent: true,
          },
        )

        if (!published) {
          // Backpressure is acceptable here; amqplib queued the write. Publisher
          // confirms are a later hardening step.
        }
      })
    },

    subscribe(
      commandName: string,
      handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
    ): void {
      handlers.set(commandName, handler)
      if (channel) {
        trackBind(bindCommandHandler(commandName, handler))
      }
    },
  }
}

function serializeError(error: unknown): RabbitMqCommandReplyEnvelope["error"] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack }
  }
  return { message: String(error) }
}
