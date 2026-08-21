import type { Channel, ConsumeMessage } from "amqplib"
import type {
  RabbitMqQueryEnvelope,
  RabbitMqQueryReplyEnvelope,
  RabbitMqQueryTransport,
} from "./query-bus.js"
import type { AmqpChannelSource } from "./connection.js"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

type PendingRequest = {
  resolve(reply: RabbitMqQueryReplyEnvelope): void
  reject(error: Error): void
  timer: ReturnType<typeof setTimeout>
}

/**
 * The transport plus the lifecycle its owner drives — the query mirror of
 * {@link AmqpRabbitMqCommandTransport}.
 */
export type AmqpRabbitMqQueryTransport = RabbitMqQueryTransport & {
  connect(): Promise<void>
  /**
   * Resolve once the connection is up and every handler subscribed so far is
   * bound to its queue and consuming.
   */
  ready(): Promise<void>
  /**
   * Close this transport's channel and fail any in-flight requests. The shared
   * connection is owned by its creator (see {@link amqpChannelSource}) and is
   * not closed here.
   */
  close(): Promise<void>
}

/**
 * AMQP request/reply transport for distributed queries.
 *
 * Mirrors {@link amqpRabbitMqCommandTransport}: a per-process exclusive reply
 * queue, durable per-query handler queues bound to the shared queries exchange,
 * and correlation-id matched replies. Takes its own channel off the shared
 * {@link AmqpChannelSource}.
 */
export function amqpRabbitMqQueryTransport(
  config: RabbitMqResolvedConfig,
  connection: AmqpChannelSource,
): AmqpRabbitMqQueryTransport {
  let channel: Channel | undefined
  let replyQueue: string | undefined
  const handlers = new Map<string, (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>>()
  const boundHandlers = new Set<string>()
  const pending = new Map<string, PendingRequest>()
  const pendingBinds = new Set<Promise<void>>()
  let connectPromise: Promise<void> | undefined
  let closed = false

  const requireChannel = (): Channel => {
    if (!channel) throw new Error("RabbitMQ query transport is not connected")
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
      request.resolve(JSON.parse(msg.content.toString("utf8")) as RabbitMqQueryReplyEnvelope)
    } catch (error) {
      request.reject(error instanceof Error ? error : new Error(String(error)))
    }
  }

  const handleQuery = async (
    msg: ConsumeMessage | null,
    handler: (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>,
  ): Promise<void> => {
    if (!msg) return
    const ch = requireChannel()
    try {
      const envelope = JSON.parse(msg.content.toString("utf8")) as RabbitMqQueryEnvelope
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
        const reply: RabbitMqQueryReplyEnvelope = {
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

  const bindQueryHandler = async (
    queryName: string,
    handler: (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>,
  ): Promise<void> => {
    if (boundHandlers.has(queryName)) return
    const ch = requireChannel()
    const queue = config.topology.queryQueue(queryName)
    const routingKey = config.topology.queryRoutingKey(queryName)

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
    await ch.bindQueue(queue, config.topology.queriesExchange, routingKey)
    await ch.prefetch(1)
    await ch.consume(queue, (msg) => handleQuery(msg, handler), { noAck: false })
    boundHandlers.add(queryName)
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

    await channel.assertExchange(config.topology.queriesExchange, "topic", { durable: true })
    if (config.retry.deadLetter) {
      await channel.assertExchange(config.retry.deadLetterExchange, "topic", { durable: true })
    }

    const reply = await channel.assertQueue(config.topology.queryReplyQueue(), {
      durable: false,
      exclusive: true,
      autoDelete: true,
    })
    replyQueue = reply.queue
    await channel.consume(reply.queue, (msg) => handleReply(msg), { noAck: true })

    boundHandlers.clear()
    for (const [queryName, handler] of handlers) {
      await bindQueryHandler(queryName, handler)
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
        request.reject(new Error(`RabbitMQ query transport closed before reply for ${requestId}`))
      }
      pending.clear()
      await channel?.close().catch(() => {})
    },

    async dispatch(envelope: RabbitMqQueryEnvelope): Promise<RabbitMqQueryReplyEnvelope> {
      await connect()
      if (closed) throw new Error("RabbitMQ query transport is closed")
      const ch = requireChannel()
      const replyTo = replyQueue
      if (!replyTo) throw new Error("RabbitMQ reply queue is not initialized")

      const body = Buffer.from(JSON.stringify(envelope))
      const routingKey = config.topology.queryRoutingKey(envelope.message.name)

      return new Promise<RabbitMqQueryReplyEnvelope>((resolve, reject) => {
        const timer = setTimeout(() => {
          pending.delete(envelope.requestId)
          reject(new Error(`Query ${routingKey} timed out after ${envelope.timeoutMs}ms`))
        }, envelope.timeoutMs)
        pending.set(envelope.requestId, { resolve, reject, timer })

        ch.publish(
          config.topology.queriesExchange,
          routingKey,
          body,
          {
            contentType: "application/json",
            correlationId: envelope.requestId,
            replyTo,
            persistent: true,
          },
        )
      })
    },

    subscribe(
      queryName: string,
      handler: (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>,
    ): void {
      handlers.set(queryName, handler)
      if (channel) {
        trackBind(bindQueryHandler(queryName, handler))
      }
    },
  }
}

function serializeError(error: unknown): RabbitMqQueryReplyEnvelope["error"] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack }
  }
  return { message: String(error) }
}
