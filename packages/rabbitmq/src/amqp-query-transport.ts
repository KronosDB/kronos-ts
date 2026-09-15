import { messagingAdmission, positiveInteger, withMessagingTimeout } from "@kronos-ts/core"
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
  let bindFailure: unknown
  let transportFailure: Error | undefined
  let writable = true
  let closePromise: Promise<void> | undefined
  const consumers = new Set<string>()
  const running = new Set<Promise<void>>()
  const maxHandlers = positiveInteger(config.limits?.maxConcurrentHandlers ?? 128, "maxConcurrentHandlers")
  // One delivery beyond handler capacity guarantees saturated parents' children
  // can be received and rejected rather than waiting for their parents forever.
  if (maxHandlers >= 65535) throw new RangeError("maxConcurrentHandlers must be below 65535 for AMQP prefetch")
  const admission = messagingAdmission("inbound handlers", maxHandlers, config.limits?.observe)
  const outboundAdmission = messagingAdmission("pending requests", config.limits?.maxPendingRequests ?? 1024, config.limits?.observe)

  function failTransport(error: Error) {
    transportFailure ??= error
    for (const request of pending.values()) {
      clearTimeout(request.timer)
      request.reject(transportFailure)
    }
    pending.clear()
  }


  const requireChannel = (): Channel => {
    if (!channel) throw new Error("RabbitMQ query transport is not connected")
    return channel
  }

  function sendReply(ch: Channel, queue: string, body: Buffer, properties: Parameters<Channel["sendToQueue"]>[2]) {
    if (!ch.sendToQueue(queue, body, properties)) {
      const error = new Error("RabbitMQ reply buffer full; delivery outcome is unknown")
      failTransport(error)
      void ch.close().catch(() => {})
      throw error
    }
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
    let activity: ReturnType<typeof admission.enter> | undefined
    try {
      if (closed) throw new Error("Messaging shutdown in progress")
      if (transportFailure) throw transportFailure
      activity = admission.enter()
      const envelope = JSON.parse(msg.content.toString("utf8")) as RabbitMqQueryEnvelope
      const reply = await handler(envelope)
      if (msg.properties.replyTo) {
        sendReply(ch,
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
      if (transportFailure) throw error
      const requestId = msg.properties.correlationId
      if (msg.properties.replyTo && requestId) {
        const reply: RabbitMqQueryReplyEnvelope = {
          requestId,
          ok: false,
          error: serializeError(error),
        }
        sendReply(ch,
          msg.properties.replyTo,
          Buffer.from(JSON.stringify(reply)),
          {
            contentType: "application/json",
            correlationId: requestId,
          },
        )
      }
      ch.nack(msg, false, false)
    } finally {
      activity?.end()
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
    const consumer = await ch.consume(queue, (msg) => {
      const task = handleQuery(msg, handler).catch((error) => {
        if (!closed && !transportFailure) console.error("RabbitMQ query transport: handling failed", error)
      })
      running.add(task)
      void task.finally(() => running.delete(task))
    }, { noAck: false })
    if (consumer.consumerTag) consumers.add(consumer.consumerTag)
    boundHandlers.add(queryName)
  }

  /** Keep a background bind awaitable by `ready` without leaving it unhandled. */
  const trackBind = (bind: Promise<void>): void => {
    pendingBinds.add(bind)
    const forget = () => {
      pendingBinds.delete(bind)
    }
    bind.then(forget, (error) => {
      bindFailure = error
      forget()
    })
  }

  const doConnect = async (): Promise<void> => {
    channel = await connection.channel()
    channel.on?.("error", (error: Error) => failTransport(error))
    channel.on?.("close", () => failTransport(new Error("RabbitMQ channel closed; outstanding delivery outcome is unknown. Recreate the connection.")))
    channel.on?.("drain", () => { writable = true })
    channel.on?.("return", (msg: ConsumeMessage) => {
      const requestId = msg.properties.correlationId
      const request = pending.get(requestId)
      if (!request) return
      pending.delete(requestId)
      clearTimeout(request.timer)
      request.reject(new Error("No RabbitMQ queue is bound for this request"))
    })
    await channel.prefetch(maxHandlers + 1, true)

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
    if (closed) throw new Error("RabbitMQ query transport is closed")
    if (transportFailure) throw transportFailure
    if (connectPromise) return connectPromise
    connectPromise = doConnect()
    return connectPromise
  }

  return {
    connect,

    async ready() {
      await connect()
      if (bindFailure) throw bindFailure
      while (pendingBinds.size > 0) {
        await Promise.all([...pendingBinds])
      }
      if (bindFailure) throw bindFailure
    },

    close() {
      if (closePromise) return closePromise
      closed = true
      closePromise = (async () => {
        try {
          await withMessagingTimeout((async () => {
            await Promise.all([...consumers].map(async (tag) => {
              try { await channel!.cancel(tag) }
              catch (error) { if (!transportFailure) throw error }
            }))
            // Reject outbound waits so a running parent can unwind during drain.
            for (const request of pending.values()) {
              clearTimeout(request.timer)
              request.reject(new Error("RabbitMQ query transport closed before reply"))
            }
            pending.clear()
            await Promise.all([...running])
          })(), config.shutdownTimeoutMs ?? 30000, "RabbitMQ query shutdown")
        } finally {
          failTransport(new Error("RabbitMQ query transport is closed"))
          await channel?.close().catch(() => {})
        }
      })()
      return closePromise
    },

    async dispatch(envelope: RabbitMqQueryEnvelope): Promise<RabbitMqQueryReplyEnvelope> {
      positiveInteger(envelope.timeoutMs, "timeoutMs")
      const expiresAt = Date.now() + envelope.timeoutMs
      const activity = outboundAdmission.enter(envelope.requestId)
      try {
        await withMessagingTimeout(connect(), envelope.timeoutMs, "RabbitMQ connect")
        if (closed) throw new Error("RabbitMQ query transport is closed")
        const ch = requireChannel()
        const replyTo = replyQueue
        if (!replyTo) throw new Error("RabbitMQ reply queue is not initialized")

        if (pending.has(envelope.requestId)) {
          throw new Error(`Request ${envelope.requestId} is already pending`)
        }
        if (!writable) throw new Error("RabbitMQ publish buffer is full; retry after drain")
        const body = Buffer.from(JSON.stringify(envelope))

        const routingKey = config.topology.queryRoutingKey(envelope.message.name)

        return await new Promise<RabbitMqQueryReplyEnvelope>((resolve, reject) => {
          const timer = setTimeout(() => {
            pending.delete(envelope.requestId)
            reject(new Error(`Query ${routingKey} timed out after ${envelope.timeoutMs}ms`))
          }, Math.max(1, expiresAt - Date.now()))
          pending.set(envelope.requestId, { resolve, reject, timer })

          try {
            writable = ch.publish(
              config.topology.queriesExchange,
              routingKey,
              body,
              {
                contentType: "application/json",
                correlationId: envelope.requestId,
                replyTo,
                persistent: true,
                mandatory: true,
                expiration: String(envelope.timeoutMs),
              },
            )
          } catch (error) {
            clearTimeout(timer)
            pending.delete(envelope.requestId)
            reject(error)
          }
        })
      } finally {
        activity.end()
      }
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
