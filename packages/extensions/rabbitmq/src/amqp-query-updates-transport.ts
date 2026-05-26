import type { Channel, ConsumeMessage } from "amqplib"
import type { AmqpConnection } from "./connection.js"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

/**
 * Wire envelope for subscription-query update broadcasts.
 *
 * `kind: "update"` carries a payload to deliver to all matching local subscribers.
 * `kind: "complete"` / `"completeExceptionally"` signal end-of-stream for all
 * subscribers of the given query name on every process.
 *
 * `senderId` identifies the publishing instance so a process can drop its own
 * loopback messages (the local query bus has already fanned out locally).
 */
export interface RabbitMqQueryUpdateEnvelope {
  readonly kind: "update" | "complete" | "completeExceptionally"
  readonly senderId: string
  readonly queryName: string
  readonly update?: unknown
  readonly error?: {
    readonly name?: string
    readonly message: string
    readonly stack?: string
  }
  /**
   * Serialized form of a structured `payloadEquals` filter. When present,
   * receivers apply it against each local subscriber's stored query payload
   * before delivering. Function filters do not serialize and therefore arrive
   * without this field, causing the receiver to deliver to all local
   * subscribers of {@link queryName}.
   */
  readonly payloadEquals?: Record<string, unknown>
}

export interface RabbitMqQueryUpdatesTransport {
  /** Publish an update / complete envelope to every subscribed instance. */
  publish(envelope: RabbitMqQueryUpdateEnvelope): Promise<void>
  /** Bind this instance's queue to the routing key for the query name. Idempotent. */
  bindQueryName(queryName: string): Promise<void>
  /** Unbind this instance's queue from the routing key. Idempotent. */
  unbindQueryName(queryName: string): Promise<void>
  /** Set the in-process handler invoked when an inbound update arrives. */
  setHandler(handler: (envelope: RabbitMqQueryUpdateEnvelope) => void): void
  /** Stable identifier for this publisher; appears as `senderId` on outbound envelopes. */
  readonly senderId: string
}

/**
 * AMQP broadcast transport for subscription-query updates.
 *
 * Topology: a topic exchange (`<prefix>.query-updates`) and one exclusive
 * auto-delete queue per instance (`<prefix>.query-updates.<service>.<instance>`).
 * The bus dynamically binds the queue to a routing key per active query name.
 *
 * Consume mode is no-ack: updates are best-effort. Losing one update means a
 * subscriber will see a stale view until the next emit — same recovery model
 * as Axon Server when a broker round-trip is dropped.
 */
export class AmqpRabbitMqQueryUpdatesTransport implements RabbitMqQueryUpdatesTransport {
  private channel: Channel | undefined
  private connectPromise: Promise<void> | undefined
  private closed = false
  private handler: ((envelope: RabbitMqQueryUpdateEnvelope) => void) | undefined
  private readonly boundKeys = new Set<string>()

  readonly senderId: string

  constructor(
    private readonly config: RabbitMqResolvedConfig,
    private readonly connection: AmqpConnection,
  ) {
    this.senderId = `${config.identity.serviceName}.${config.identity.instanceId}`
  }

  async connect(): Promise<void> {
    if (this.connectPromise) return this.connectPromise
    this.connectPromise = this.doConnect()
    return this.connectPromise
  }

  private async doConnect(): Promise<void> {
    this.channel = await this.connection.channel()

    await this.channel.assertExchange(this.config.topology.queryUpdatesExchange, "topic", {
      durable: true,
    })

    await this.channel.assertQueue(this.config.topology.queryUpdatesQueue(), {
      durable: false,
      exclusive: true,
      autoDelete: true,
    })

    await this.channel.consume(
      this.config.topology.queryUpdatesQueue(),
      (msg) => this.handleInbound(msg),
      { noAck: true },
    )

    // Rebind any routing keys requested before connect resolved.
    for (const key of this.boundKeys) {
      await this.channel.bindQueue(
        this.config.topology.queryUpdatesQueue(),
        this.config.topology.queryUpdatesExchange,
        key,
      )
    }
  }

  async close(): Promise<void> {
    this.closed = true
    await this.channel?.close().catch(() => {})
  }

  async publish(envelope: RabbitMqQueryUpdateEnvelope): Promise<void> {
    await this.connect()
    if (this.closed) return
    const channel = this.requireChannel()
    const routingKey = this.config.topology.queryUpdatesRoutingKey(envelope.queryName)
    channel.publish(
      this.config.topology.queryUpdatesExchange,
      routingKey,
      Buffer.from(JSON.stringify(envelope)),
      { contentType: "application/json", persistent: false },
    )
  }

  async bindQueryName(queryName: string): Promise<void> {
    const routingKey = this.config.topology.queryUpdatesRoutingKey(queryName)
    if (this.boundKeys.has(routingKey)) return
    this.boundKeys.add(routingKey)
    await this.connect()
    if (this.closed) return
    const channel = this.requireChannel()
    await channel.bindQueue(
      this.config.topology.queryUpdatesQueue(),
      this.config.topology.queryUpdatesExchange,
      routingKey,
    )
  }

  async unbindQueryName(queryName: string): Promise<void> {
    const routingKey = this.config.topology.queryUpdatesRoutingKey(queryName)
    if (!this.boundKeys.has(routingKey)) return
    this.boundKeys.delete(routingKey)
    if (this.closed) return
    const channel = this.channel
    if (!channel) return
    await channel.unbindQueue(
      this.config.topology.queryUpdatesQueue(),
      this.config.topology.queryUpdatesExchange,
      routingKey,
    )
  }

  setHandler(handler: (envelope: RabbitMqQueryUpdateEnvelope) => void): void {
    this.handler = handler
  }

  private handleInbound(msg: ConsumeMessage | null): void {
    if (!msg) return
    if (!this.handler) return
    try {
      const envelope = JSON.parse(msg.content.toString("utf8")) as RabbitMqQueryUpdateEnvelope
      this.handler(envelope)
    } catch {
      // Malformed envelopes are dropped — broadcast is best-effort.
    }
  }

  private requireChannel(): Channel {
    if (!this.channel) throw new Error("RabbitMQ query-updates transport is not connected")
    return this.channel
  }
}
