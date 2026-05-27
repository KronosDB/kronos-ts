import type { Channel, ConsumeMessage } from "amqplib"
import type { AmqpConnection } from "./connection.js"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

/**
 * A subscription known to the cluster — either owned locally on this instance
 * or owned by another instance and learned over gossip.
 */
export interface SubscriberRecord {
  readonly subId: string
  readonly queryName: string
  readonly payload: unknown
  readonly ownerInstanceId: string
}

export interface SerializedError {
  readonly name?: string
  readonly message: string
  readonly stack?: string
}

/**
 * Targeted message delivered to the owner of a specific subscription. The
 * owner applies it locally against the buffered subscriber stream.
 */
export type DeliverEnvelope =
  | { readonly kind: "update"; readonly subId: string; readonly update: unknown }
  | { readonly kind: "complete"; readonly subId: string }
  | {
      readonly kind: "completeExceptionally"
      readonly subId: string
      readonly error: SerializedError
    }

/**
 * Gossip envelopes broadcast to every instance over the fanout exchange.
 *
 * `claim`/`release` keep the cluster-wide subscriber mirror in sync. `sync`
 * announces a fresh instance and prompts peers to re-emit their owned claims
 * so the joiner can bootstrap its mirror.
 *
 * Every envelope carries the publisher's `ownerInstanceId` so receivers can
 * drop their own loopback (the local mirror was already updated synchronously
 * on the publish path).
 */
export type GossipEnvelope =
  | {
      readonly kind: "claim"
      readonly ownerInstanceId: string
      readonly subId: string
      readonly queryName: string
      readonly payload: unknown
    }
  | { readonly kind: "release"; readonly ownerInstanceId: string; readonly subId: string }
  | { readonly kind: "syncRequest"; readonly requesterId: string }

export interface DistributedSubscriberRegistry {
  /** Stable identifier for this process. */
  readonly instanceId: string

  /** Add (or overwrite) a locally-owned subscriber and broadcast the claim. */
  claim(record: Omit<SubscriberRecord, "ownerInstanceId">): Promise<void>

  /** Remove a locally-owned subscriber and broadcast the release. */
  release(subId: string): Promise<void>

  /** Iterate every record in the cluster-wide mirror (locally owned + remote). */
  records(): IterableIterator<SubscriberRecord>

  /**
   * Route a delivery to the owner of `subId`. Local owners are dispatched
   * synchronously to the in-process handler; remote owners receive a direct-
   * queue publish keyed by their instanceId.
   */
  deliver(envelope: DeliverEnvelope): Promise<void>

  /** Set the handler invoked when a `DeliverEnvelope` for a local sub arrives. */
  setDeliverHandler(handler: (envelope: DeliverEnvelope) => void): void

  connect(): Promise<void>
  close(): Promise<void>
}

/**
 * AMQP-backed implementation.
 *
 * Topology:
 *
 *   - `<prefix>.subscribers.gossip` — fanout exchange. Every instance owns an
 *     exclusive auto-delete queue bound to it. Carries claim / release /
 *     syncRequest messages.
 *
 *   - `<prefix>.subscribers.direct` — direct exchange. Every instance owns an
 *     exclusive auto-delete queue bound by routing key equal to its
 *     `instanceId`. Carries DeliverEnvelope messages targeted at the owner.
 *
 * Consume mode is no-ack on both queues — the registry is a best-effort eventual
 * mirror. A dropped claim is healed by the next sync request; a dropped deliver
 * looks like a missed update, the same failure mode the broker-routed model has
 * when a stream segment is lost.
 *
 * Loopback dedup: the publisher applies its own claim/release to the local
 * mirror synchronously before publishing, and ignores its own envelopes on the
 * inbound side via `ownerInstanceId === instanceId`.
 *
 * Joiner protocol: on connect the new instance publishes a `syncRequest`.
 * Existing peers respond by re-broadcasting each of their owned claims over
 * the same fanout exchange. New instance fills its mirror as they arrive; in
 * the meantime emits routed to subs the joiner has yet to learn about are
 * dropped on the joiner side — by design, since the joiner can't have any of
 * its own subscribers yet either. The window self-closes as the mirror fills.
 */
export class AmqpDistributedSubscriberRegistry implements DistributedSubscriberRegistry {
  private channel: Channel | undefined
  private connectPromise: Promise<void> | undefined
  private closed = false
  private deliverHandler: ((envelope: DeliverEnvelope) => void) | undefined
  private readonly mirror = new Map<string, SubscriberRecord>()
  private readonly locallyOwnedSubIds = new Set<string>()

  readonly instanceId: string

  constructor(
    private readonly config: RabbitMqResolvedConfig,
    private readonly connection: AmqpConnection,
  ) {
    this.instanceId = `${config.identity.serviceName}.${config.identity.instanceId}`
  }

  async connect(): Promise<void> {
    if (this.connectPromise) return this.connectPromise
    this.connectPromise = this.doConnect()
    return this.connectPromise
  }

  private async doConnect(): Promise<void> {
    this.channel = await this.connection.channel()
    const ch = this.channel

    await ch.assertExchange(this.config.topology.subscribersGossipExchange, "fanout", {
      durable: true,
    })
    await ch.assertExchange(this.config.topology.subscribersDirectExchange, "direct", {
      durable: true,
    })

    const gossipQueue = this.config.topology.subscribersGossipQueue()
    const directQueue = this.config.topology.subscribersDirectQueue()

    await ch.assertQueue(gossipQueue, { durable: false, exclusive: true, autoDelete: true })
    await ch.assertQueue(directQueue, { durable: false, exclusive: true, autoDelete: true })

    await ch.bindQueue(gossipQueue, this.config.topology.subscribersGossipExchange, "")
    await ch.bindQueue(directQueue, this.config.topology.subscribersDirectExchange, this.instanceId)

    await ch.consume(gossipQueue, (msg) => this.handleGossip(msg), { noAck: true })
    await ch.consume(directQueue, (msg) => this.handleDirect(msg), { noAck: true })

    // Announce ourselves so existing peers re-broadcast their owned claims.
    this.publishGossip({ kind: "syncRequest", requesterId: this.instanceId })
  }

  async close(): Promise<void> {
    this.closed = true
    await this.channel?.close().catch(() => {})
  }

  async claim(record: Omit<SubscriberRecord, "ownerInstanceId">): Promise<void> {
    const full: SubscriberRecord = { ...record, ownerInstanceId: this.instanceId }
    this.mirror.set(full.subId, full)
    this.locallyOwnedSubIds.add(full.subId)
    await this.connect()
    if (this.closed) return
    this.publishGossip({
      kind: "claim",
      ownerInstanceId: this.instanceId,
      subId: full.subId,
      queryName: full.queryName,
      payload: full.payload,
    })
  }

  async release(subId: string): Promise<void> {
    this.mirror.delete(subId)
    this.locallyOwnedSubIds.delete(subId)
    await this.connect()
    if (this.closed) return
    this.publishGossip({ kind: "release", ownerInstanceId: this.instanceId, subId })
  }

  *records(): IterableIterator<SubscriberRecord> {
    for (const record of this.mirror.values()) yield record
  }

  async deliver(envelope: DeliverEnvelope): Promise<void> {
    const record = this.mirror.get(envelope.subId)
    if (!record) return

    if (record.ownerInstanceId === this.instanceId) {
      this.deliverHandler?.(envelope)
      return
    }

    await this.connect()
    if (this.closed) return
    const ch = this.requireChannel()
    ch.publish(
      this.config.topology.subscribersDirectExchange,
      record.ownerInstanceId,
      Buffer.from(JSON.stringify(envelope)),
      { contentType: "application/json", persistent: false },
    )
  }

  setDeliverHandler(handler: (envelope: DeliverEnvelope) => void): void {
    this.deliverHandler = handler
  }

  private publishGossip(envelope: GossipEnvelope): void {
    if (this.closed) return
    const ch = this.channel
    if (!ch) return
    ch.publish(
      this.config.topology.subscribersGossipExchange,
      "",
      Buffer.from(JSON.stringify(envelope)),
      { contentType: "application/json", persistent: false },
    )
  }

  private handleGossip(msg: ConsumeMessage | null): void {
    if (!msg) return
    let envelope: GossipEnvelope
    try {
      envelope = JSON.parse(msg.content.toString("utf8")) as GossipEnvelope
    } catch {
      return
    }

    if (envelope.kind === "claim") {
      // Loopback — local mirror already updated synchronously by claim().
      if (envelope.ownerInstanceId === this.instanceId) return
      this.mirror.set(envelope.subId, {
        subId: envelope.subId,
        queryName: envelope.queryName,
        payload: envelope.payload,
        ownerInstanceId: envelope.ownerInstanceId,
      })
    } else if (envelope.kind === "release") {
      if (envelope.ownerInstanceId === this.instanceId) return
      this.mirror.delete(envelope.subId)
    } else if (envelope.kind === "syncRequest") {
      // Skip our own announcement; respond to every other instance by
      // re-broadcasting our owned claims over the same fanout exchange.
      if (envelope.requesterId === this.instanceId) return
      for (const subId of this.locallyOwnedSubIds) {
        const record = this.mirror.get(subId)
        if (!record) continue
        this.publishGossip({
          kind: "claim",
          ownerInstanceId: this.instanceId,
          subId: record.subId,
          queryName: record.queryName,
          payload: record.payload,
        })
      }
    }
  }

  private handleDirect(msg: ConsumeMessage | null): void {
    if (!msg) return
    if (!this.deliverHandler) return
    let envelope: DeliverEnvelope
    try {
      envelope = JSON.parse(msg.content.toString("utf8")) as DeliverEnvelope
    } catch {
      return
    }
    this.deliverHandler(envelope)
  }

  private requireChannel(): Channel {
    if (!this.channel) throw new Error("Distributed subscriber registry is not connected")
    return this.channel
  }
}
