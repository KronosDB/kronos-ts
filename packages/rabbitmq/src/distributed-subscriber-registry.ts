import type { Channel, ConsumeMessage } from "amqplib"
import type { SerializedError } from "@kronos-ts/core"
import type { AmqpChannelSource } from "./connection.js"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

/**
 * A subscription known to the cluster, as {@link rabbitMqQueryBus} needs to see
 * it. This vocabulary used to live in core behind a `SubscriptionRegistry`
 * seam; it is here now because the AMQP gossip mirror is the only thing that
 * ever implemented it.
 */
export type ClusterSubscriberRecord = {
  readonly subId: string
  readonly queryName: string
  /** The subscribing query's payload — what a `SubscriptionFilter` matches on. */
  readonly payload: unknown
}

/** One targeted change to a single subscription's update stream. */
export type SubscriptionDelivery =
  | { readonly kind: "update"; readonly subId: string; readonly update: unknown }
  | { readonly kind: "complete"; readonly subId: string }
  | {
      readonly kind: "completeExceptionally"
      readonly subId: string
      readonly error: SerializedError
    }

/**
 * A subscription known to the cluster — either owned locally on this instance
 * or owned by another instance and learned over gossip.
 *
 * The query bus only ever reads the {@link ClusterSubscriberRecord} half;
 * `ownerInstanceId` is how THIS transport knows where to send a delivery.
 */
export type SubscriberRecord = ClusterSubscriberRecord & {
  readonly ownerInstanceId: string
}

/**
 * Targeted message delivered to the owner of a specific subscription. The
 * owner applies it locally against the buffered subscriber stream.
 */
export type DeliverEnvelope = SubscriptionDelivery

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

/**
 * The cluster-wide subscription mirror {@link rabbitMqQueryBus} emits against.
 *
 * Mirror-first rather than broadcast-first, on purpose: every instance can
 * enumerate EVERY cluster subscription along with its payload, so the query bus
 * applies the subscription filter LOCALLY — which is what lets a plain function
 * predicate work across instances, since it never has to cross the wire — and
 * then routes each matching delivery to the owning instance.
 */
export type DistributedSubscriberRegistry = {
  /** Stable identifier for this process. */
  readonly instanceId: string

  /** Register a locally-owned subscriber and announce it to the cluster. */
  claim(record: ClusterSubscriberRecord): Promise<void>

  /** Drop a locally-owned subscriber and announce the release. */
  release(subId: string): Promise<void>

  /** Iterate every record in the cluster-wide mirror (locally owned + remote). */
  records(): IterableIterator<SubscriberRecord>

  /** Route one delivery to whichever instance owns `subId`. */
  deliver(delivery: SubscriptionDelivery): Promise<void>

  /** Install the callback invoked when a delivery for a locally-owned sub arrives. */
  setDeliverHandler(handler: (delivery: SubscriptionDelivery) => void): void

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
export function amqpDistributedSubscriberRegistry(
  config: RabbitMqResolvedConfig,
  connection: AmqpChannelSource,
): DistributedSubscriberRegistry {
  const instanceId = `${config.identity.serviceName}.${config.identity.instanceId}`

  let channel: Channel | undefined
  let connectPromise: Promise<void> | undefined
  let closed = false
  let deliverHandler: ((envelope: DeliverEnvelope) => void) | undefined
  const mirror = new Map<string, SubscriberRecord>()
  const locallyOwnedSubIds = new Set<string>()

  const requireChannel = (): Channel => {
    if (!channel) throw new Error("Distributed subscriber registry is not connected")
    return channel
  }

  const publishGossip = (envelope: GossipEnvelope): void => {
    if (closed) return
    const ch = channel
    if (!ch) return
    ch.publish(
      config.topology.subscribersGossipExchange,
      "",
      Buffer.from(JSON.stringify(envelope)),
      { contentType: "application/json", persistent: false },
    )
  }

  const handleGossip = (msg: ConsumeMessage | null): void => {
    if (!msg) return
    let envelope: GossipEnvelope
    try {
      envelope = JSON.parse(msg.content.toString("utf8")) as GossipEnvelope
    } catch {
      return
    }

    if (envelope.kind === "claim") {
      // Loopback — local mirror already updated synchronously by claim().
      if (envelope.ownerInstanceId === instanceId) return
      mirror.set(envelope.subId, {
        subId: envelope.subId,
        queryName: envelope.queryName,
        payload: envelope.payload,
        ownerInstanceId: envelope.ownerInstanceId,
      })
    } else if (envelope.kind === "release") {
      if (envelope.ownerInstanceId === instanceId) return
      mirror.delete(envelope.subId)
    } else if (envelope.kind === "syncRequest") {
      // Skip our own announcement; respond to every other instance by
      // re-broadcasting our owned claims over the same fanout exchange.
      if (envelope.requesterId === instanceId) return
      for (const subId of locallyOwnedSubIds) {
        const record = mirror.get(subId)
        if (!record) continue
        publishGossip({
          kind: "claim",
          ownerInstanceId: instanceId,
          subId: record.subId,
          queryName: record.queryName,
          payload: record.payload,
        })
      }
    }
  }

  const handleDirect = (msg: ConsumeMessage | null): void => {
    if (!msg) return
    if (!deliverHandler) return
    let envelope: DeliverEnvelope
    try {
      envelope = JSON.parse(msg.content.toString("utf8")) as DeliverEnvelope
    } catch {
      return
    }
    deliverHandler(envelope)
  }

  const doConnect = async (): Promise<void> => {
    channel = await connection.channel()
    const ch = channel

    await ch.assertExchange(config.topology.subscribersGossipExchange, "fanout", {
      durable: true,
    })
    await ch.assertExchange(config.topology.subscribersDirectExchange, "direct", {
      durable: true,
    })

    const gossipQueue = config.topology.subscribersGossipQueue()
    const directQueue = config.topology.subscribersDirectQueue()

    await ch.assertQueue(gossipQueue, { durable: false, exclusive: true, autoDelete: true })
    await ch.assertQueue(directQueue, { durable: false, exclusive: true, autoDelete: true })

    await ch.bindQueue(gossipQueue, config.topology.subscribersGossipExchange, "")
    await ch.bindQueue(directQueue, config.topology.subscribersDirectExchange, instanceId)

    await ch.consume(gossipQueue, (msg) => handleGossip(msg), { noAck: true })
    await ch.consume(directQueue, (msg) => handleDirect(msg), { noAck: true })

    // Announce ourselves so existing peers re-broadcast their owned claims.
    publishGossip({ kind: "syncRequest", requesterId: instanceId })
  }

  const connect = async (): Promise<void> => {
    if (connectPromise) return connectPromise
    connectPromise = doConnect()
    return connectPromise
  }

  return {
    instanceId,

    connect,

    async close() {
      closed = true
      await channel?.close().catch(() => {})
    },

    async claim(record: ClusterSubscriberRecord): Promise<void> {
      const full: SubscriberRecord = { ...record, ownerInstanceId: instanceId }
      mirror.set(full.subId, full)
      locallyOwnedSubIds.add(full.subId)
      await connect()
      if (closed) return
      publishGossip({
        kind: "claim",
        ownerInstanceId: instanceId,
        subId: full.subId,
        queryName: full.queryName,
        payload: full.payload,
      })
    },

    async release(subId: string): Promise<void> {
      mirror.delete(subId)
      locallyOwnedSubIds.delete(subId)
      await connect()
      if (closed) return
      publishGossip({ kind: "release", ownerInstanceId: instanceId, subId })
    },

    *records(): IterableIterator<SubscriberRecord> {
      for (const record of mirror.values()) yield record
    },

    async deliver(envelope: DeliverEnvelope): Promise<void> {
      const record = mirror.get(envelope.subId)
      if (!record) return

      if (record.ownerInstanceId === instanceId) {
        deliverHandler?.(envelope)
        return
      }

      await connect()
      if (closed) return
      const ch = requireChannel()
      ch.publish(
        config.topology.subscribersDirectExchange,
        record.ownerInstanceId,
        Buffer.from(JSON.stringify(envelope)),
        { contentType: "application/json", persistent: false },
      )
    },

    setDeliverHandler(handler: (envelope: DeliverEnvelope) => void): void {
      deliverHandler = handler
    },
  }
}
