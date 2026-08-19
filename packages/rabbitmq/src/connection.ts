import type { Channel, ChannelModel } from "amqplib"
import {
  resolveRabbitMqConfig,
  type RabbitMqResolvedConfig,
  type RabbitMqRetryConfig,
} from "./rabbitmq.js"
import type { RabbitMqTopologyConfig } from "./topology.js"
import type { RabbitMqCommandTransport } from "./command-bus.js"
import type { RabbitMqQueryTransport } from "./query-bus.js"
import type { DistributedSubscriberRegistry } from "./distributed-subscriber-registry.js"
import { AmqpRabbitMqCommandTransport } from "./amqp-command-transport.js"
import { AmqpRabbitMqQueryTransport } from "./amqp-query-transport.js"
import { AmqpDistributedSubscriberRegistry } from "./distributed-subscriber-registry.js"

/** Establishes a raw AMQP connection. Swapped for a fake in tests. */
export type AmqpConnect = (url: string) => Promise<ChannelModel>

/**
 * The raw socket multiplexer: one lazily-established AMQP connection that hands
 * out channels. This is what a transport borrows — it never owns the socket.
 */
export interface AmqpChannelSource {
  /**
   * Open a fresh channel on the (lazily established) connection. Each transport
   * takes its own channel so prefetch and consumer state stay isolated.
   */
  channel(): Promise<Channel>
  /** Close the underlying connection. Idempotent — safe to call more than once. */
  close(): Promise<void>
}

async function defaultAmqpConnect(url: string): Promise<ChannelModel> {
  const amqp = (await import("amqplib")) as { connect(url: string): Promise<ChannelModel> }
  return amqp.connect(url)
}

/**
 * Create a shared AMQP channel source. The socket opens on the first
 * `channel()` call and is reused for every channel thereafter.
 *
 * Ownership: whoever calls this owns the socket and is responsible for calling
 * `close()`. Transports borrow channels and only close their own channels.
 * {@link rabbitMqConnection} is the normal caller; this is exported for tests
 * that drive a transport directly against a fake broker.
 */
export function amqpChannelSource(
  url: string,
  connect: AmqpConnect = defaultAmqpConnect,
): AmqpChannelSource {
  let connection: Promise<ChannelModel> | undefined

  return {
    async channel() {
      connection ??= connect(url)
      return (await connection).createChannel()
    },
    async close() {
      const pending = connection
      connection = undefined
      if (pending) await (await pending).close().catch(() => {})
    },
  }
}

/** Identity plus the topology/retry knobs that shape what gets declared on the broker. */
export interface RabbitMqConnectionOptions {
  /** Shared by every replica of the same deployment. */
  readonly serviceName: string
  /** Unique per running process — reply and gossip queue names are derived from it. */
  readonly instanceId: string
  readonly topology?: RabbitMqTopologyConfig
  readonly retry?: RabbitMqRetryConfig
  /** Swap the raw AMQP dial-out. Defaults to `amqplib.connect`; fakeable in tests. */
  readonly amqpConnect?: AmqpConnect
}

/**
 * The RESOURCE a RabbitMQ deployment shares: one broker connection, the three
 * channels layered on it (commands, queries, subscription-update gossip), and
 * the lifecycle that turns consumers on and shuts them down.
 *
 * The transports are on it because they ARE the connection's channels — they
 * cannot outlive it and there is exactly one of each per connection. The BUSES
 * are not: `rabbitMqCommandBus(rabbit, local)` and
 * `rabbitMqQueryBus(rabbit, local)` are plain functions over this, and a caller
 * who wants only commands builds only that one.
 */
export interface RabbitMqConnection extends AmqpChannelSource {
  /** Config after defaults — the topology names in use, handy for diagnostics. */
  readonly config: RabbitMqResolvedConfig
  readonly commandTransport: RabbitMqCommandTransport
  readonly queryTransport: RabbitMqQueryTransport
  readonly subscriberRegistry: DistributedSubscriberRegistry
  /**
   * Resolve once every handler subscribed so far is actually bound and
   * consuming on the broker. Call it after handlers have been registered;
   * until it resolves, a command routed to this process can land on a queue
   * nobody consumes yet.
   */
  start(): Promise<void>
  /** Close the three channels and then the shared socket. */
  close(): Promise<void>
}

/**
 * Open a RabbitMQ connection and declare this process's topology on it.
 *
 * ```ts
 * const rabbit = await rabbitMqConnection("amqp://localhost", {
 *   serviceName: "billing",
 *   instanceId: process.env.POD_NAME!,
 * })
 * const commandBus = interceptingCommandBus(
 *   rabbitMqCommandBus(rabbit, simpleCommandBus(unitOfWork)), lineage)
 * const queryBus = interceptingQueryBus(
 *   rabbitMqQueryBus(rabbit, simpleQueryBus(unitOfWork)), lineage)
 *
 * const app = kronos({ commandHandlers, queryHandlers })
 * await rabbit.start()      // handlers are registered — bind and consume
 * // …
 * await app.stop(); await rabbit.close()
 * ```
 *
 * ASYNC ON PURPOSE. The awaited connect is what puts the exchanges and this
 * process's reply/gossip queues on the broker BEFORE any handler subscribes.
 * A handler's queue is only consumed once that handler binds it, so nothing can
 * be delivered to a handler that is not there yet; `start()` is the join point
 * that tells you every bind has landed.
 */
export async function rabbitMqConnection(
  url: string,
  options: RabbitMqConnectionOptions,
): Promise<RabbitMqConnection> {
  const { serviceName, instanceId, amqpConnect, ...rest } = options
  const config = resolveRabbitMqConfig({
    url,
    identity: { serviceName, instanceId },
    ...rest,
  })

  const channels = amqpChannelSource(url, amqpConnect)
  const commandTransport = new AmqpRabbitMqCommandTransport(config, channels)
  const queryTransport = new AmqpRabbitMqQueryTransport(config, channels)
  const subscriberRegistry = new AmqpDistributedSubscriberRegistry(config, channels)

  await Promise.all([
    commandTransport.connect(),
    queryTransport.connect(),
    subscriberRegistry.connect(),
  ])

  return {
    config,
    commandTransport,
    queryTransport,
    subscriberRegistry,
    channel: () => channels.channel(),
    async start() {
      await Promise.all([commandTransport.ready(), queryTransport.ready()])
    },
    async close() {
      await Promise.all([
        commandTransport.close(),
        queryTransport.close(),
        subscriberRegistry.close(),
      ])
      await channels.close()
    },
  }
}
