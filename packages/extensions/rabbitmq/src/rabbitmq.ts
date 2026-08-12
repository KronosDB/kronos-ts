import type { CommandBus, QueryBus } from "@kronos-ts/messaging"
import {
  createRabbitMqTopologyNames,
  type RabbitMqIdentity,
  type RabbitMqTopologyConfig,
} from "./topology.js"
import { createRabbitMqCommandBus } from "./command-bus.js"
import { createRabbitMqQueryBus } from "./query-bus.js"
import { AmqpRabbitMqCommandTransport } from "./amqp-command-transport.js"
import { AmqpRabbitMqQueryTransport } from "./amqp-query-transport.js"
import { AmqpDistributedSubscriberRegistry } from "./distributed-subscriber-registry.js"
import { createAmqpConnection, type AmqpConnect } from "./connection.js"

export interface RabbitMqCommandDispatchConfig {
  /** Prefer local handlers when registered; otherwise route through RabbitMQ. Default: true. */
  readonly preferLocalHandlers?: boolean
  /** Force all command dispatch through RabbitMQ, even when a local handler exists. */
  readonly alwaysUseDistributedBus?: boolean
  /** Default request/reply timeout for command dispatch. Default: 30000. */
  readonly defaultTimeoutMs?: number
}

export interface RabbitMqQueryDispatchConfig {
  /** Prefer local handlers when registered; otherwise route through RabbitMQ. Default: true. */
  readonly preferLocalHandlers?: boolean
  /** Force all query dispatch through RabbitMQ, even when a local handler exists. */
  readonly alwaysUseDistributedBus?: boolean
  /** Default request/reply timeout for query dispatch. Default: 30000. */
  readonly defaultTimeoutMs?: number
}

export interface RabbitMqRetryConfig {
  /** Dead-letter failed command messages instead of silently dropping them. Default: true. */
  readonly deadLetter?: boolean
  /** Dead-letter exchange name. Default: <prefix>.dlx. */
  readonly deadLetterExchange?: string
}

export interface RabbitMqConfig {
  readonly url: string
  /** Who this process is on the broker — see {@link RabbitMqIdentity}. */
  readonly identity: RabbitMqIdentity
  readonly topology?: RabbitMqTopologyConfig
  readonly commands?: RabbitMqCommandDispatchConfig
  readonly queries?: RabbitMqQueryDispatchConfig
  readonly retry?: RabbitMqRetryConfig
}

export interface RabbitMqResolvedConfig {
  readonly identity: RabbitMqIdentity
  readonly url: string
  readonly topology: ReturnType<typeof createRabbitMqTopologyNames>
  readonly commands: Required<RabbitMqCommandDispatchConfig>
  readonly queries: Required<RabbitMqQueryDispatchConfig>
  readonly retry: Required<RabbitMqRetryConfig>
}

export function resolveRabbitMqConfig(config: RabbitMqConfig): RabbitMqResolvedConfig {
  return {
    identity: config.identity,
    url: config.url,
    topology: createRabbitMqTopologyNames(config.identity, config.topology),
    commands: {
      preferLocalHandlers: config.commands?.preferLocalHandlers ?? true,
      alwaysUseDistributedBus: config.commands?.alwaysUseDistributedBus ?? false,
      defaultTimeoutMs: config.commands?.defaultTimeoutMs ?? 30_000,
    },
    queries: {
      preferLocalHandlers: config.queries?.preferLocalHandlers ?? true,
      alwaysUseDistributedBus: config.queries?.alwaysUseDistributedBus ?? false,
      defaultTimeoutMs: config.queries?.defaultTimeoutMs ?? 30_000,
    },
    retry: {
      deadLetter: config.retry?.deadLetter ?? true,
      deadLetterExchange: config.retry?.deadLetterExchange ?? `${config.topology?.prefix ?? "kronos"}.dlx`,
    },
  }
}

/** The buses this backend contributes — spread over an app's components. */
export interface RabbitMqComponents {
  readonly commandBus: CommandBus
  readonly queryBus: QueryBus
}

/**
 * A RabbitMQ messaging backend.
 *
 * Same shape as the Postgres backend: an async factory that connects eagerly,
 * hands back the components it provides, and gives you a `start`/`close` pair to
 * call where your bootstrap says — the order is written down in your composition
 * root rather than encoded in framework stages.
 */
export interface RabbitMqBackend {
  readonly components: RabbitMqComponents
  /** Config after defaults — the topology names in use, handy for diagnostics. */
  readonly config: RabbitMqResolvedConfig
  /**
   * Resolve once every handler subscribed so far is actually bound and
   * consuming on the broker. Call it after `kronos` has registered handlers;
   * until it resolves, a command routed to this process can land on a queue
   * nobody consumes yet.
   */
  start(): Promise<void>
  /** Close both transports, the subscriber registry, and the shared connection. */
  close(): Promise<void>
}

export interface RabbitMqOptions extends RabbitMqConfig {
  /**
   * The in-process command bus to wrap. Commands with a local handler are
   * served from here (unless `commands.alwaysUseDistributedBus`); everything
   * else goes over the broker.
   */
  readonly localCommandBus: CommandBus
  /** The in-process query bus to wrap — same deal as `localCommandBus`. */
  readonly localQueryBus: QueryBus
  /** Swap the raw AMQP dial-out. Defaults to `amqplib.connect`; fakeable in tests. */
  readonly amqpConnect?: AmqpConnect
}

/**
 * RabbitMQ distributed messaging.
 *
 * Wraps the command and query buses with RabbitMQ-backed transports. Direct
 * request/reply commands and queries share one channel each; a third channel
 * hosts the subscription-query update broadcast (topic exchange plus an
 * exclusive per-instance queue). All three share a single broker connection.
 *
 * ```ts
 * const base   = inMemoryComponents()
 * const rabbit = await rabbitMq({
 *   url: "amqp://localhost",
 *   identity: { serviceName: "billing", instanceId: process.env.POD_NAME! },
 *   localCommandBus: base.commandBus,
 *   localQueryBus: base.queryBus,
 * })
 * const app = kronos({ components: { ...base, ...rabbit.components }, modules })
 * await rabbit.start()      // handlers are registered — bind and consume
 * // …
 * await app.stop(); await rabbit.close()
 * ```
 *
 * The local buses are arguments because the wrapped bus needs something to fall
 * back to; pass the ones you built for the app (`inMemoryComponents()` gives you
 * a pair) and put `rabbit.components` on top so handlers register against the
 * wrapper.
 */
export async function rabbitMq(options: RabbitMqOptions): Promise<RabbitMqBackend> {
  const { localCommandBus, localQueryBus, amqpConnect, ...config } = options
  const resolved = resolveRabbitMqConfig(config)

  const connection = createAmqpConnection(resolved.url, amqpConnect)
  const commandTransport = new AmqpRabbitMqCommandTransport(resolved, connection)
  const queryTransport = new AmqpRabbitMqQueryTransport(resolved, connection)
  const subscriberRegistry = new AmqpDistributedSubscriberRegistry(resolved, connection)

  const commandBus = createRabbitMqCommandBus({
    localSegment: localCommandBus,
    transport: commandTransport,
    config: resolved,
  })

  const queryBus = createRabbitMqQueryBus({
    localSegment: localQueryBus,
    transport: queryTransport,
    subscriberRegistry,
    config: resolved,
  })

  // Eager connect: exchanges and this process's reply/gossip queues exist before
  // any handler subscribes. No command queue is consumed until a handler binds
  // it, so nothing can be delivered to a handler that isn't there yet.
  await Promise.all([
    commandTransport.connect(),
    queryTransport.connect(),
    subscriberRegistry.connect(),
  ])

  return {
    components: { commandBus, queryBus },
    config: resolved,
    async start() {
      await Promise.all([commandTransport.ready(), queryTransport.ready()])
    },
    async close() {
      await Promise.all([
        commandTransport.close(),
        queryTransport.close(),
        subscriberRegistry.close(),
      ])
      await connection.close()
    },
  }
}
