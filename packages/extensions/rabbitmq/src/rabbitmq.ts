import type { App, KronosIdentity } from "@kronos-ts/app"
import { createRabbitMqTopologyNames, type RabbitMqTopologyConfig } from "./topology.js"
import { createRabbitMqCommandBus } from "./command-bus.js"
import { createRabbitMqQueryBus } from "./query-bus.js"
import { AmqpRabbitMqCommandTransport } from "./amqp-command-transport.js"
import { AmqpRabbitMqQueryTransport } from "./amqp-query-transport.js"
import { createAmqpConnection } from "./connection.js"

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

export interface RabbitMqExtensionConfig {
  readonly url: string
  readonly topology?: RabbitMqTopologyConfig
  readonly commands?: RabbitMqCommandDispatchConfig
  readonly queries?: RabbitMqQueryDispatchConfig
  readonly retry?: RabbitMqRetryConfig
}

export interface RabbitMqResolvedConfig {
  readonly identity: KronosIdentity
  readonly url: string
  readonly topology: ReturnType<typeof createRabbitMqTopologyNames>
  readonly commands: Required<RabbitMqCommandDispatchConfig>
  readonly queries: Required<RabbitMqQueryDispatchConfig>
  readonly retry: Required<RabbitMqRetryConfig>
}

export function resolveRabbitMqConfig(app: App, config: RabbitMqExtensionConfig): RabbitMqResolvedConfig {
  return {
    identity: app.identity,
    url: config.url,
    topology: createRabbitMqTopologyNames(app.identity, config.topology),
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

/**
 * RabbitMQ distributed messaging extension.
 *
 * Wraps the command and query buses with RabbitMQ-backed request/reply
 * transports. Both transports share a single broker connection, each taking
 * its own channel. Subscription queries remain process-local — distributing
 * their update streams is out of scope for this version.
 */
export function rabbitMq(config: RabbitMqExtensionConfig): (app: App) => void {
  return (app) => {
    const resolved = resolveRabbitMqConfig(app, config)
    const connection = createAmqpConnection(resolved.url)
    const commandTransport = new AmqpRabbitMqCommandTransport(resolved, connection)
    const queryTransport = new AmqpRabbitMqQueryTransport(resolved, connection)

    app.decorate("commandBus", (localSegment) =>
      createRabbitMqCommandBus({
        localSegment,
        transport: commandTransport,
        config: resolved,
      }),
    )

    app.decorate("queryBus", (localSegment) =>
      createRabbitMqQueryBus({
        localSegment,
        transport: queryTransport,
        config: resolved,
      }),
    )

    app.onStart("connect", () => commandTransport.connect())
    app.onStart("connect", () => queryTransport.connect())
    app.onStop("connect", () => commandTransport.close())
    app.onStop("connect", () => queryTransport.close())
    app.onStop("connect", () => connection.close())
  }
}
