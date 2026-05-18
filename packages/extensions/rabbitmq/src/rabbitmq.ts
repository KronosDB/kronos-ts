import type { App, KronosIdentity } from "@kronos-ts/core"
import { createRabbitMqTopologyNames, type RabbitMqTopologyConfig } from "./topology.js"
import { createRabbitMqCommandBus } from "./command-bus.js"
import { AmqpRabbitMqCommandTransport } from "./amqp-command-transport.js"

export interface RabbitMqCommandDispatchConfig {
  /** Prefer local handlers when registered; otherwise route through RabbitMQ. Default: true. */
  readonly preferLocalHandlers?: boolean
  /** Force all command dispatch through RabbitMQ, even when a local handler exists. */
  readonly alwaysUseDistributedBus?: boolean
  /** Default request/reply timeout for command dispatch. Default: 30000. */
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
  readonly retry?: RabbitMqRetryConfig
}

export interface RabbitMqResolvedConfig {
  readonly identity: KronosIdentity
  readonly url: string
  readonly topology: ReturnType<typeof createRabbitMqTopologyNames>
  readonly commands: Required<RabbitMqCommandDispatchConfig>
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
    retry: {
      deadLetter: config.retry?.deadLetter ?? true,
      deadLetterExchange: config.retry?.deadLetterExchange ?? `${config.topology?.prefix ?? "kronos"}.dlx`,
    },
  }
}

/**
 * RabbitMQ distributed messaging extension.
 *
 * Command transport implementation is intentionally staged. The first committed
 * surface resolves app-level identity/topology and reserves the extension entry
 * point; the next step wires the command bus decorator around this config.
 */
export function rabbitMq(config: RabbitMqExtensionConfig): (app: App) => void {
  return (app) => {
    const resolved = resolveRabbitMqConfig(app, config)
    const commandTransport = new AmqpRabbitMqCommandTransport(resolved)

    app.decorate("commandBus", (localSegment) =>
      createRabbitMqCommandBus({
        localSegment,
        transport: commandTransport,
        config: resolved,
      }),
    )

    app.onStart("connect", () => commandTransport.connect())
    app.onStop("connect", () => commandTransport.close())
  }
}
