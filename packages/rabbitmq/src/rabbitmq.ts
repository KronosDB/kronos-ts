import {
  rabbitMqTopologyNames,
  type RabbitMqIdentity,
  type RabbitMqTopologyConfig,
} from "./topology.js"

export type RabbitMqRetryConfig = {
  /** Dead-letter failed command messages instead of silently dropping them. Default: true. */
  readonly deadLetter?: boolean
  /** Dead-letter exchange name. Default: <prefix>.dlx. */
  readonly deadLetterExchange?: string
}

/**
 * What a connection declares on the broker. Reply TIMEOUTS are deliberately not
 * here: a timeout is a property of one dispatch, so it belongs to the bus that
 * makes the dispatch (`rabbitMqCommandBus(local, rabbit, { timeoutMs })`), not
 * to the socket that carries it. Two buses over one connection may honestly
 * want different patience.
 */
export type RabbitMqConfig = {
  readonly url: string
  /** Who this process is on the broker — see {@link RabbitMqIdentity}. */
  readonly identity: RabbitMqIdentity
  readonly topology?: RabbitMqTopologyConfig
  readonly retry?: RabbitMqRetryConfig
}

export type RabbitMqResolvedConfig = {
  readonly identity: RabbitMqIdentity
  readonly url: string
  readonly topology: ReturnType<typeof rabbitMqTopologyNames>
  readonly retry: Required<RabbitMqRetryConfig>
}

export function resolveRabbitMqConfig(config: RabbitMqConfig): RabbitMqResolvedConfig {
  return {
    identity: config.identity,
    url: config.url,
    topology: rabbitMqTopologyNames(config.identity, config.topology),
    retry: {
      deadLetter: config.retry?.deadLetter ?? true,
      deadLetterExchange:
        config.retry?.deadLetterExchange ?? `${config.topology?.prefix ?? "kronos"}.dlx`,
    },
  }
}
