import type { QualifiedName } from "@kronos-ts/common"
import type { KronosIdentity } from "@kronos-ts/app"

export interface RabbitMqTopologyConfig {
  readonly prefix?: string
  readonly commandsExchange?: string
  readonly queriesExchange?: string
  readonly subscribersGossipExchange?: string
  readonly subscribersDirectExchange?: string
  readonly durableQueues?: boolean
}

export interface RabbitMqTopologyNames {
  readonly commandsExchange: string
  readonly queriesExchange: string
  readonly subscribersGossipExchange: string
  readonly subscribersDirectExchange: string
  commandRoutingKey(commandName: QualifiedName | string): string
  commandQueue(commandName: QualifiedName | string): string
  queryRoutingKey(queryName: QualifiedName | string): string
  queryQueue(queryName: QualifiedName | string): string
  subscribersGossipQueue(): string
  subscribersDirectQueue(): string
  commandReplyQueue(): string
  queryReplyQueue(): string
}

export function createRabbitMqTopologyNames(
  identity: KronosIdentity,
  config: RabbitMqTopologyConfig = {},
): RabbitMqTopologyNames {
  const prefix = sanitizeSegment(config.prefix ?? "kronos")
  const service = sanitizeSegment(identity.serviceName)
  const instance = sanitizeSegment(identity.instanceId)
  const commandsExchange = config.commandsExchange ?? `${prefix}.commands`
  const queriesExchange = config.queriesExchange ?? `${prefix}.queries`
  const subscribersGossipExchange =
    config.subscribersGossipExchange ?? `${prefix}.subscribers.gossip`
  const subscribersDirectExchange =
    config.subscribersDirectExchange ?? `${prefix}.subscribers.direct`

  return {
    commandsExchange,
    queriesExchange,
    subscribersGossipExchange,
    subscribersDirectExchange,
    commandRoutingKey(commandName) {
      return messageName(commandName)
    },
    commandQueue(commandName) {
      return `${prefix}.commands.${service}.${sanitizeMessageName(messageName(commandName))}`
    },
    queryRoutingKey(queryName) {
      return messageName(queryName)
    },
    queryQueue(queryName) {
      return `${prefix}.queries.${service}.${sanitizeMessageName(messageName(queryName))}`
    },
    subscribersGossipQueue() {
      return `${prefix}.subscribers.gossip.${service}.${instance}`
    },
    subscribersDirectQueue() {
      return `${prefix}.subscribers.direct.${service}.${instance}`
    },
    commandReplyQueue() {
      return `${prefix}.replies.${service}.${instance}`
    },
    queryReplyQueue() {
      return `${prefix}.query-replies.${service}.${instance}`
    },
  }
}

function messageName(name: QualifiedName | string): string {
  if (typeof name === "string") return name
  const maybe = name as unknown as { namespace?: string; name?: string; localName?: string }
  const local = maybe.name ?? maybe.localName
  return maybe.namespace && local ? `${maybe.namespace}.${local}` : String(name)
}

function sanitizeMessageName(value: string): string {
  return value.replace(/[^A-Za-z0-9_.-]/g, "_")
}

function sanitizeSegment(value: string): string {
  const trimmed = value.trim()
  if (!trimmed) throw new Error("RabbitMQ topology segment must not be empty")
  return sanitizeMessageName(trimmed)
}
