/**
 * The topology options are the ones the connection reads. Nothing here runs;
 * it is judged by `tsc` through the root `tsconfig.json` `files` array.
 */
import type { RabbitMqConnectionOptions } from "../connection.js"
import type { RabbitMqTopologyConfig } from "../topology.js"

export const namesAreOptions: RabbitMqTopologyConfig = {
  prefix: "shop",
  commandsExchange: "shop.commands",
  queriesExchange: "shop.queries",
  subscribersGossipExchange: "shop.subscribers.gossip",
  subscribersDirectExchange: "shop.subscribers.direct",
}

/** Handler queues are always durable; there is no option to say otherwise. */
// @ts-expect-error: `durableQueues` is not a topology option
export const durabilityIsNotAnOption: RabbitMqTopologyConfig = { durableQueues: false }

export const connectionRefusesItToo: RabbitMqConnectionOptions = {
  serviceName: "shop",
  instanceId: "shop-1",
  // @ts-expect-error: `durableQueues` is not a topology option
  topology: { durableQueues: false },
}
