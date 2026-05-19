export {
  rabbitMq,
  resolveRabbitMqConfig,
  type RabbitMqExtensionConfig,
  type RabbitMqCommandDispatchConfig,
  type RabbitMqQueryDispatchConfig,
  type RabbitMqRetryConfig,
  type RabbitMqResolvedConfig,
} from "./rabbitmq.js"

export {
  createRabbitMqTopologyNames,
  type RabbitMqTopologyConfig,
  type RabbitMqTopologyNames,
} from "./topology.js"

export {
  createRabbitMqCommandBus,
  type RabbitMqCommandEnvelope,
  type RabbitMqCommandReplyEnvelope,
  type RabbitMqCommandTransport,
  type RabbitMqCommandBusOptions,
} from "./command-bus.js"

export {
  createRabbitMqQueryBus,
  type RabbitMqQueryEnvelope,
  type RabbitMqQueryReplyEnvelope,
  type RabbitMqQueryTransport,
  type RabbitMqQueryBusOptions,
} from "./query-bus.js"

export { AmqpRabbitMqCommandTransport } from "./amqp-command-transport.js"
export { AmqpRabbitMqQueryTransport } from "./amqp-query-transport.js"

export {
  createAmqpConnection,
  type AmqpConnection,
  type AmqpConnect,
} from "./connection.js"
