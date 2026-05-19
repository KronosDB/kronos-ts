---
"@kronos-ts/rabbitmq": minor
---

Add distributed query transport. The RabbitMQ extension now decorates the
`queryBus` alongside the `commandBus`: direct request/reply queries route over a
dedicated `kronos.queries` exchange with per-query handler queues and a
per-process reply queue, mirroring the command transport. Subscription queries
remain process-local. Adds a `queries` config block (`preferLocalHandlers`,
`alwaysUseDistributedBus`, `defaultTimeoutMs`).

The command and query transports now share a single broker connection (one per
process), each taking its own channel, instead of opening a connection each.
