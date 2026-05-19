# @kronos-ts/rabbitmq

## 0.2.0

### Minor Changes

- f50be5e: Add distributed query transport. The RabbitMQ extension now decorates the
  `queryBus` alongside the `commandBus`: direct request/reply queries route over a
  dedicated `kronos.queries` exchange with per-query handler queues and a
  per-process reply queue, mirroring the command transport. Subscription queries
  remain process-local. Adds a `queries` config block (`preferLocalHandlers`,
  `alwaysUseDistributedBus`, `defaultTimeoutMs`).

  The command and query transports now share a single broker connection (one per
  process), each taking its own channel, instead of opening a connection each.

## 0.1.1

### Patch Changes

- Correct the package description — the RabbitMQ extension provides distributed
  command and query transport, not event transport.
