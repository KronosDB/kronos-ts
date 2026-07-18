# @kronos-ts/rabbitmq

## 0.3.11

### Patch Changes

- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/messaging@0.9.2
  - @kronos-ts/modelling@0.2.10
  - @kronos-ts/eventsourcing@0.3.3
  - @kronos-ts/app@0.5.3

## 0.3.10

### Patch Changes

- Updated dependencies [ad944b9]
  - @kronos-ts/messaging@0.9.1
  - @kronos-ts/app@0.5.2
  - @kronos-ts/eventsourcing@0.3.2
  - @kronos-ts/modelling@0.2.9

## 0.3.9

### Patch Changes

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0
  - @kronos-ts/app@0.5.1
  - @kronos-ts/eventsourcing@0.3.1
  - @kronos-ts/modelling@0.2.8

## 0.3.8

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0
  - @kronos-ts/app@0.5.0
  - @kronos-ts/eventsourcing@0.3.0
  - @kronos-ts/modelling@0.2.7

## 0.3.7

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0
  - @kronos-ts/app@0.4.1
  - @kronos-ts/eventsourcing@0.2.3
  - @kronos-ts/modelling@0.2.6

## 0.3.6

### Patch Changes

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0
  - @kronos-ts/app@0.4.0
  - @kronos-ts/eventsourcing@0.2.2
  - @kronos-ts/modelling@0.2.5

## 0.3.5

### Patch Changes

- Updated dependencies [4ac26c0]
  - @kronos-ts/eventsourcing@0.2.1
  - @kronos-ts/app@0.3.4
  - @kronos-ts/messaging@0.5.1
  - @kronos-ts/modelling@0.2.4

## 0.3.4

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/eventsourcing@0.2.0
  - @kronos-ts/messaging@0.5.0
  - @kronos-ts/app@0.3.3
  - @kronos-ts/modelling@0.2.3

## 0.3.3

### Patch Changes

- Updated dependencies [dc0f67e]
- Updated dependencies [f5ed7da]
  - @kronos-ts/messaging@0.4.0
  - @kronos-ts/app@0.3.2
  - @kronos-ts/eventsourcing@0.1.5
  - @kronos-ts/modelling@0.2.2

## 0.3.2

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1
  - @kronos-ts/app@0.3.1
  - @kronos-ts/eventsourcing@0.1.4
  - @kronos-ts/modelling@0.2.1

## 0.3.1

### Patch Changes

- Updated dependencies [c1a1cf5]
- Updated dependencies [74dc43d]
  - @kronos-ts/modelling@0.2.0
  - @kronos-ts/app@0.3.0
  - @kronos-ts/messaging@0.3.0
  - @kronos-ts/eventsourcing@0.1.3

## 0.3.0

### Minor Changes

- Add distributed subscription queries via a gossip-mirror subscriber registry.
  Every `subscribe` publishes a claim over a fanout gossip exchange; each
  instance maintains a cluster-wide `Map<subId, SubscriberRecord>` mirror.
  `emitUpdate` walks the local mirror (where every subscriber's payload lives),
  applies the `SubscriptionFilter`, and routes per-subscriber delivery over a
  direct exchange keyed by the owner's instanceId — so function-form filters
  work across instances by executing colocated with their payloads. A joiner
  publishes a syncRequest on connect and peers re-broadcast owned claims to fill
  its mirror. Topology: `kronos.subscribers.gossip` (fanout) +
  `kronos.subscribers.direct` (direct). Instance-death failover and owner
  election are out of scope.

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0
  - @kronos-ts/app@0.2.0
  - @kronos-ts/eventsourcing@0.1.2
  - @kronos-ts/modelling@0.1.2

## 0.2.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/messaging@0.1.1
  - @kronos-ts/modelling@0.1.1
  - @kronos-ts/eventsourcing@0.1.1
  - @kronos-ts/app@0.1.1

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
