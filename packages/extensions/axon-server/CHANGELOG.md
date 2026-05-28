# @kronos-ts/axon-server

## 0.2.0

### Minor Changes

- Add distributed subscription queries over the QueryService stream. Axon Server
  holds the subscriptionId↔handler mapping, so each handler tracks the
  server-injected subscribers it is given and targets emits directly by
  subscriptionIdentifier. `handleSubscriptionQueryRequest` processes inbound
  subscribe (runs the handler, returns initialResult) and unsubscribe;
  `emitUpdate` / `completeSubscription` / `completeSubscriptionExceptionally`
  iterate tracked subscribers, apply the `SubscriptionFilter`, and send
  per-subscriber responses.

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0
  - @kronos-ts/app@0.2.0
  - @kronos-ts/eventsourcing@0.1.2
  - @kronos-ts/modelling@0.1.2

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/messaging@0.1.1
  - @kronos-ts/modelling@0.1.1
  - @kronos-ts/eventsourcing@0.1.1
  - @kronos-ts/app@0.1.1
