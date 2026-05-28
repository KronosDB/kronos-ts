# @kronos-ts/messaging

## 0.2.0

### Minor Changes

- Add a durable EventScheduler for deferring events to a future time.

  `schedule(event, at)` is callable only inside a UnitOfWork so a scheduled
  event commits or rolls back atomically with the originating command;
  `cancel(token)` returns a `CancelResult` discriminated union
  (`cancelled` | `already-appended` | `not-found`).

  - `@kronos-ts/messaging` exports the `EventScheduler` contract and a
    `setTimeout`-backed in-memory implementation for tests, plus a lazy
    transactional UnitOfWork runner so writers share one transaction per UoW.
  - `@kronos-ts/app` adds `eventScheduler` as a typed `KronosComponents` slot
    with an in-memory default that emits a durability startup warning.
  - `@kronos-ts/postgres` provides a durable scheduler backed by
    `kronos_scheduled_events` with a `FOR UPDATE SKIP LOCKED` polling worker;
    `schedule_id` is reused as the event id so re-fires after a crash dedupe
    via the events table's UNIQUE constraint.

- Add structured subscription filters for cross-process routing.
  `SubscriptionFilter<P> = ((payload) => boolean) | { payloadEquals: Partial<P> }`
  lets subscription-query emit filters be evaluated by remote receivers with no
  access to the emitter's closure: function filters remain in-process fallbacks,
  `payloadEquals` is the serializable form distributed transports gate on. Adds
  `applySubscriptionFilter` / `extractStructuredFilter` / `matchesPayloadEquals`
  and threads the type through `emitUpdate`, `completeSubscription`, and
  `completeSubscriptionExceptionally` on the query buses.

### Patch Changes

- @kronos-ts/eventsourcing@0.1.2

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/eventsourcing@0.1.1
