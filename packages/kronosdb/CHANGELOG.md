# @kronos-ts/kronosdb

## 0.5.1

### Patch Changes

- Updated dependencies [9ad1a3c]
  - @kronos-ts/eventsourcing@0.4.1
  - @kronos-ts/messaging@0.10.1
  - @kronos-ts/modelling@0.3.1

## 0.5.0

### Minor Changes

- b46a045: Functional composition — the container is gone.

  BREAKING. The app builder, slot registry, decorator pipeline, lifecycle
  stages, extensions-as-mutators and `defineModule` are removed. The entry
  point is `kronos({ components, modules })`; a module is
  `module(name, overrides?, ...registrations)` with per-state snapshot
  options as `[state, options]` tuples; dependencies are plain function
  arguments.

  Handlers receive their capabilities as a typed context argument
  (`load`/`append`/`send`/`emitUpdate`/`schedule`/`transaction`); the
  module-level `append`/`load`/`send`/`emitUpdate` helpers are no longer
  exported. Query handlers get a read-only context (`load`/`transaction`).
  `append` accepts a batch of `[descriptor, payload]` tuples. `on()` is
  evolver/query only; `onEvent()` is removed.

  Every backend extension is an async factory returning
  `{ components, start(), close() }` with a uniform no-arg `start()`.
  Remote processor administration moved to opt-in control planes
  (`kronosDbControlPlane`, `axonServerControlPlane`) fed by
  `app.processors`. Correlation lineage now survives distributed dispatch
  (interception sits above the local/remote fork) and is seeded from
  incoming messages in the invocation wrappers. Axon reconnect detection is
  armed by the data path, independent of the control plane.

  All 59 `create*`-prefixed factories are renamed to what they return
  (`inMemoryEventStore`, `simpleCommandBus`, `postgresEventStore`, …).
  Drizzle stores no longer take the ORM operator bundle — only
  `{ db, table }`.

### Patch Changes

- Updated dependencies [b46a045]
  - @kronos-ts/messaging@0.10.0
  - @kronos-ts/eventsourcing@0.4.0
  - @kronos-ts/modelling@0.3.0

## 0.4.0

### Minor Changes

- 440b453: Event scheduler client for KronosDB's server-side scheduled appends.

  `createKronosDbScheduler(connection, serializer)` exposes `schedule`, `cancel`,
  and `list`. The store appends the event when due — no client-side timers or
  polling — and the schedule is durable once `schedule()` resolves. Supply your
  own token to make retried schedule calls idempotent. gRPC failures map to
  typed errors: `ScheduleAlreadyExistsError`, `ScheduleAlreadyResolvedError`,
  `ScheduleNotFoundError`.

### Patch Changes

- 1c86acb: Reconnect backoff now applies ±25% jitter so scaled-out service instances
  that lose their connection together (server restart, failover) don't
  reconnect as one synchronized wave.

## 0.3.1

### Patch Changes

- f3f9fbc: Publish compiled `dist` entrypoints in npm manifests instead of development-only TypeScript source paths. This makes the packages directly importable in Node.js while retaining concrete versions for workspace dependencies.
- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/messaging@0.9.2
  - @kronos-ts/modelling@0.2.10
  - @kronos-ts/eventsourcing@0.3.3
  - @kronos-ts/app@0.5.3

## 0.3.0

### Minor Changes

- e71323e: Adopt KronosDB 0.5 batched read wire format (requires server >= 0.5).

  Source and Stream responses now arrive as event batches (`SequencedEventBatch`)
  instead of one event per gRPC message; the client unpacks them transparently,
  so `source()` results and event-processor streams behave exactly as before —
  just faster (server-side batching removes per-message framing overhead on
  whole-log reads and live tailing). Permit-based flow control now counts
  events, not messages. `SourceRequest`/`StreamSubscribe` gain a `batchSize`
  option (0 = server default). Older servers (<= 0.4) are not supported by this
  version of the client.

  Also fixes `generate-proto` to emit `.js` import suffixes so regenerated
  sources pass typecheck under `moduleResolution: node16`.

- 3651f7a: Add a `messaging` option to the `kronosDb()` extension config (default `true`).

  With `messaging: false` the extension populates only the `eventStore` and `snapshotStore` slots, leaving `commandBus`/`queryBus` free for another transport (e.g. the RabbitMQ extension) or the in-memory defaults. The platform control plane (processor pause/start/split/merge, status reporting) stays active in both modes.

## 0.2.10

### Patch Changes

- Updated dependencies [ad944b9]
  - @kronos-ts/messaging@0.9.1
  - @kronos-ts/app@0.5.2
  - @kronos-ts/eventsourcing@0.3.2
  - @kronos-ts/modelling@0.2.9

## 0.2.9

### Patch Changes

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0
  - @kronos-ts/app@0.5.1
  - @kronos-ts/eventsourcing@0.3.1
  - @kronos-ts/modelling@0.2.8

## 0.2.8

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0
  - @kronos-ts/app@0.5.0
  - @kronos-ts/eventsourcing@0.3.0
  - @kronos-ts/modelling@0.2.7

## 0.2.7

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0
  - @kronos-ts/app@0.4.1
  - @kronos-ts/eventsourcing@0.2.3
  - @kronos-ts/modelling@0.2.6

## 0.2.6

### Patch Changes

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0
  - @kronos-ts/app@0.4.0
  - @kronos-ts/eventsourcing@0.2.2
  - @kronos-ts/modelling@0.2.5

## 0.2.5

### Patch Changes

- Updated dependencies [4ac26c0]
  - @kronos-ts/eventsourcing@0.2.1
  - @kronos-ts/app@0.3.4
  - @kronos-ts/messaging@0.5.1
  - @kronos-ts/modelling@0.2.4

## 0.2.4

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/eventsourcing@0.2.0
  - @kronos-ts/messaging@0.5.0
  - @kronos-ts/app@0.3.3
  - @kronos-ts/modelling@0.2.3

## 0.2.3

### Patch Changes

- Updated dependencies [dc0f67e]
- Updated dependencies [f5ed7da]
  - @kronos-ts/messaging@0.4.0
  - @kronos-ts/app@0.3.2
  - @kronos-ts/eventsourcing@0.1.5
  - @kronos-ts/modelling@0.2.2

## 0.2.2

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1
  - @kronos-ts/app@0.3.1
  - @kronos-ts/eventsourcing@0.1.4
  - @kronos-ts/modelling@0.2.1

## 0.2.1

### Patch Changes

- Updated dependencies [c1a1cf5]
- Updated dependencies [74dc43d]
  - @kronos-ts/modelling@0.2.0
  - @kronos-ts/app@0.3.0
  - @kronos-ts/messaging@0.3.0
  - @kronos-ts/eventsourcing@0.1.3

## 0.2.0

### Minor Changes

- Add distributed subscription queries over the QueryService stream. The
  KronosDB server holds the subscriptionId↔handler mapping, so each handler
  tracks the server-injected subscribers it is given and targets emits directly
  by subscriptionIdentifier. `handleSubscriptionQueryRequest` processes inbound
  subscribe (runs the handler, returns initialResult) and unsubscribe;
  `emitUpdate` / `completeSubscription` / `completeSubscriptionExceptionally`
  iterate tracked subscribers, apply the `SubscriptionFilter`, and send
  per-subscriber responses. Function and `payloadEquals` filters both evaluate
  locally on the emitter.

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
