# @kronos-ts/messaging

## 0.10.1

### Patch Changes

- Updated dependencies [9ad1a3c]
  - @kronos-ts/eventsourcing@0.4.1

## 0.10.0

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
  - @kronos-ts/eventsourcing@0.4.0

## 0.9.2

### Patch Changes

- f3f9fbc: Publish compiled `dist` entrypoints in npm manifests instead of development-only TypeScript source paths. This makes the packages directly importable in Node.js while retaining concrete versions for workspace dependencies.
- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/eventsourcing@0.3.3

## 0.9.1

### Patch Changes

- ad944b9: Label persisted tokens with PascalCase `token_type` values (`GlobalSequenceToken` / `GapAwareToken`).

  The token serializer wrote the kebab-case token kind into the `token_type` column, leaving `gap-aware` sitting next to legacy `GlobalSequenceToken` rows. The value is informational — deserialization keys off the `gapKey` in the body, not the type string — so this only affects the readability of the token table.

  - @kronos-ts/eventsourcing@0.3.2

## 0.9.0

### Minor Changes

- 9eb84ff: Carry the commit-order key in durable tracking tokens so gap-free tailing resumes correctly.

  The postgres engine tails events in `(transaction_id, sequence_position)` order with a `pg_snapshot_xmin` watermark, but durable tokens stored only `sequence_position`. On stream reopen the catch-up filter compared positions alone, so an event with a lower `sequence_position` but higher `transaction_id` — which happens when a transaction writes other rows (stamping its xid) before appending its event — was permanently skipped.

  - `messaging`: adds `gapAwareToken(sequence, gapKey)` (a `TrackingToken` carrying an opaque commit-order key alongside the position), `advanceTokenTo`, and `serializeToken`/`deserializeToken`. `SequencedEvent` and `StreamingCondition` gain an optional `token`, letting an engine hand the processor its own resume cursor instead of a bare position. Both processors persist the engine-supplied token when present.
  - `postgres`: `open()` emits a gap-aware token per event and, on reopen, resumes the `(transaction_id, sequence_position)` tuple cursor from it. Engines that supply no token (in-memory, Axon Server) are unaffected.
  - token stores (`knex`, `kysely`, `drizzle`, `prisma`, `typeorm`): serialize through the shared `messaging` helpers so the commit-order key round-trips instead of being flattened to a position.

  Token format change: tokens written before this release carry no commit-order key. They rehydrate as position-only tokens and resume via the legacy catch-up branch on first reopen, then mint gap-aware tokens going forward; to close the window immediately, reset the affected processors.

### Patch Changes

- @kronos-ts/eventsourcing@0.3.1

## 0.8.0

### Minor Changes

- 56bfb6d: Expose event processors for host/admin control, with a status snapshot.

  - `EventProcessorStatus` (running / error / position / caughtUp / replaying) is added to the common `event-processor` module, and `TrackingEventProcessor` now implements the common `EventProcessor` interface and reports `status()`. The processor tracks caught-up and last-error state, clearing the error once a later batch succeeds.
  - `RunningApp.eventProcessors()` returns the built processors keyed by name — the seam a host or admin UI enumerates to read status and call `start()` / `stop()` / `resetTokens()`. The framework ships no watchdog or auto-restart; operating processors is the host's responsibility.
  - The `EventProcessorStatus` type previously exported from the streaming processor module is no longer re-exported (the streaming processor keeps its own internal per-segment status type).

- 56bfb6d: Default event processors to propagate handler errors; remove the swallowing logging handler.

  - `loggingErrorHandler` is removed. It logged a failed handler and advanced the token, silently skipping the event — which corrupts a read model. AF5 retired the equivalent swallow-and-continue handler to legacy; the only live processor error handler there is the propagating one.
  - The default `errorHandler` for tracking, streaming, and subscribing processors is now `propagatingErrorHandler()`. A failed handler no longer advances the token: the batch rolls back and is redelivered (with backoff), so a transient failure recovers on retry and a real bug stops the processor at the offending event instead of skipping it. To deliberately move past a poison pill, attach a dead-letter queue.
  - This changes the default behavior of any processor that previously relied on the swallow-and-continue default. Supply a custom `errorHandler` if you need skip-on-error semantics.

### Patch Changes

- Updated dependencies [56bfb6d]
  - @kronos-ts/eventsourcing@0.3.0

## 0.7.0

### Minor Changes

- dafdf12: Add a metrics seam and OpenTelemetry metrics extension.

  - New backend-agnostic `MetricsRecorder` (counter/histogram instruments that take attributes) and `noOpMetricsRecorder()` in the messaging core — the metrics analogue of `SpanFactory`.
  - `meteringHandlerEnhancerDefinition(recorder)` records per-invocation metrics uniformly for command/query/event handlers: a `messages.handled` counter (tagged with `outcome` = success | failure), a `message.handler.duration` histogram, and an `event.processing.lag` histogram (delay between an event's timestamp and when it was handled). The metric namespace is configurable.
  - `@kronos-ts/opentelemetry` adds `createOpenTelemetryMetricsRecorder()` (over the OpenTelemetry Metrics API) and an `openTelemetryMetrics()` extension that wires the metering enhancer. Compose it alongside `openTelemetry()`.

### Patch Changes

- @kronos-ts/eventsourcing@0.2.3

## 0.6.0

### Minor Changes

- 291acd2: Propagate correlation and trace context across the event-handler boundary.

  - Correlation lineage now spans command → event → processor → command. Event processors seed correlation data from the triggering event before invoking its handlers, so a command (or event) dispatched from an event handler inherits the event's `correlationId` and is stamped with `causationId` = the event's identifier. `append()` applies the active correlation data to each event as it is staged, so appended events carry the correct lineage.
  - Correlation is configured in one place via `app.correlationDataProvider(...)`, defaulting to a single `messageOriginProvider()`. The configured providers feed the command/query handler extract step, the per-event seeding in every event processor, and the dispatch/append application. Correlation data is applied exactly once, at staging.
  - New `contributeCorrelationData(partial)` adds lineage keys to the active UnitOfWork (e.g. an OpenTelemetry `traceparent`) so they ride along on outgoing and appended messages. `applyCorrelationData(message, providers)` is exported for reuse.
  - Tracing: handler spans are now created from the message being handled and re-parent across the message boundary — command/query handlers continue the current trace, event handlers start a new trace linked to the triggering event. The span is made the active context for the duration of handling (`Span.runActive`), and its trace context is captured onto the UnitOfWork so appended and dispatched messages — including those published at commit time — carry it. `SpanFactory` gains optional `createLinkedHandlerSpan` and `currentTraceContext`.
  - The command bus now traces dispatch only; the handler enhancer is the single source of handler spans, so a command no longer gets a duplicate handle span.

- 291acd2: Add a `kind` discriminator to messages and bring the postgres event store to full `EventMessage` round-trip parity.

  - `Message` now carries `readonly kind: "command" | "event" | "query"` (exported as `MessageKind`), narrowed to the literal on `CommandMessage`, `EventMessage`, and `QueryMessage`. Handler interceptors can branch on message category at runtime via `message.kind`. Gateways, `send()`, `append()`, `schedule()`, and every event-store reconstruction set it; `kind` is derived, never persisted.
  - The postgres event store now persists `version` and `message_timestamp` and selects `event_id`, so `source()` and `open()` reconstruct the complete `EventMessage` — `identifier`, authored `timestamp`, and `version`. Previously these three fields were dropped on read, diverging from the in-memory, axon-server, and kronosdb engines.
  - Schema change is CREATE-only. `CREATE TABLE IF NOT EXISTS` does not add columns to an existing table and the new columns are `NOT NULL`, so an events table created before this release must be hand-migrated (`ALTER TABLE ... ADD COLUMN`) or reset before upgrading.

### Patch Changes

- Updated dependencies [291acd2]
  - @kronos-ts/eventsourcing@0.2.2

## 0.5.1

### Patch Changes

- Updated dependencies [4ac26c0]
  - @kronos-ts/eventsourcing@0.2.1

## 0.5.0

### Minor Changes

- 6a3dca4: Add `schedule()`, `scheduleAfter()`, and `cancelSchedule()` handler helpers for the event scheduler.

  Call them from inside a command or event handler the same way as `append()` / `send()` — pass an event descriptor + payload and a fire time, and the helper builds the event message and uses the configured `EventScheduler`. No fetching the scheduler from the app or hand-building an `EventMessage`.

  - `schedule(event, payload, at: Date)` schedules at an absolute time.
  - `scheduleAfter(event, payload, delayMs)` schedules a delay from now.
  - Both return a `ScheduleToken`; `cancelSchedule(token)` cancels it.

  The scheduler is injected into the active UnitOfWork at handler-invocation entry (event processors and command handlers), so a schedule participates in the handler's transaction — it commits with the handler and rolls back if the handler throws. Event metadata defaults to the UoW metadata, carrying correlation/causation onto the fired event.

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/eventsourcing@0.2.0

## 0.4.0

### Minor Changes

- dc0f67e: Add a dead-letter queue (DLQ) for streaming and tracking event processors.

  When a DLQ is configured, a failing event is parked instead of redelivering its batch: the batch commits, the token advances past it, and later events in the same sequence are blocked to preserve ordering. The enqueue runs in the same UnitOfWork transaction as the token update.

  - `SequencingPolicy` (`sequentialPerTag`, `defaultSequencingPolicy`, `fullConcurrencyPolicy`) chooses an event's ordered sequence.
  - Enqueue policies + a `Decisions` factory, including `retryThenEvictPolicy` (caps retries via diagnostics, then evicts).
  - Reprocessing: `reprocessDeadLetters()` replays parked sequences through the same handlers; optional scheduled drain via `dlqRetryInterval`.
  - `DeadLetterListener` observability hook (no-op / logging / multi) and an OpenTelemetry listener.
  - A full queue applies backpressure (`DeadLetterQueueOverflowError`); `resetTokens()` clears the DLQ when `resetClearsDeadLetters` is set.
  - Persistent backends: `drizzleDeadLetterQueue`, `kyselyDeadLetterQueue`, `knexDeadLetterQueue`, `typeormDeadLetterQueue`, `prismaDeadLetterQueue`, each enqueueing inside the active transaction.
  - Builder methods: `.deadLetterQueue()`, `.enqueuePolicy()`, `.sequencingPolicy()`, `.deadLetterListener()`, `.resetClearsDeadLetters()`, `.dlqRetryInterval()`.

  The event-processor `batchSize` default changes from 100 to 1, keeping per-entity `load()` decisions isolated to their own UnitOfWork. Raise it for read-model projections that only apply idempotent view updates.

### Patch Changes

- Updated dependencies [f5ed7da]
  - @kronos-ts/eventsourcing@0.1.5

## 0.3.1

### Patch Changes

- 4b8faa5: Fix streaming/tracking event processors skipping a failed event batch until restart

  When an event handler batch failed (the UnitOfWork aborted before `PREPARE_COMMIT`), the tracking token was correctly held back, but the live `MessageStream` cursor had already advanced past the batch during accumulation. Nothing realigned the stream to the un-committed checkpoint, so the next poll read the _next_ event and the failed batch was silently skipped until the processor restarted and re-read the token store.

  Both `createTrackingEventProcessor` and `createStreamingEventProcessor` now close and discard the stream on a batch failure and reopen it from the committed token on the next cycle, so the failed events are redelivered without requiring a restart. This matches Axon Framework's close-and-reopen-from-token recovery contract. `createStreamingEventProcessor` gains an `errorBackoffMs` option (default 1000ms) to throttle retries of a deterministically failing handler.

  - @kronos-ts/eventsourcing@0.1.4

## 0.3.0

### Minor Changes

- 74dc43d: Command handlers now run inside the UnitOfWork's transaction, so a handler's appended events and any other writes it makes commit — or roll back — atomically.

  Previously the command bus opened a fresh UnitOfWork that bypassed the configured `unitOfWorkFactory`, so a transaction provided by a backend never reached command handlers (each `append()` opened its own short-lived transaction instead). The in-memory `createSimpleCommandBus` now runs handlers through the configured `unitOfWorkFactory` — matching the distributed buses (kronosdb / axon-server), which already did this. With the in-memory default factory (`runInNewUoW`) behavior is unchanged; a transactional backend gives each command's UoW a transaction.

  `@kronos-ts/postgres`: command handlers are transactional out of the box — **lazy**, so pure-read handlers never claim a connection. `PostgresAdapterTransaction` gains `unwrap<T>()`, which returns the live driver connection backing the UoW transaction (pg `PoolClient`, or the scoped `sql` for porsager/Bun). Use it to run your own SQL, or bind an ORM, in the same transaction as your events:

  ```ts
  const tx = await getOrBeginActiveTransaction<PostgresAdapterTransaction>();
  await tx!.query("UPDATE widgets SET name = $1 WHERE id = $2", [name, id]);
  append(WidgetUpdated, { id, name }); // same commit
  // or hand tx.unwrap() to Drizzle/Kysely — see the @kronos-ts/postgres README.
  ```

  **Breaking (`@kronos-ts/messaging`):** `createCommandGateway(bus, unitOfWorkRunner?)` is now `createCommandGateway(bus)` — the gateway is a thin message-builder and no longer opens a UnitOfWork; the command bus owns the single per-command UoW (AF5-aligned). `createSimpleCommandBus()` now accepts an optional `UoWRunner` (defaults to `runInNewUoW`). Direct callers of `createCommandGateway` that passed a runner should drop the second argument; the transactional runner now belongs on the `unitOfWorkFactory` slot, which the bus consumes.

  **Breaking (`@kronos-ts/postgres`)** for custom adapter authors only: `PostgresAdapterTransaction` now requires an `unwrap<T>(): T` method returning the underlying driver connection. The three bundled adapters (pg / postgres / bun-sql) implement it; custom adapters must add it.

### Patch Changes

- @kronos-ts/eventsourcing@0.1.3

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
