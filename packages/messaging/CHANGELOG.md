# @kronos-ts/messaging

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
