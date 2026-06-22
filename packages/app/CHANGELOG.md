# @kronos-ts/app

## 0.3.2

### Patch Changes

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

- Updated dependencies [dc0f67e]
- Updated dependencies [f5ed7da]
  - @kronos-ts/messaging@0.4.0
  - @kronos-ts/eventsourcing@0.1.5
  - @kronos-ts/modelling@0.2.2

## 0.3.1

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1
  - @kronos-ts/eventsourcing@0.1.4
  - @kronos-ts/modelling@0.2.1

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

- c1a1cf5: **Breaking:** `state()` evolvers are now registered through a builder —
  `evolve: (on) => [...]` — instead of a bare array. The `on` handed to the
  builder is bound to the state type, so evolver callbacks no longer need a
  `(s: State)` annotation: the state type is fixed by `initial` before the
  evolvers are checked, so they are validated against it instead of competing to
  infer it. This delivers the documented "state type is inferred from `initial`"
  behavior.

  Migrate by wrapping the array in `(on) => [...]`, dropping the `(s: State)`
  annotations, and removing the now-unused `on` import (the builder supplies it).
  Annotate `initial` (`initial: (id): State => ...`) only when it under-specifies
  the type via empty arrays or unions.

  ```ts
  // before
  evolve: [
    on(CourseCreated, (s: CourseState, { payload }) => ({
      ...s,
      created: true,
    })),
  ];
  // after
  evolve: (on) => [
    on(CourseCreated, (s, { payload }) => ({ ...s, created: true })),
  ];
  ```

  `@kronos-ts/app`: `kronos({ states })` partial-config now accepts precisely-typed
  state modules (`StateModule<any, any>[]`), matching `App.states()`. The previous
  `StateModule[]` (`<unknown, unknown>`) rejected concrete modules because `Id`
  sits in a contravariant position — a latent bug surfaced once the evolve builder
  began inferring state types precisely.

- Updated dependencies [c1a1cf5]
- Updated dependencies [74dc43d]
  - @kronos-ts/modelling@0.2.0
  - @kronos-ts/messaging@0.3.0
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

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0
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
