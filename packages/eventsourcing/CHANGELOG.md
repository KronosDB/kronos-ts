# @kronos-ts/eventsourcing

## 0.3.3

### Patch Changes

- f3f9fbc: Publish compiled `dist` entrypoints in npm manifests instead of development-only TypeScript source paths. This makes the packages directly importable in Node.js while retaining concrete versions for workspace dependencies.
- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/messaging@0.9.2
  - @kronos-ts/modelling@0.2.10

## 0.3.2

### Patch Changes

- Updated dependencies [ad944b9]
  - @kronos-ts/messaging@0.9.1
  - @kronos-ts/modelling@0.2.9

## 0.3.1

### Patch Changes

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0
  - @kronos-ts/modelling@0.2.8

## 0.3.0

### Minor Changes

- 56bfb6d: Carry correlation data onto scheduled events.

  `schedule()` now merges the active unit of work's correlation data onto the scheduled event at schedule-time, mirroring `append()`. The fired event carries the correct correlationId/causationId of the message that scheduled it, instead of only the unit-of-work metadata. No-op when no correlation data is set.

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0
  - @kronos-ts/modelling@0.2.7

## 0.2.3

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0
  - @kronos-ts/modelling@0.2.6

## 0.2.2

### Patch Changes

- 291acd2: Propagate correlation and trace context across the event-handler boundary.

  - Correlation lineage now spans command → event → processor → command. Event processors seed correlation data from the triggering event before invoking its handlers, so a command (or event) dispatched from an event handler inherits the event's `correlationId` and is stamped with `causationId` = the event's identifier. `append()` applies the active correlation data to each event as it is staged, so appended events carry the correct lineage.
  - Correlation is configured in one place via `app.correlationDataProvider(...)`, defaulting to a single `messageOriginProvider()`. The configured providers feed the command/query handler extract step, the per-event seeding in every event processor, and the dispatch/append application. Correlation data is applied exactly once, at staging.
  - New `contributeCorrelationData(partial)` adds lineage keys to the active UnitOfWork (e.g. an OpenTelemetry `traceparent`) so they ride along on outgoing and appended messages. `applyCorrelationData(message, providers)` is exported for reuse.
  - Tracing: handler spans are now created from the message being handled and re-parent across the message boundary — command/query handlers continue the current trace, event handlers start a new trace linked to the triggering event. The span is made the active context for the duration of handling (`Span.runActive`), and its trace context is captured onto the UnitOfWork so appended and dispatched messages — including those published at commit time — carry it. `SpanFactory` gains optional `createLinkedHandlerSpan` and `currentTraceContext`.
  - The command bus now traces dispatch only; the handler enhancer is the single source of handler spans, so a command no longer gets a duplicate handle span.

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0
  - @kronos-ts/modelling@0.2.5

## 0.2.1

### Patch Changes

- 4ac26c0: Validate `schedule()`/`scheduleAfter()` inputs and bound transaction lifetime.

  - `schedule()` rejects an invalid `at` (`Invalid Date`); `scheduleAfter()` rejects a non-finite `delayMs`. A past time / negative delay is still allowed and fires as soon as possible.
  - `postgresTransactionManager` applies `idle_in_transaction_session_timeout` (default 30000ms) via `SET LOCAL` on every transaction, with an optional `statement_timeout`. Configurable through `postgres({ transaction: { idleInTransactionTimeoutMs, statementTimeoutMs } })`. Set either to `0` to disable.
  - `postgresTransactionManager.begin()` now rejects instead of hanging when the transaction callback fails before the handle is returned.
  - @kronos-ts/messaging@0.5.1
  - @kronos-ts/modelling@0.2.4

## 0.2.0

### Minor Changes

- 6a3dca4: Add `schedule()`, `scheduleAfter()`, and `cancelSchedule()` handler helpers for the event scheduler.

  Call them from inside a command or event handler the same way as `append()` / `send()` — pass an event descriptor + payload and a fire time, and the helper builds the event message and uses the configured `EventScheduler`. No fetching the scheduler from the app or hand-building an `EventMessage`.

  - `schedule(event, payload, at: Date)` schedules at an absolute time.
  - `scheduleAfter(event, payload, delayMs)` schedules a delay from now.
  - Both return a `ScheduleToken`; `cancelSchedule(token)` cancels it.

  The scheduler is injected into the active UnitOfWork at handler-invocation entry (event processors and command handlers), so a schedule participates in the handler's transaction — it commits with the handler and rolls back if the handler throws. Event metadata defaults to the UoW metadata, carrying correlation/causation onto the fired event.

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/messaging@0.5.0
  - @kronos-ts/modelling@0.2.3

## 0.1.5

### Patch Changes

- f5ed7da: Fix `load()` returning the wrong entity's state when two different ids of the same state module are loaded within one UnitOfWork.

  The per-UnitOfWork state cache keyed on `${module.name}:${String(id)}`. State ids are objects, so `String(id)` produced `"[object Object]"` for every id — collapsing distinct ids of a module to one cache entry, so the second `load()` returned the first's state. The cache key is now a structural serialization of the id (sorted keys, bigint-safe), so distinct ids get distinct entries and id construction order is irrelevant.

- Updated dependencies [dc0f67e]
  - @kronos-ts/messaging@0.4.0
  - @kronos-ts/modelling@0.2.2

## 0.1.4

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1
  - @kronos-ts/modelling@0.2.1

## 0.1.3

### Patch Changes

- Updated dependencies [c1a1cf5]
- Updated dependencies [74dc43d]
  - @kronos-ts/modelling@0.2.0
  - @kronos-ts/messaging@0.3.0

## 0.1.2

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0
  - @kronos-ts/modelling@0.1.2

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/messaging@0.1.1
  - @kronos-ts/modelling@0.1.1
