# @kronos-ts/eventsourcing

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
