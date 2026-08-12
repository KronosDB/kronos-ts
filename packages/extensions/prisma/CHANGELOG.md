# @kronos-ts/prisma

## 0.2.10

### Patch Changes

- @kronos-ts/messaging@0.10.1

## 0.2.9

### Patch Changes

- Updated dependencies [b46a045]
  - @kronos-ts/messaging@0.10.0

## 0.2.8

### Patch Changes

- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/messaging@0.9.2

## 0.2.7

### Patch Changes

- Updated dependencies [ad944b9]
  - @kronos-ts/messaging@0.9.1

## 0.2.6

### Patch Changes

- 9eb84ff: Carry the commit-order key in durable tracking tokens so gap-free tailing resumes correctly.

  The postgres engine tails events in `(transaction_id, sequence_position)` order with a `pg_snapshot_xmin` watermark, but durable tokens stored only `sequence_position`. On stream reopen the catch-up filter compared positions alone, so an event with a lower `sequence_position` but higher `transaction_id` — which happens when a transaction writes other rows (stamping its xid) before appending its event — was permanently skipped.

  - `messaging`: adds `gapAwareToken(sequence, gapKey)` (a `TrackingToken` carrying an opaque commit-order key alongside the position), `advanceTokenTo`, and `serializeToken`/`deserializeToken`. `SequencedEvent` and `StreamingCondition` gain an optional `token`, letting an engine hand the processor its own resume cursor instead of a bare position. Both processors persist the engine-supplied token when present.
  - `postgres`: `open()` emits a gap-aware token per event and, on reopen, resumes the `(transaction_id, sequence_position)` tuple cursor from it. Engines that supply no token (in-memory, Axon Server) are unaffected.
  - token stores (`knex`, `kysely`, `drizzle`, `prisma`, `typeorm`): serialize through the shared `messaging` helpers so the commit-order key round-trips instead of being flattened to a position.

  Token format change: tokens written before this release carry no commit-order key. They rehydrate as position-only tokens and resume via the legacy catch-up branch on first reopen, then mint gap-aware tokens going forward; to close the window immediately, reset the affected processors.

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0

## 0.2.5

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0

## 0.2.4

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0

## 0.2.3

### Patch Changes

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0

## 0.2.2

### Patch Changes

- @kronos-ts/messaging@0.5.1

## 0.2.1

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/messaging@0.5.0

## 0.2.0

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

- Updated dependencies [dc0f67e]
  - @kronos-ts/messaging@0.4.0

## 0.1.4

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1

## 0.1.3

### Patch Changes

- Updated dependencies [74dc43d]
  - @kronos-ts/messaging@0.3.0

## 0.1.2

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/messaging@0.1.1
