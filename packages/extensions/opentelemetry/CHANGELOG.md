# @kronos-ts/opentelemetry

## 0.4.2

### Patch Changes

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0
  - @kronos-ts/app@0.5.1

## 0.4.1

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0
  - @kronos-ts/app@0.5.0

## 0.4.0

### Minor Changes

- dafdf12: Add a metrics seam and OpenTelemetry metrics extension.

  - New backend-agnostic `MetricsRecorder` (counter/histogram instruments that take attributes) and `noOpMetricsRecorder()` in the messaging core — the metrics analogue of `SpanFactory`.
  - `meteringHandlerEnhancerDefinition(recorder)` records per-invocation metrics uniformly for command/query/event handlers: a `messages.handled` counter (tagged with `outcome` = success | failure), a `message.handler.duration` histogram, and an `event.processing.lag` histogram (delay between an event's timestamp and when it was handled). The metric namespace is configurable.
  - `@kronos-ts/opentelemetry` adds `createOpenTelemetryMetricsRecorder()` (over the OpenTelemetry Metrics API) and an `openTelemetryMetrics()` extension that wires the metering enhancer. Compose it alongside `openTelemetry()`.

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0
  - @kronos-ts/app@0.4.1

## 0.3.0

### Minor Changes

- 291acd2: Propagate correlation and trace context across the event-handler boundary.

  - Correlation lineage now spans command → event → processor → command. Event processors seed correlation data from the triggering event before invoking its handlers, so a command (or event) dispatched from an event handler inherits the event's `correlationId` and is stamped with `causationId` = the event's identifier. `append()` applies the active correlation data to each event as it is staged, so appended events carry the correct lineage.
  - Correlation is configured in one place via `app.correlationDataProvider(...)`, defaulting to a single `messageOriginProvider()`. The configured providers feed the command/query handler extract step, the per-event seeding in every event processor, and the dispatch/append application. Correlation data is applied exactly once, at staging.
  - New `contributeCorrelationData(partial)` adds lineage keys to the active UnitOfWork (e.g. an OpenTelemetry `traceparent`) so they ride along on outgoing and appended messages. `applyCorrelationData(message, providers)` is exported for reuse.
  - Tracing: handler spans are now created from the message being handled and re-parent across the message boundary — command/query handlers continue the current trace, event handlers start a new trace linked to the triggering event. The span is made the active context for the duration of handling (`Span.runActive`), and its trace context is captured onto the UnitOfWork so appended and dispatched messages — including those published at commit time — carry it. `SpanFactory` gains optional `createLinkedHandlerSpan` and `currentTraceContext`.
  - The command bus now traces dispatch only; the handler enhancer is the single source of handler spans, so a command no longer gets a duplicate handle span.

### Patch Changes

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0
  - @kronos-ts/app@0.4.0

## 0.2.2

### Patch Changes

- @kronos-ts/app@0.3.4
- @kronos-ts/messaging@0.5.1

## 0.2.1

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/messaging@0.5.0
  - @kronos-ts/app@0.3.3

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
  - @kronos-ts/app@0.3.2

## 0.1.4

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1
  - @kronos-ts/app@0.3.1

## 0.1.3

### Patch Changes

- Updated dependencies [c1a1cf5]
- Updated dependencies [74dc43d]
  - @kronos-ts/app@0.3.0
  - @kronos-ts/messaging@0.3.0

## 0.1.2

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0
  - @kronos-ts/app@0.2.0

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/messaging@0.1.1
  - @kronos-ts/app@0.1.1
