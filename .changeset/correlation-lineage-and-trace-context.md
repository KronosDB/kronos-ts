---
"@kronos-ts/messaging": minor
"@kronos-ts/app": minor
"@kronos-ts/opentelemetry": minor
"@kronos-ts/eventsourcing": patch
---

Propagate correlation and trace context across the event-handler boundary.

- Correlation lineage now spans command → event → processor → command. Event processors seed correlation data from the triggering event before invoking its handlers, so a command (or event) dispatched from an event handler inherits the event's `correlationId` and is stamped with `causationId` = the event's identifier. `append()` applies the active correlation data to each event as it is staged, so appended events carry the correct lineage.
- Correlation is configured in one place via `app.correlationDataProvider(...)`, defaulting to a single `messageOriginProvider()`. The configured providers feed the command/query handler extract step, the per-event seeding in every event processor, and the dispatch/append application. Correlation data is applied exactly once, at staging.
- New `contributeCorrelationData(partial)` adds lineage keys to the active UnitOfWork (e.g. an OpenTelemetry `traceparent`) so they ride along on outgoing and appended messages. `applyCorrelationData(message, providers)` is exported for reuse.
- Tracing: handler spans are now created from the message being handled and re-parent across the message boundary — command/query handlers continue the current trace, event handlers start a new trace linked to the triggering event. The span is made the active context for the duration of handling (`Span.runActive`), and its trace context is captured onto the UnitOfWork so appended and dispatched messages — including those published at commit time — carry it. `SpanFactory` gains optional `createLinkedHandlerSpan` and `currentTraceContext`.
- The command bus now traces dispatch only; the handler enhancer is the single source of handler spans, so a command no longer gets a duplicate handle span.
