---
"@kronos-ts/messaging": minor
"@kronos-ts/opentelemetry": minor
---

Add a metrics seam and OpenTelemetry metrics extension.

- New backend-agnostic `MetricsRecorder` (counter/histogram instruments that take attributes) and `noOpMetricsRecorder()` in the messaging core — the metrics analogue of `SpanFactory`.
- `meteringHandlerEnhancerDefinition(recorder)` records per-invocation metrics uniformly for command/query/event handlers: a `messages.handled` counter (tagged with `outcome` = success | failure), a `message.handler.duration` histogram, and an `event.processing.lag` histogram (delay between an event's timestamp and when it was handled). The metric namespace is configurable.
- `@kronos-ts/opentelemetry` adds `createOpenTelemetryMetricsRecorder()` (over the OpenTelemetry Metrics API) and an `openTelemetryMetrics()` extension that wires the metering enhancer. Compose it alongside `openTelemetry()`.
