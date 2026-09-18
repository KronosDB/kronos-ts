---
"@kronos-ts/otlp": minor
---

Metrics come with the handler span, and custom metrics are as easy as a log line.

- `otlpHandler` records `kronos.messages.handled`, `kronos.messages.failed` and `kronos.message.handler.duration` itself, keyed by the same name as its span. `kronos.messages.failed` carries `error_type`, the error's constructor name, so a typed domain rejection is its own series.
- `otlpHandler` supplies `ctx.metrics` with `count(name, attributes?, options?)` and `record(name, value, attributes?, options?)`, bound to the handler's message name and kind. `metrics(exporter, bound?)` builds one elsewhere; `inertMetrics` records nothing.
- Removed: `otlpMetricsHandler`. Delete it from handler stacks; `otlpHandler` covers it. Whether metrics are exported is the exporter's switch: `otlpExporter({ metrics: false })`.
- The exporter is complete enough to replace an OpenTelemetry SDK setup: `headers` for authenticated backends; `resource` for attributes such as `service.version`; `endpoints` for a URL per signal; `compression: "gzip"`; `sample`, a parent-based head sampler taking a ratio (deterministic in the trace id) or a function, which leaves metrics and logs exact; histogram `bounds` per series; and `maxSeriesPerMetric` (default 1000), past which new attribute combinations are dropped, counted into `kronos.otlp.dropped` and reported once to `onExportError` naming the metric.
- `traceparent` now carries the sampled flag both ways: an unsampled incoming trace stays unsampled, and an unsampled span propagates `-00` so downstream services skip it too.
