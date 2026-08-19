---
"@kronos-ts/core": minor
"@kronos-ts/otlp": minor
---

`@kronos-ts/opentelemetry` is REMOVED, replaced by `@kronos-ts/otlp`: the protocol, not the ecosystem.

The old package took the OpenTelemetry API as a peer and an SDK pair as dev
dependencies, and reached core through a pair of seams core had to carry for its
benefit — `SpanFactory` and `MetricsRecorder`, plus `tracingHandler` and
`meteringHandler`. Those seams are DELETED from core, which now contains ZERO
tracing vocabulary. Observability is a package of functions over the public
shapes, which anybody could have written — so it is one.

`@kronos-ts/otlp` speaks OTLP/JSON over `fetch` and depends on
`@kronos-ts/core` and nothing else. No SDK, no global tracer, no patching, and
no OpenTelemetry dependency anywhere in the tree.

```ts
const exporter = otlpExporter({ endpoint: "http://collector:4318", serviceName: "billing" })

const commandBus = otlpCommandBus(interceptingCommandBus(bus, lineage), exporter)
const handlers   = slice.commandHandlers.map((h) => ({ ...h, handler: otlpHandler(h.handler, exporter) }))
```

- `otlpExporter({ endpoint, serviceName, flushIntervalMs? })` — a resource:
  batches spans and metrics, flushes on an interval, POSTs to `/v1/traces` and
  `/v1/metrics`, and `close()` flushes then stops. W3C trace and span ids are
  generated here; 64-bit nanosecond times are encoded as strings, as OTLP/JSON
  requires.
- `otlpCommandBus(bus, exporter)` / `otlpQueryBus(bus, exporter)` — a span per
  dispatch, with `traceparent` injected into the outgoing message metadata.
- `otlpHandler(handler, exporter, label?)` — wraps the handler FUNCTION and
  extracts `traceparent` from the handled message. Command and query MESSAGES
  become CHILDREN of the extracted context; event messages get their own trace
  with a LINK back to the producing span, so a projection catching up over a
  batch of old events is not swallowed into whatever produced them. Which leg it
  is comes from `message.kind` — there is no kind argument and no entry to ask.
- `otlpMetricsHandler(handler, exporter, label?)` — duration, throughput and failure
  counters, sliced by `message_type` and `message_name`, both read off the
  message.
- `label` ABSENT names the span (and keys the series) by the message's qualified
  name. Pass a `(message: Message) => string` to name it otherwise — a function
  OF THE MESSAGE, never a per-handler string closed over at wiring time.

otel-js interop is a consumer concern: write wrappers over the same public
shapes. That was always the honest boundary, and pretending otherwise cost core
two seams.
