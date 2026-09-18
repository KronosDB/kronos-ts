// ---------------------------------------------------------------------------
// @kronos-ts/otlp — the protocol, not the ecosystem.
//
// OTLP/JSON over `fetch`, W3C trace context over message metadata, and four
// wrappers over public core shapes. Zero dependencies on the OpenTelemetry
// npm scope: no SDK, no global tracer, no context manager, no patching,
// nothing to initialize before the first import. Interop with otel-js is a consumer
// writing the same four wrappers against their own tracer — the shapes here
// are ordinary, and so is that.
// ---------------------------------------------------------------------------

export {
  otlpExporter,
  spanId,
  traceId,
  SpanKind,
  type Attributes,
  type AttributeValue,
  type LogExport,
  type Measurement,
  type OtlpExporter,
  type OtlpExporterOptions,
  type OtlpSpan,
  type SpanKindValue,
  type StartSpanOptions,
  type TraceContext,
} from "./otlp-exporter.js"

export { otlpCommandBus, otlpQueryBus } from "./otlp-bus.js"

export { otlpHandler, type TraceCapability } from "./otlp-handler.js"

// The trace SCOPE — the value that says "you are here". `otlpHandler` supplies
// one as `ctx.trace`; an edge builds one with `trace(exporter, parent)`; a
// client that takes an optional one falls back to `inertTrace`.
export { trace, inertTrace, type Trace, type SpanOptions } from "./trace.js"

// The core `Logger` contract over `/v1/logs`. Pair it with core's
// `loggingHandler`; `consoleLogger` is the no-exporter alternative.
export { otlpLogger, type OtlpLoggerOptions } from "./otlp-logger.js"

// Custom metrics, as easy as a log line. `otlpHandler` supplies one as
// `ctx.metrics`, bound to the handler's message name and kind; the three
// standard series come with the handler span itself.
export { metrics, inertMetrics, type Metrics, type MetricsCapability, type MetricOptions } from "./metrics.js"

// W3C trace-context plumbing — exported because a transport or an edge that
// wants to join the same trace needs exactly these two, and hand-rolling a
// second traceparent parser next door would be the actual duplication.
export { TRACEPARENT, formatTraceparent, traceparentOf, withTraceparent } from "./traceparent.js"
