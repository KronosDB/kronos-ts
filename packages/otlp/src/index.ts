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
  type Measurement,
  type OtlpExporter,
  type OtlpExporterOptions,
  type OtlpSpan,
  type SpanKindValue,
  type StartSpanOptions,
  type TraceContext,
} from "./otlp-exporter.js"

export { otlpCommandBus, otlpQueryBus } from "./otlp-bus.js"

export { otlpHandler } from "./otlp-handler.js"

export { otlpMetricsHandler } from "./otlp-metrics-handler.js"

// W3C trace-context plumbing — exported because a transport or an edge that
// wants to join the same trace needs exactly these two, and hand-rolling a
// second traceparent parser next door would be the actual duplication.
export { TRACEPARENT, formatTraceparent, traceparentOf, withTraceparent } from "./traceparent.js"
