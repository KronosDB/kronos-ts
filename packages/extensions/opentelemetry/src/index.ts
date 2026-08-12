export {
  openTelemetrySpanFactory,
  type OpenTelemetrySpanFactoryOptions,
} from "./opentelemetry-span-factory.js"

export {
  openTelemetry,
  openTelemetryMetrics,
  type OpenTelemetryTracing,
} from "./opentelemetry.js"

export {
  openTelemetryMetricsRecorder,
  type OpenTelemetryMetricsRecorderOptions,
} from "./opentelemetry-metrics-recorder.js"

export { openTelemetryDeadLetterListener } from "./opentelemetry-dead-letter-listener.js"

// The tracing command-bus decorator lives in messaging; re-exported here so the
// OpenTelemetry surface is a single import.
export { tracingCommandBus } from "@kronos-ts/messaging"
