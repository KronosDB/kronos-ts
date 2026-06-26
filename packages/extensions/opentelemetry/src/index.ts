export {
  createOpenTelemetrySpanFactory,
  type OpenTelemetrySpanFactoryOptions,
} from "./opentelemetry-span-factory.js"

export { openTelemetry, openTelemetryMetrics } from "./opentelemetry.js"

export {
  createOpenTelemetryMetricsRecorder,
  type OpenTelemetryMetricsRecorderOptions,
} from "./opentelemetry-metrics-recorder.js"

export { createOpenTelemetryDeadLetterListener } from "./opentelemetry-dead-letter-listener.js"

// Re-export tracing command bus for convenience
export { createTracingCommandBus } from "@kronos-ts/messaging"
