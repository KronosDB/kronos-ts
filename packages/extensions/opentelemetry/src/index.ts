export {
  createOpenTelemetrySpanFactory,
  type OpenTelemetrySpanFactoryOptions,
} from "./opentelemetry-span-factory.js"

export { openTelemetry } from "./opentelemetry.js"

// Re-export tracing command bus for convenience
export { createTracingCommandBus } from "@kronos-ts/messaging"
