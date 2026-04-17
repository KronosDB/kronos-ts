export {
  createOpenTelemetrySpanFactory,
  type OpenTelemetrySpanFactoryOptions,
} from "./opentelemetry-span-factory.js"

export {
  openTelemetryEnhancer,
} from "./opentelemetry-configuration-enhancer.js"

// Re-export tracing command bus for convenience
export { createTracingCommandBus } from "@kronos-ts/messaging"
