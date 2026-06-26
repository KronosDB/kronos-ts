import type { App } from "@kronos-ts/app"
import {
  createTracingCommandBus,
  tracingHandlerEnhancerDefinition,
  meteringHandlerEnhancerDefinition,
  type MeteringOptions,
} from "@kronos-ts/messaging"
import {
  createOpenTelemetrySpanFactory,
  type OpenTelemetrySpanFactoryOptions,
} from "./opentelemetry-span-factory.js"
import {
  createOpenTelemetryMetricsRecorder,
  type OpenTelemetryMetricsRecorderOptions,
} from "./opentelemetry-metrics-recorder.js"

/**
 * OpenTelemetry tracing extension for Kronos.
 *
 * Wires:
 * - Command tracing via `app.decorate('commandBus', ...)` — the resulting
 *   command bus emits a span per dispatch via {@link createTracingCommandBus}.
 * - Event/query handler tracing via `app.handlerEnhancer(...)` — each
 *   handler invocation gets a child span via {@link tracingHandlerEnhancerDefinition}.
 *
 * The internal `spanFactory` is a closure variable (NOT a typed slot, per D-87).
 * Both consumers (the bus decorator and the handler enhancer) live inside this
 * extension; there is no outside reader, so a typed slot would add cost without
 * benefit.
 *
 * ## Ordering invariant — apply tracing AFTER any distributed-bus extension
 *
 * For tracing to wrap the distributed bus, call `.use(openTelemetry())` AFTER
 * `.use(kronosDb(...))` or `.use(axonServer(...))` — decorator-registration
 * order is preserved by `.use()`, and the most-recently registered decorator
 * wraps the previously registered ones. If you call `.use(openTelemetry())`
 * BEFORE the distributed-bus extension, the tracing decorator wraps the
 * in-memory delegate that gets replaced by the slot factory at start time,
 * and no spans are emitted on the wire.
 *
 * @example
 * ```typescript
 * await kronos()
 *   .use(kronosDb({ ... }))     // 1. register the distributed bus first
 *   .use(openTelemetry())       // 2. then wrap with tracing AFTER
 *   .start()
 * ```
 */
export function openTelemetry(
  options: OpenTelemetrySpanFactoryOptions = {},
): (app: App) => void {
  return (app) => {
    const spanFactory = createOpenTelemetrySpanFactory(options)
    app.decorate("commandBus", (delegate) =>
      createTracingCommandBus(delegate, spanFactory),
    )
    app.handlerEnhancer(tracingHandlerEnhancerDefinition(spanFactory))
  }
}

/**
 * OpenTelemetry metrics extension for Kronos.
 *
 * Wires a metering handler enhancer (via `app.handlerEnhancer(...)`) that
 * records throughput, latency, error rate, and event-processing lag for every
 * command/query/event handler invocation, using the OpenTelemetry Metrics API.
 *
 * Compose independently of (and alongside) {@link openTelemetry}:
 *
 * @example
 * ```typescript
 * await kronos()
 *   .use(openTelemetry())        // tracing
 *   .use(openTelemetryMetrics()) // metrics
 *   .start()
 * ```
 *
 * Requires an OpenTelemetry MeterProvider to be configured for measurements to
 * be exported; without one this is effectively a no-op.
 */
export function openTelemetryMetrics(
  options: OpenTelemetryMetricsRecorderOptions & MeteringOptions = {},
): (app: App) => void {
  return (app) => {
    const recorder = createOpenTelemetryMetricsRecorder(options)
    app.handlerEnhancer(meteringHandlerEnhancerDefinition(recorder, options))
  }
}
