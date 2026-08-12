import {
  createTracingCommandBus,
  tracingHandlerEnhancerDefinition,
  meteringHandlerEnhancerDefinition,
  type CommandBus,
  type HandlerEnhancerDefinition,
  type MeteringOptions,
  type SpanFactory,
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
 * OpenTelemetry tracing for Kronos — plain factories, no app mutation.
 *
 * There is no decorator pipeline any more. Bus decoration and handler
 * enhancement are ordinary function composition, wired by the caller in
 * their composition root:
 *
 * ```typescript
 * const { spanFactory, handlerEnhancer } = openTelemetry()
 *
 * const commandBus = createTracingCommandBus(baseCommandBus, spanFactory)
 *
 * registerCommandHandlersNatively(commands, {
 *   commandBus,
 *   config,
 *   moduleName: "billing",
 *   handlerEnhancer,
 * })
 * ```
 *
 * ## Ordering invariant — wrap tracing AFTER any distributed-bus decoration
 *
 * For tracing to wrap the distributed bus, apply `createTracingCommandBus`
 * AFTER wrapping with a distributed-bus adapter (e.g. `kronosDb` / `axonServer`),
 * so tracing is the outermost wrapper:
 *
 * ```typescript
 * const commandBus = createTracingCommandBus(
 *   distributedCommandBus(baseCommandBus, ...),
 *   spanFactory,
 * )
 * ```
 *
 * If tracing wraps the in-memory delegate BEFORE the distributed-bus wrapper
 * is applied, no spans are emitted on the wire.
 */
export interface OpenTelemetryTracing {
  /** The underlying SpanFactory — pass to `createTracingCommandBus` or reuse directly. */
  readonly spanFactory: SpanFactory
  /** Handler-side tracing — pass as `handlerEnhancer` to the native registration helpers. */
  readonly handlerEnhancer: HandlerEnhancerDefinition
}

export function openTelemetry(
  options: OpenTelemetrySpanFactoryOptions = {},
): OpenTelemetryTracing {
  const spanFactory = createOpenTelemetrySpanFactory(options)
  return {
    spanFactory,
    handlerEnhancer: tracingHandlerEnhancerDefinition(spanFactory),
  }
}

/** Convenience: wrap a command bus with tracing using a freshly built span factory. */
export function tracingCommandBus(
  delegate: CommandBus,
  spanFactory: SpanFactory,
): CommandBus {
  return createTracingCommandBus(delegate, spanFactory)
}

/**
 * OpenTelemetry metrics for Kronos — a plain `HandlerEnhancerDefinition`.
 *
 * Records throughput, latency, error rate, and event-processing lag for every
 * command/query/event handler invocation, using the OpenTelemetry Metrics API.
 * Pass it as `handlerEnhancer` alongside (or instead of) {@link openTelemetry}'s:
 *
 * ```typescript
 * const tracing = openTelemetry()
 * const metrics = openTelemetryMetrics()
 *
 * const handlerEnhancer = multiHandlerEnhancerDefinition([
 *   tracing.handlerEnhancer,
 *   metrics,
 * ])
 * ```
 *
 * Requires an OpenTelemetry MeterProvider to be configured for measurements to
 * be exported; without one this is effectively a no-op.
 */
export function openTelemetryMetrics(
  options: OpenTelemetryMetricsRecorderOptions & MeteringOptions = {},
): HandlerEnhancerDefinition {
  const recorder = createOpenTelemetryMetricsRecorder(options)
  return meteringHandlerEnhancerDefinition(recorder, options)
}
