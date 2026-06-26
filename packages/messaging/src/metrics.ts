import type { HandlerEnhancerDefinition, HandlerMetadata } from "./handler-enhancer.js"
import type { Message } from "./message.js"

/**
 * Attribute set attached to a metric measurement. Values are restricted to the
 * primitives supported by common metrics backends.
 */
export type MetricAttributes = Record<string, string | number | boolean>

/** Options for creating an instrument. */
export interface InstrumentOptions {
  /** Human-readable description of what the instrument measures. */
  description?: string
  /** Unit of measure, e.g. "ms" or "1". */
  unit?: string
}

/** A monotonically increasing counter. */
export interface Counter {
  add(value: number, attributes?: MetricAttributes): void
}

/** A distribution of recorded values (e.g. durations). */
export interface Histogram {
  record(value: number, attributes?: MetricAttributes): void
}

/**
 * Backend-agnostic metrics seam, analogous to {@link import("./span-factory.js").SpanFactory}
 * for tracing. Implementations (e.g. OpenTelemetry) create the actual
 * instruments; the framework records measurements through this interface.
 *
 * Instruments are expected to be idempotent by name — calling `counter("x")`
 * twice returns instruments that write to the same series.
 */
export interface MetricsRecorder {
  counter(name: string, options?: InstrumentOptions): Counter
  histogram(name: string, options?: InstrumentOptions): Histogram
}

/** A recorder that drops every measurement. Default when no metrics are configured. */
export function noOpMetricsRecorder(): MetricsRecorder {
  const counter: Counter = { add() {} }
  const histogram: Histogram = { record() {} }
  return {
    counter() { return counter },
    histogram() { return histogram },
  }
}

/** Options for {@link meteringHandlerEnhancerDefinition}. */
export interface MeteringOptions {
  /** Metric name prefix. Default: "kronos". */
  namespace?: string
}

function isMessage(value: unknown): value is Message {
  return (
    typeof value === "object" &&
    value !== null &&
    "metadata" in value &&
    "identifier" in value
  )
}

/**
 * Handler enhancer that records metrics for every handler invocation. Composes
 * alongside other enhancers (e.g. tracing) and fires uniformly for command,
 * query, and event handlers.
 *
 * Records, attributed by `message_type` / `message_name` / `handler_group`:
 * - `<ns>.messages.handled` (counter) — also tagged `outcome` = success | failure
 * - `<ns>.message.handler.duration` (histogram, ms) — handler execution time
 * - `<ns>.event.processing.lag` (histogram, ms) — for event handlers, the delay
 *   between the event's authored timestamp and the moment it was handled
 *
 * Note: this measures handler *invocations*. Dispatch-side counts (e.g. commands
 * with no handler, or ignored events) are not captured here.
 */
export function meteringHandlerEnhancerDefinition(
  recorder: MetricsRecorder,
  options: MeteringOptions = {},
): HandlerEnhancerDefinition {
  const ns = options.namespace ?? "kronos"
  const handled = recorder.counter(`${ns}.messages.handled`, {
    description: "Count of message handler invocations",
    unit: "1",
  })
  const duration = recorder.histogram(`${ns}.message.handler.duration`, {
    description: "Message handler execution time",
    unit: "ms",
  })
  const lag = recorder.histogram(`${ns}.event.processing.lag`, {
    description: "Delay between an event's timestamp and when it was handled",
    unit: "ms",
  })

  return {
    wrapHandler<T extends (...args: any[]) => any>(handler: T, metadata: HandlerMetadata): T {
      const base: MetricAttributes = {
        message_type: metadata.messageType,
        message_name: metadata.messageName,
        handler_group: metadata.handlerGroup,
      }
      const isEvent = metadata.messageType === "event"

      return (async (...args: any[]) => {
        const start = performance.now()
        let outcome = "success"
        try {
          return await handler(...args)
        } catch (err) {
          outcome = "failure"
          throw err
        } finally {
          duration.record(performance.now() - start, base)
          handled.add(1, { ...base, outcome })
          if (isEvent) {
            const message = args[0]
            if (isMessage(message) && typeof message.timestamp === "number") {
              lag.record(Math.max(0, Date.now() - message.timestamp), {
                message_name: metadata.messageName,
                handler_group: metadata.handlerGroup,
              })
            }
          }
        }
      }) as unknown as T
    },
  }
}
