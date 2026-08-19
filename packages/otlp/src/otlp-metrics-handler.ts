import type { Message } from "@kronos-ts/core"
import type { Attributes, OtlpExporter } from "./otlp-exporter.js"
import { messageName } from "./otlp-handler.js"

// ---------------------------------------------------------------------------
// The three numbers you actually page on: how long, how many, how many failed.
// Separate from `otlpHandler` because metering and tracing are separate
// decisions — sample the traces, keep all the counters.
//
// Like `otlpHandler`, it reads nothing from the entry: the series it writes are
// keyed off the handled message.
// ---------------------------------------------------------------------------

const DURATION = "kronos.message.handler.duration"
const HANDLED = "kronos.messages.handled"
const FAILED = "kronos.messages.failed"

/**
 * Wrap a handler function so each invocation records:
 *
 * - `kronos.message.handler.duration` (histogram, ms) — latency
 * - `kronos.messages.handled` (counter) — throughput
 * - `kronos.messages.failed` (counter) — failures, incremented in addition to
 *   `handled`, so an error rate is one division and never a subtraction of two
 *   series that were sampled at different moments
 *
 * All three carry the same attributes — `message_type` and `message_name`, both
 * read off the handled message — so they slice together.
 *
 * `label` ABSENT keys the series by the message's qualified name; pass one to
 * key them otherwise. It is a function OF THE MESSAGE, so the series stay
 * bounded by the message vocabulary rather than by wiring.
 *
 * ```ts
 * commandHandlers: commands.map((h) => ({
 *   ...h,
 *   handler: otlpMetricsHandler(otlpHandler(h.handler, exporter), exporter),
 * }))
 * ```
 */
export function otlpMetricsHandler<M extends Message, C, R>(
  next: (message: M, context: C) => R,
  exporter: OtlpExporter,
  label?: (message: Message) => string,
): (message: M, context: C) => Promise<Awaited<R>> {
  return async (message, context): Promise<Awaited<R>> => {
    const attributes: Attributes = {
      message_type: message.kind,
      message_name: label ? label(message) : messageName(message),
    }
    const started = performance.now()
    try {
      const result = await next(message, context)
      exporter.addCount({
        name: HANDLED,
        value: 1,
        unit: "1",
        description: "Count of message handler invocations",
        attributes,
      })
      return result
    } catch (error) {
      exporter.addCount({
        name: HANDLED,
        value: 1,
        unit: "1",
        description: "Count of message handler invocations",
        attributes,
      })
      exporter.addCount({
        name: FAILED,
        value: 1,
        unit: "1",
        description: "Count of message handler invocations that threw",
        attributes,
      })
      throw error
    } finally {
      exporter.recordHistogram({
        name: DURATION,
        value: performance.now() - started,
        unit: "ms",
        description: "Handler invocation duration",
        attributes,
      })
    }
  }
}
