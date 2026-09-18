import type { Attributes, OtlpExporter } from "./otlp-exporter.js"

// ---------------------------------------------------------------------------
// CUSTOM METRICS, AS EASY AS A LOG LINE.
//
// `otlpHandler` supplies one of these as `ctx.metrics`, bound to the handler's
// message name and kind — so every custom series is sliceable by which handler
// emitted it, and a handler never closes over the exporter:
//
//   ctx.metrics.count("orders.rejected", { reason: "out-of-stock" })
//   ctx.metrics.record("payments.provider.latency", ms, { provider: "adyen" }, { unit: "ms" })
//
// What a metric is FOR: something you graph or alert on over time without
// caring which specific entity it was — an outcome, a reason, a provider. An
// id is never an attribute; that is what a log line or a span is for. Business
// facts themselves are events, and a projection counts those better than a
// metric does. The exporter enforces the difference: past a cap of distinct
// attribute combinations per metric it drops the excess, counts it, and says
// which metric it was.
// ---------------------------------------------------------------------------

export type MetricOptions = {
  /** UCUM unit, e.g. `"ms"` or `"1"`. */
  readonly unit?: string
  readonly description?: string
  /** Histogram bucket boundaries for `record`. The defaults fit millisecond durations. */
  readonly bounds?: readonly number[]
}

export type Metrics = {
  /** Add to a monotonic counter. `by` defaults to 1. */
  count(name: string, attributes?: Attributes, options?: MetricOptions & { readonly by?: number }): void
  /** Record a value into a histogram. */
  record(name: string, value: number, attributes?: Attributes, options?: MetricOptions): void
}

/** The capability `otlpHandler` supplies. A handler names it to reach `ctx.metrics`. */
export type MetricsCapability = {
  readonly metrics: Metrics
}

/**
 * A {@link Metrics} over `exporter`, with `bound` merged under every call's
 * attributes (the call's own win). `otlpHandler` binds the handled message's
 * name and kind; an edge can build one with its own low-cardinality context.
 */
export function metrics(exporter: OtlpExporter, bound: Attributes = {}): Metrics {
  return {
    count(name, attributes, options) {
      exporter.addCount({
        name,
        value: options?.by ?? 1,
        ...(options?.unit ? { unit: options.unit } : {}),
        ...(options?.description ? { description: options.description } : {}),
        attributes: { ...bound, ...attributes },
      })
    },
    record(name, value, attributes, options) {
      exporter.recordHistogram({
        name,
        value,
        ...(options?.unit ? { unit: options.unit } : {}),
        ...(options?.description ? { description: options.description } : {}),
        ...(options?.bounds ? { bounds: options.bounds } : {}),
        attributes: { ...bound, ...attributes },
      })
    },
  }
}

/** Records nothing. For code that takes an optional `Metrics` in a deployment without an exporter. */
export const inertMetrics: Metrics = {
  count: () => {},
  record: () => {},
}
