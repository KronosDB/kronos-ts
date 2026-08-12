import { metrics, type Meter } from "@opentelemetry/api"
import type {
  MetricsRecorder,
  Counter,
  Histogram,
  InstrumentOptions,
} from "@kronos-ts/messaging"

export interface OpenTelemetryMetricsRecorderOptions {
  /** Meter to create instruments from. Defaults to `metrics.getMeter("kronos-framework")`. */
  meter?: Meter
}

/**
 * MetricsRecorder implementation backed by the OpenTelemetry Metrics API.
 *
 * Instruments are created lazily on the configured Meter; the OTel SDK
 * deduplicates instruments by name, so repeated `counter`/`histogram` calls for
 * the same name write to the same series. As with the span factory, you need an
 * OpenTelemetry MeterProvider configured (e.g. via the OTel SDK or java agent)
 * for measurements to be exported — otherwise this is effectively a no-op.
 */
export function openTelemetryMetricsRecorder(
  options: OpenTelemetryMetricsRecorderOptions = {},
): MetricsRecorder {
  const meter = options.meter ?? metrics.getMeter("kronos-framework")

  return {
    counter(name: string, opts?: InstrumentOptions): Counter {
      const instrument = meter.createCounter(name, {
        description: opts?.description,
        unit: opts?.unit,
      })
      return {
        add(value, attributes) {
          instrument.add(value, attributes)
        },
      }
    },
    histogram(name: string, opts?: InstrumentOptions): Histogram {
      const instrument = meter.createHistogram(name, {
        description: opts?.description,
        unit: opts?.unit,
      })
      return {
        record(value, attributes) {
          instrument.record(value, attributes)
        },
      }
    },
  }
}
