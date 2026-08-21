// ---------------------------------------------------------------------------
// The OTLP wire, hand-rolled.
//
// Nothing from the OpenTelemetry npm scope: no SDK, no global tracer, no
// context manager, no propagator, no instrumentation patching. A span is a record we
// push onto an array; a trace id is sixteen random bytes; a batch is a JSON
// POST. That is the whole protocol, and it is small enough to own.
// ---------------------------------------------------------------------------

/** The attribute value types OTLP/JSON can carry without a schema. */
export type AttributeValue = string | number | boolean

/** A set of attributes attached to a span or a metric data point. */
export type Attributes = Readonly<Record<string, AttributeValue>>

/**
 * The two ids that identify a span inside a trace — the whole of what a
 * W3C `traceparent` carries across a message boundary.
 */
export type TraceContext = {
  readonly traceId: string
  readonly spanId: string
}

/**
 * OTLP span kinds, by their wire numbers.
 *
 * PRODUCER/CONSUMER for the asynchronous legs (a command dispatched onto a
 * bus, an event delivered by a processor), CLIENT/SERVER for the
 * request/response leg (a query and the handler that answers it).
 */
export const SpanKind = {
  INTERNAL: 1,
  SERVER: 2,
  CLIENT: 3,
  PRODUCER: 4,
  CONSUMER: 5,
} as const

export type SpanKindValue = (typeof SpanKind)[keyof typeof SpanKind]

/** OTLP status codes. */
const StatusCode = { UNSET: 0, OK: 1, ERROR: 2 } as const

/** Everything that decides the shape of a span, in one order-free record. */
export type StartSpanOptions = {
  readonly name: string
  readonly kind: SpanKindValue
  /**
   * The span this one CONTINUES: same trace, `parentSpanId` set. Mutually
   * exclusive with `links` in practice, though nothing here forbids both.
   */
  readonly parent?: TraceContext
  /**
   * The spans this one POINTS AT without being nested under them. A linked
   * span is the root of its own trace.
   */
  readonly links?: readonly TraceContext[]
  readonly attributes?: Attributes
}

/** A started span. Ends exactly once — the second `end()`/`fail()` is a no-op. */
export type OtlpSpan = TraceContext & {
  /** End with status OK. */
  end(): void
  /** End with status ERROR, recording `error`'s message. */
  fail(error: unknown): void
}

/** One measurement handed to the exporter. */
export type Measurement = {
  readonly name: string
  readonly value: number
  /** UCUM unit, e.g. `"ms"` or `"1"`. */
  readonly unit?: string
  readonly description?: string
  readonly attributes?: Attributes
}

export type OtlpExporterOptions = {
  /** Collector base URL — `/v1/traces` and `/v1/metrics` are appended. */
  readonly endpoint: string
  /** Value of the `service.name` resource attribute. */
  readonly serviceName: string
  /** How often the batch is POSTed. Default 5000ms. */
  readonly flushIntervalMs?: number
}

/**
 * The RESOURCE: it owns a buffer, a timer and a socket's worth of work.
 * Build one per process, hand it to the wrappers, `close()` it on shutdown.
 */
export type OtlpExporter = {
  /** Start a span. It enters the batch when it ends. */
  startSpan(options: StartSpanOptions): OtlpSpan
  /** Add to a monotonic sum, keyed by name + unit + attributes. */
  addCount(measurement: Measurement): void
  /** Record into an explicit-bucket histogram, keyed the same way. */
  recordHistogram(measurement: Measurement): void
  /** POST whatever is buffered. Never rejects — telemetry must not break a host. */
  flush(): Promise<void>
  /** Stop the flush loop, then flush what is left. */
  close(): Promise<void>
}

// ---------------------------------------------------------------------------
// W3C ids — 16 random bytes / 8 random bytes, lowercase hex
// ---------------------------------------------------------------------------

const HEX = "0123456789abcdef"

function randomHex(byteLength: number): string {
  const bytes = new Uint8Array(byteLength)
  crypto.getRandomValues(bytes)
  let out = ""
  for (const byte of bytes) {
    out += HEX[byte >> 4]
    out += HEX[byte & 0x0f]
  }
  return out
}

/** A W3C trace id: 16 random bytes as 32 lowercase hex characters. */
export function traceId(): string {
  return randomHex(16)
}

/** A W3C span id: 8 random bytes as 16 lowercase hex characters. */
export function spanId(): string {
  return randomHex(8)
}

// ---------------------------------------------------------------------------
// OTLP/JSON encoding helpers
// ---------------------------------------------------------------------------

type KeyValue = {
  key: string
  value: Record<string, unknown>
}

function anyValue(value: AttributeValue): Record<string, unknown> {
  if (typeof value === "string") return { stringValue: value }
  if (typeof value === "boolean") return { boolValue: value }
  return Number.isInteger(value) ? { intValue: String(value) } : { doubleValue: value }
}

function keyValues(attributes: Attributes | undefined): KeyValue[] {
  if (!attributes) return []
  return Object.entries(attributes).map(([key, value]) => ({ key, value: anyValue(value) }))
}

/**
 * Epoch milliseconds as an OTLP/JSON nanosecond STRING.
 *
 * Via BigInt on purpose: `Date.now() * 1e6` is ~1.8e18, far past
 * `Number.MAX_SAFE_INTEGER`, so the obvious version silently loses the low
 * digits. Microsecond precision is kept exactly; the last three digits are
 * zeros because that is genuinely all we know.
 */
function unixNano(epochMs: number): string {
  return String(BigInt(Math.round(epochMs * 1000)) * 1000n)
}

/** Default explicit histogram bucket boundaries, in the instrument's unit. */
const DEFAULT_BOUNDS = [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]

/** Instrument identity: same name + unit + attribute set = same series. */
function seriesKey(name: string, unit: string, attributes: Attributes | undefined): string {
  const entries = Object.entries(attributes ?? {}).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
  return JSON.stringify([name, unit, entries])
}

// ---------------------------------------------------------------------------
// Buffered records
// ---------------------------------------------------------------------------

type SpanRecord = {
  traceId: string
  spanId: string
  parentSpanId?: string
  name: string
  kind: SpanKindValue
  startTimeUnixNano: string
  endTimeUnixNano: string
  attributes: KeyValue[]
  links: { traceId: string; spanId: string }[]
  status: { code: number; message?: string }
}

type SumSeries = {
  name: string
  unit: string
  description?: string
  attributes: Attributes | undefined
  value: number
}

type HistogramSeries = {
  name: string
  unit: string
  description?: string
  attributes: Attributes | undefined
  count: number
  sum: number
  min: number
  max: number
  buckets: number[]
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/**
 * A batching OTLP/JSON exporter over `fetch`.
 *
 * ```ts
 * const exporter = otlpExporter({ endpoint: "http://localhost:4318", serviceName: "billing" })
 * const commandBus = otlpCommandBus(interceptingCommandBus(localCommandBus(uow), correlation), exporter)
 * // …
 * await exporter.close()
 * ```
 *
 * Export failures are SWALLOWED. A collector that is down, slow or wrong must
 * not turn into a failed command: the batch is dropped and the process keeps
 * serving. That is the one asymmetry this package insists on.
 */
export function otlpExporter(options: OtlpExporterOptions): OtlpExporter {
  const { endpoint, serviceName } = options
  const flushIntervalMs = options.flushIntervalMs ?? 5000
  const base = endpoint.endsWith("/") ? endpoint.slice(0, -1) : endpoint

  let spans: SpanRecord[] = []
  let sums = new Map<string, SumSeries>()
  let histograms = new Map<string, HistogramSeries>()
  let windowStartMs = Date.now()
  let closed = false
  /** Serializes flushes so two POSTs of the same batch can never overlap. */
  let pending: Promise<void> = Promise.resolve()

  const resource = { attributes: keyValues({ "service.name": serviceName }) }
  const scope = { name: "@kronos-ts/otlp" }

  async function post(path: string, body: unknown): Promise<void> {
    try {
      await globalThis.fetch(`${base}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      })
    } catch {
      // Dropped on purpose — see the note on otlpExporter.
    }
  }

  function drainSpans(): unknown | undefined {
    if (spans.length === 0) return undefined
    const batch = spans
    spans = []
    return { resourceSpans: [{ resource, scopeSpans: [{ scope, spans: batch }] }] }
  }

  function drainMetrics(): unknown | undefined {
    if (sums.size === 0 && histograms.size === 0) return undefined
    const startTimeUnixNano = unixNano(windowStartMs)
    const timeUnixNano = unixNano(Date.now())
    windowStartMs = Date.now()

    const metrics: unknown[] = []
    for (const series of sums.values()) {
      metrics.push({
        name: series.name,
        unit: series.unit,
        ...(series.description ? { description: series.description } : {}),
        sum: {
          dataPoints: [
            {
              attributes: keyValues(series.attributes),
              startTimeUnixNano,
              timeUnixNano,
              asInt: String(Math.round(series.value)),
            },
          ],
          // DELTA: each flush reports the window, then resets. A batching
          // exporter has no business pretending to remember a process total.
          aggregationTemporality: 1,
          isMonotonic: true,
        },
      })
    }
    for (const series of histograms.values()) {
      metrics.push({
        name: series.name,
        unit: series.unit,
        ...(series.description ? { description: series.description } : {}),
        histogram: {
          dataPoints: [
            {
              attributes: keyValues(series.attributes),
              startTimeUnixNano,
              timeUnixNano,
              count: String(series.count),
              sum: series.sum,
              min: series.min,
              max: series.max,
              bucketCounts: series.buckets.map(String),
              explicitBounds: DEFAULT_BOUNDS,
            },
          ],
          aggregationTemporality: 1,
        },
      })
    }
    sums = new Map()
    histograms = new Map()
    return { resourceMetrics: [{ resource, scopeMetrics: [{ scope, metrics }] }] }
  }

  async function doFlush(): Promise<void> {
    const traces = drainSpans()
    const metrics = drainMetrics()
    if (traces) await post("/v1/traces", traces)
    if (metrics) await post("/v1/metrics", metrics)
  }

  function flush(): Promise<void> {
    pending = pending.then(doFlush)
    return pending
  }

  const timer = setInterval(() => {
    void flush()
  }, flushIntervalMs)
  // A telemetry timer must never be the reason a process refuses to exit.
  ;(timer as { unref?: () => void }).unref?.()

  return {
    startSpan(spanOptions: StartSpanOptions): OtlpSpan {
      const id = spanId()
      const trace = spanOptions.parent?.traceId ?? traceId()
      const startEpochMs = Date.now()
      const startPerf = performance.now()
      let ended = false

      const finish = (status: { code: number; message?: string }) => {
        if (ended) return
        ended = true
        spans.push({
          traceId: trace,
          spanId: id,
          ...(spanOptions.parent ? { parentSpanId: spanOptions.parent.spanId } : {}),
          name: spanOptions.name,
          kind: spanOptions.kind,
          startTimeUnixNano: unixNano(startEpochMs),
          endTimeUnixNano: unixNano(startEpochMs + (performance.now() - startPerf)),
          attributes: keyValues(spanOptions.attributes),
          links: (spanOptions.links ?? []).map((link) => ({
            traceId: link.traceId,
            spanId: link.spanId,
          })),
          status,
        })
      }

      return {
        traceId: trace,
        spanId: id,
        end: () => finish({ code: StatusCode.OK }),
        fail: (error: unknown) => finish({ code: StatusCode.ERROR, message: errorMessage(error) }),
      }
    },

    addCount(measurement: Measurement): void {
      const unit = measurement.unit ?? "1"
      const key = seriesKey(measurement.name, unit, measurement.attributes)
      const existing = sums.get(key)
      if (existing) {
        existing.value += measurement.value
        return
      }
      sums.set(key, {
        name: measurement.name,
        unit,
        ...(measurement.description ? { description: measurement.description } : {}),
        attributes: measurement.attributes,
        value: measurement.value,
      })
    },

    recordHistogram(measurement: Measurement): void {
      const unit = measurement.unit ?? "1"
      const key = seriesKey(measurement.name, unit, measurement.attributes)
      let series = histograms.get(key)
      if (!series) {
        series = {
          name: measurement.name,
          unit,
          ...(measurement.description ? { description: measurement.description } : {}),
          attributes: measurement.attributes,
          count: 0,
          sum: 0,
          min: Number.POSITIVE_INFINITY,
          max: Number.NEGATIVE_INFINITY,
          buckets: new Array<number>(DEFAULT_BOUNDS.length + 1).fill(0),
        }
        histograms.set(key, series)
      }
      series.count += 1
      series.sum += measurement.value
      series.min = Math.min(series.min, measurement.value)
      series.max = Math.max(series.max, measurement.value)
      let bucket = DEFAULT_BOUNDS.findIndex((bound) => measurement.value <= bound)
      if (bucket === -1) bucket = DEFAULT_BOUNDS.length
      series.buckets[bucket] = (series.buckets[bucket] ?? 0) + 1
    },

    flush,

    close(): Promise<void> {
      if (!closed) {
        closed = true
        clearInterval(timer)
      }
      return flush()
    },
  }
}
