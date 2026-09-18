import type { LogLevel } from "@kronos-ts/core"

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

/** A set of attributes attached to a span, a log record or a metric data point. */
export type Attributes = Readonly<Record<string, AttributeValue>>

/**
 * The two ids that identify a span inside a trace — the whole of what a
 * W3C `traceparent` carries across a message boundary.
 */
export type TraceContext = {
  readonly traceId: string
  readonly spanId: string
  /**
   * The W3C sampled flag. `false` means the trace was decided NOT to be
   * recorded at its root; every span under it follows. Absent means sampled.
   */
  readonly sampled?: boolean
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
  /**
   * End with status ERROR. Records the error's TYPE (its constructor name) —
   * not its message, which may carry user data — unless the exporter was
   * built with `errors: "message"`.
   */
  fail(error: unknown): void
}

/** One measurement handed to the exporter. */
export type Measurement = {
  readonly name: string
  readonly value: number
  /**
   * Histogram bucket boundaries, in the instrument's unit. The defaults fit
   * millisecond durations; an amount or a size wants its own. Fixed by the
   * first record of a series.
   */
  readonly bounds?: readonly number[]
  /** UCUM unit, e.g. `"ms"` or `"1"`. */
  readonly unit?: string
  readonly description?: string
  readonly attributes?: Attributes
}

/** One log record handed to the exporter. What `otlpLogger` builds; hosts rarely call this directly. */
export type LogExport = {
  /** Epoch milliseconds. */
  readonly time: number
  readonly level: LogLevel
  readonly message: string
  readonly attributes?: Attributes
  /** The span the record was written under, when known. */
  readonly trace?: TraceContext
}

export type OtlpExporterOptions = {
  /** Collector base URL — `/v1/traces`, `/v1/metrics` and `/v1/logs` are appended. */
  readonly endpoint: string
  /** Value of the `service.name` resource attribute. */
  readonly serviceName: string
  /**
   * Extra RESOURCE attributes, sent once per batch beside `service.name` —
   * `service.version`, `deployment.environment.name`, `service.instance.id`.
   * What identifies the process, never anything per request.
   */
  readonly resource?: Attributes
  /**
   * Extra HTTP headers on every POST — how a hosted backend authenticates
   * (`authorization`, `x-honeycomb-team`, …). `content-type` is always JSON.
   */
  readonly headers?: Readonly<Record<string, string>>
  /** How often the batch is POSTed. Default 5000ms. */
  readonly flushIntervalMs?: number
  /** Deadline for one POST. A collector that hangs cannot hold `close()` past this. Default 10000ms. */
  readonly exportTimeoutMs?: number
  /** Spans kept between flushes; beyond it the OLDEST are dropped and counted. Default 10000. */
  readonly maxBufferedSpans?: number
  /** Log records kept between flushes; same policy. Default 10000. */
  readonly maxBufferedLogs?: number
  /**
   * What `span.fail(error)` records. `"type"` (default) records only the
   * error's constructor name; `"message"` also records `error.message`. The
   * default is the privacy-safe one: an error message can carry anything.
   */
  readonly errors?: "type" | "message"
  /**
   * Full URLs per signal, for a backend that takes them at different places.
   * A signal not named here goes to `endpoint` + `/v1/<signal>`.
   */
  readonly endpoints?: { readonly traces?: string; readonly metrics?: string; readonly logs?: string }
  /** `"gzip"` compresses every POST body. Default: none. */
  readonly compression?: "gzip"
  /**
   * Head sampling, decided ONCE at the root of a trace and followed by every
   * span under it (parent-based): a ratio in `[0, 1]`, deterministic in the
   * trace id so every process agrees, or your own `(traceId) => boolean`. An
   * unsampled span records nothing but still carries its ids and an unsampled
   * `traceparent`, so downstream services skip the trace too. Metrics and
   * logs are NOT sampled — counters stay exact. Default: record everything.
   */
  readonly sample?: number | ((traceId: string) => boolean)
  /**
   * `false` records and exports NO metrics — for a deployment with no metrics
   * backend. Spans and logs are unaffected. Default `true`.
   */
  readonly metrics?: boolean
  /**
   * Distinct attribute combinations one metric name may have between flushes.
   * Past it, new combinations are dropped, counted into `kronos.otlp.dropped`
   * and reported once to `onExportError` — an id in an attribute is the usual
   * cause. Default 1000.
   */
  readonly maxSeriesPerMetric?: number
  /**
   * Told when a POST fails or is rejected. Telemetry never throws into the
   * host; this is the one place a host can learn its collector is unhappy.
   */
  readonly onExportError?: (error: unknown, signal: "traces" | "metrics" | "logs") => void
}

/**
 * The RESOURCE: it owns a buffer, a timer and a socket's worth of work.
 * Build one per process, hand it to the wrappers, `close()` it LAST on
 * shutdown — after the app stopped and every transport drained, so nothing
 * still running records into a closed exporter.
 */
export type OtlpExporter = {
  /** Start a span. It enters the batch when it ends. After `close()`, an inert span that records nothing. */
  startSpan(options: StartSpanOptions): OtlpSpan
  /** Add to a monotonic sum, keyed by name + unit + attributes. */
  addCount(measurement: Measurement): void
  /** Record into an explicit-bucket histogram, keyed the same way. */
  recordHistogram(measurement: Measurement): void
  /** Buffer a log record. Ignored after `close()`. */
  emitLog(record: LogExport): void
  /** POST whatever is buffered. Never rejects — telemetry must not break a host. */
  flush(): Promise<void>
  /** Stop intake, stop the flush loop, then flush what is left within the export deadline. */
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

/** OTLP severity numbers for the four levels. */
const SEVERITY: Record<LogLevel, { number: number; text: string }> = {
  debug: { number: 5, text: "DEBUG" },
  info: { number: 9, text: "INFO" },
  warn: { number: 13, text: "WARN" },
  error: { number: 17, text: "ERROR" },
}

/** The series every dropped record is counted into, by signal. */
const DROPPED = "kronos.otlp.dropped"

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

type LogRecordWire = {
  timeUnixNano: string
  severityNumber: number
  severityText: string
  body: { stringValue: string }
  attributes: KeyValue[]
  traceId?: string
  spanId?: string
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
  bounds: readonly number[]
  buckets: number[]
}

/** The error's TYPE — a constructor name, never its message. */
function errorType(error: unknown): string {
  if (error instanceof Error) return error.name || error.constructor.name || "Error"
  return typeof error
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** A span that records nothing — after `close()`, or under an unsampled trace. It still has ids to propagate. */
function inertSpan(trace: string): OtlpSpan {
  return {
    traceId: trace,
    spanId: spanId(),
    sampled: false,
    end: () => {},
    fail: () => {},
  }
}

/** The ratio decision, deterministic in the trace id: the top 32 bits as a fraction of 2^32. */
function ratioSampled(trace: string, ratio: number): boolean {
  if (ratio >= 1) return true
  if (ratio <= 0) return false
  return Number.parseInt(trace.slice(0, 8), 16) / 0x1_0000_0000 < ratio
}

async function gzip(text: string): Promise<ArrayBuffer> {
  const stream = new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"))
  return new Response(stream).arrayBuffer()
}

/**
 * A batching OTLP/JSON exporter over `fetch`.
 *
 * ```ts
 * const exporter = otlpExporter({ endpoint: "http://localhost:4318", serviceName: "billing" })
 * const commandBus = otlpCommandBus(interceptingCommandBus(localCommandBus(uow), correlation), exporter)
 * // …
 * await app.stop()
 * await exporter.close()
 * ```
 *
 * Export failures are SWALLOWED (and reported to `onExportError` when given).
 * A collector that is down, slow or wrong must not turn into a failed
 * command: the batch is dropped and the process keeps serving. Buffers are
 * BOUNDED: past `maxBufferedSpans`/`maxBufferedLogs` the oldest records are
 * dropped and counted into the `kronos.otlp.dropped` sum, so a collector
 * outage costs memory up to a ceiling, not without limit. Every POST has a
 * deadline, so `close()` returns within it even against a hanging collector.
 */
export function otlpExporter(options: OtlpExporterOptions): OtlpExporter {
  const { endpoint, serviceName } = options
  const flushIntervalMs = options.flushIntervalMs ?? 5000
  const exportTimeoutMs = options.exportTimeoutMs ?? 10_000
  const maxBufferedSpans = options.maxBufferedSpans ?? 10_000
  const maxBufferedLogs = options.maxBufferedLogs ?? 10_000
  const recordMessages = options.errors === "message"
  const metricsEnabled = options.metrics !== false
  const maxSeriesPerMetric = options.maxSeriesPerMetric ?? 1000
  const base = endpoint.endsWith("/") ? endpoint.slice(0, -1) : endpoint

  let spans: SpanRecord[] = []
  let logs: LogRecordWire[] = []
  let sums = new Map<string, SumSeries>()
  let histograms = new Map<string, HistogramSeries>()
  const dropped = { traces: 0, logs: 0, metrics: 0 }
  /** Distinct series seen per metric name this window — the cardinality guard. */
  let seriesPerMetric = new Map<string, number>()
  const reportedUnbounded = new Set<string>()

  /** May a NEW series be opened for `name`? Counts it if so; drops and reports if not. */
  function admitSeries(name: string): boolean {
    if (name === DROPPED) return true
    const seen = seriesPerMetric.get(name) ?? 0
    if (seen >= maxSeriesPerMetric) {
      dropped.metrics += 1
      if (!reportedUnbounded.has(name)) {
        reportedUnbounded.add(name)
        options.onExportError?.(
          new Error(
            `metric "${name}" exceeded ${maxSeriesPerMetric} distinct attribute combinations in one flush window; ` +
              `further combinations are dropped. An attribute is probably unbounded (an id?) — metrics take ` +
              `outcomes and reasons, logs and spans take ids.`,
          ),
          "metrics",
        )
      }
      return false
    }
    seriesPerMetric.set(name, seen + 1)
    return true
  }
  let windowStartMs = Date.now()
  let closed = false
  /** Serializes flushes so two POSTs of the same batch can never overlap. */
  let pending: Promise<void> = Promise.resolve()

  const resource = { attributes: keyValues({ ...options.resource, "service.name": serviceName }) }
  const scope = { name: "@kronos-ts/otlp" }

  async function post(path: string, body: unknown, signal: "traces" | "metrics" | "logs"): Promise<void> {
    try {
      const json = JSON.stringify(body)
      const compressed = options.compression === "gzip"
      const response = await globalThis.fetch(options.endpoints?.[signal] ?? `${base}${path}`, {
        method: "POST",
        headers: {
          ...options.headers,
          "content-type": "application/json",
          ...(compressed ? { "content-encoding": "gzip" } : {}),
        },
        body: compressed ? await gzip(json) : json,
        signal: AbortSignal.timeout(exportTimeoutMs),
      })
      if (!response.ok) {
        options.onExportError?.(new Error(`OTLP ${signal} export rejected: HTTP ${response.status}`), signal)
      }
    } catch (error) {
      // Dropped on purpose — see the note on otlpExporter.
      options.onExportError?.(error, signal)
    }
  }

  /** Count what was dropped since the last flush into the metrics batch, then forget it. */
  function accountDropped(): void {
    for (const signal of ["traces", "logs", "metrics"] as const) {
      if (dropped[signal] === 0) continue
      addCount({
        name: DROPPED,
        value: dropped[signal],
        unit: "1",
        description: "Records dropped because the exporter's buffer was full",
        attributes: { signal },
      })
      dropped[signal] = 0
    }
  }

  function drainSpans(): unknown | undefined {
    if (spans.length === 0) return undefined
    const batch = spans
    spans = []
    return { resourceSpans: [{ resource, scopeSpans: [{ scope, spans: batch }] }] }
  }

  function drainLogs(): unknown | undefined {
    if (logs.length === 0) return undefined
    const batch = logs
    logs = []
    return { resourceLogs: [{ resource, scopeLogs: [{ scope, logRecords: batch }] }] }
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
              explicitBounds: series.bounds,
            },
          ],
          aggregationTemporality: 1,
        },
      })
    }
    sums = new Map()
    histograms = new Map()
    seriesPerMetric = new Map()
    return { resourceMetrics: [{ resource, scopeMetrics: [{ scope, metrics }] }] }
  }

  async function doFlush(): Promise<void> {
    accountDropped()
    const traces = drainSpans()
    const logBatch = drainLogs()
    const metrics = drainMetrics()
    if (traces) await post("/v1/traces", traces, "traces")
    if (logBatch) await post("/v1/logs", logBatch, "logs")
    if (metrics) await post("/v1/metrics", metrics, "metrics")
  }

  function flush(): Promise<void> {
    pending = pending.then(doFlush)
    return pending
  }

  function addCount(measurement: Measurement): void {
    if (!metricsEnabled) return
    const unit = measurement.unit ?? "1"
    const key = seriesKey(measurement.name, unit, measurement.attributes)
    const existing = sums.get(key)
    if (existing) {
      existing.value += measurement.value
      return
    }
    if (!admitSeries(measurement.name)) return
    sums.set(key, {
      name: measurement.name,
      unit,
      ...(measurement.description ? { description: measurement.description } : {}),
      attributes: measurement.attributes,
      value: measurement.value,
    })
  }

  const timer = setInterval(() => {
    void flush()
  }, flushIntervalMs)
  // A telemetry timer must never be the reason a process refuses to exit.
  ;(timer as { unref?: () => void }).unref?.()

  return {
    startSpan(spanOptions: StartSpanOptions): OtlpSpan {
      const trace = spanOptions.parent?.traceId ?? traceId()
      if (closed) return inertSpan(trace)
      // PARENT-BASED: a span under a parent follows the parent's decision; a
      // root decides once, and the decision rides the traceparent from there.
      const sampled = spanOptions.parent
        ? spanOptions.parent.sampled !== false
        : typeof options.sample === "function"
          ? options.sample(trace)
          : ratioSampled(trace, options.sample ?? 1)
      if (!sampled) return inertSpan(trace)
      const id = spanId()
      const startEpochMs = Date.now()
      const startPerf = performance.now()
      let ended = false

      const finish = (status: { code: number; message?: string }, extra?: Attributes) => {
        if (ended) return
        ended = true
        if (closed) return
        if (spans.length >= maxBufferedSpans) {
          spans.shift()
          dropped.traces += 1
        }
        spans.push({
          traceId: trace,
          spanId: id,
          ...(spanOptions.parent ? { parentSpanId: spanOptions.parent.spanId } : {}),
          name: spanOptions.name,
          kind: spanOptions.kind,
          startTimeUnixNano: unixNano(startEpochMs),
          endTimeUnixNano: unixNano(startEpochMs + (performance.now() - startPerf)),
          attributes: keyValues(extra ? { ...spanOptions.attributes, ...extra } : spanOptions.attributes),
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
        sampled: true,
        end: () => finish({ code: StatusCode.OK }),
        fail: (error: unknown) =>
          finish(
            { code: StatusCode.ERROR, message: recordMessages ? errorMessage(error) : errorType(error) },
            { "error.type": errorType(error) },
          ),
      }
    },

    addCount,

    recordHistogram(measurement: Measurement): void {
      if (!metricsEnabled) return
      const unit = measurement.unit ?? "1"
      const key = seriesKey(measurement.name, unit, measurement.attributes)
      let series = histograms.get(key)
      if (!series) {
        if (!admitSeries(measurement.name)) return
        series = {
          name: measurement.name,
          unit,
          ...(measurement.description ? { description: measurement.description } : {}),
          attributes: measurement.attributes,
          count: 0,
          sum: 0,
          min: Number.POSITIVE_INFINITY,
          max: Number.NEGATIVE_INFINITY,
          bounds: measurement.bounds ?? DEFAULT_BOUNDS,
          buckets: new Array<number>((measurement.bounds ?? DEFAULT_BOUNDS).length + 1).fill(0),
        }
        histograms.set(key, series)
      }
      series.count += 1
      series.sum += measurement.value
      series.min = Math.min(series.min, measurement.value)
      series.max = Math.max(series.max, measurement.value)
      let bucket = series.bounds.findIndex((bound) => measurement.value <= bound)
      if (bucket === -1) bucket = series.bounds.length
      series.buckets[bucket] = (series.buckets[bucket] ?? 0) + 1
    },

    emitLog(record: LogExport): void {
      if (closed) return
      if (logs.length >= maxBufferedLogs) {
        logs.shift()
        dropped.logs += 1
      }
      const severity = SEVERITY[record.level]
      logs.push({
        timeUnixNano: unixNano(record.time),
        severityNumber: severity.number,
        severityText: severity.text,
        body: { stringValue: record.message },
        attributes: keyValues(record.attributes),
        ...(record.trace ? { traceId: record.trace.traceId, spanId: record.trace.spanId } : {}),
      })
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
