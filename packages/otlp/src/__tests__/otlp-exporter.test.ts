import { afterEach, describe, expect, it } from "bun:test"
import { SpanKind, otlpExporter, spanId, traceId } from "../otlp-exporter.js"
import { attribute, delay, stubFetch, type FetchStub } from "./stub-fetch.js"
import { formatTraceparent, traceparentOf } from "../traceparent.js"

let fetchStub: FetchStub | undefined

afterEach(() => {
  fetchStub?.restore()
  fetchStub = undefined
})

describe("otlpExporter — ids", () => {
  it("generates 32-hex-character trace ids and 16-hex-character span ids", () => {
    expect(traceId()).toMatch(/^[0-9a-f]{32}$/)
    expect(spanId()).toMatch(/^[0-9a-f]{16}$/)
    expect(traceId()).not.toBe(traceId())
    expect(spanId()).not.toBe(spanId())
  })

  it("gives a parentless span its own trace and a parented span its parent's", () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://collector:4318", serviceName: "svc" })

    const root = exporter.startSpan({ name: "root", kind: SpanKind.INTERNAL })
    const child = exporter.startSpan({ name: "child", kind: SpanKind.CONSUMER, parent: root })

    expect(child.traceId).toBe(root.traceId)
    expect(child.spanId).not.toBe(root.spanId)
    expect(root.traceId).toMatch(/^[0-9a-f]{32}$/)
  })
})

describe("otlpExporter — trace envelope", () => {
  it("POSTs OTLP/JSON resourceSpans to <endpoint>/v1/traces on close()", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://collector:4318",
      serviceName: "billing",
      flushIntervalMs: 60_000,
    })

    const parent = exporter.startSpan({ name: "dispatch(x.Y)", kind: SpanKind.PRODUCER })
    const span = exporter.startSpan({
      name: "billing.Y",
      kind: SpanKind.CONSUMER,
      parent,
      attributes: { "kronos.message.name": "Y", "kronos.retries": 2, "kronos.ok": true },
    })
    span.end()
    parent.end()

    expect(fetchStub.posts).toHaveLength(0) // nothing left the process yet — it batches
    await exporter.close()

    expect(fetchStub.posts).toHaveLength(1)
    const post = fetchStub.posts[0]!
    expect(post.url).toBe("http://collector:4318/v1/traces")

    const resourceSpans = post.body.resourceSpans
    expect(resourceSpans).toHaveLength(1)
    expect(attribute(resourceSpans[0].resource.attributes, "service.name")).toEqual({
      stringValue: "billing",
    })
    expect(resourceSpans[0].scopeSpans[0].scope).toEqual({ name: "@kronos-ts/otlp" })

    const spans = resourceSpans[0].scopeSpans[0].spans
    expect(spans).toHaveLength(2)
    const handled = spans[0]
    expect(handled.name).toBe("billing.Y")
    expect(handled.kind).toBe(SpanKind.CONSUMER)
    expect(handled.traceId).toBe(parent.traceId)
    expect(handled.parentSpanId).toBe(parent.spanId)
    expect(handled.links).toEqual([])
    expect(handled.status).toEqual({ code: 1 })
    expect(attribute(handled.attributes, "kronos.retries")).toEqual({ intValue: "2" })
    expect(attribute(handled.attributes, "kronos.ok")).toEqual({ boolValue: true })
  })

  it("encodes times as nanosecond strings, not numbers", async () => {
    fetchStub = stubFetch()
    const before = Date.now()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).end()
    await exporter.close()

    const span = fetchStub.spans()[0]
    expect(typeof span.startTimeUnixNano).toBe("string")
    expect(typeof span.endTimeUnixNano).toBe("string")
    expect(span.startTimeUnixNano).toMatch(/^\d+$/)
    // The obvious `Date.now() * 1e6` loses precision past 2^53; assert the
    // value is a real epoch nanosecond, exact to the microsecond.
    const startNanos = BigInt(span.startTimeUnixNano)
    expect(startNanos).toBeGreaterThanOrEqual(BigInt(before) * 1_000_000n)
    expect(startNanos).toBeLessThan(BigInt(Date.now() + 1000) * 1_000_000n)
    expect(BigInt(span.endTimeUnixNano)).toBeGreaterThanOrEqual(startNanos)
  })

  it("records a failed span with ERROR status and the error's TYPE — never its message", async () => {
    // An error message can carry anything a user typed; the type cannot.
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    class Declined extends Error {
      override name = "Declined"
    }
    exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).fail(new Declined("card 4242 declined"))
    await exporter.close()

    const span = fetchStub.spans()[0]
    expect(span.status).toEqual({ code: 2, message: "Declined" })
    expect(attribute(span.attributes, "error.type")).toEqual({ stringValue: "Declined" })
    expect(JSON.stringify(span)).not.toContain("4242")
  })

  it("records the error message only when asked to", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc", errors: "message" })
    exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).fail(new Error("boom"))
    await exporter.close()

    expect(fetchStub.spans()[0].status).toEqual({ code: 2, message: "boom" })
  })

  it("emits a linked span as its own trace root with a links entry", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const producing = { traceId: "a".repeat(32), spanId: "b".repeat(16) }
    exporter.startSpan({ name: "proj.E", kind: SpanKind.CONSUMER, links: [producing] }).end()
    await exporter.close()

    const span = fetchStub.spans()[0]
    expect(span.parentSpanId).toBeUndefined()
    expect(span.traceId).not.toBe(producing.traceId)
    expect(span.links).toEqual([producing])
  })
})

describe("otlpExporter — batching and the flush loop", () => {
  it("batches many spans into ONE post", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      flushIntervalMs: 60_000,
    })
    for (let i = 0; i < 25; i++) {
      exporter.startSpan({ name: `s${i}`, kind: SpanKind.INTERNAL }).end()
    }
    await exporter.close()

    expect(fetchStub.traces()).toHaveLength(1)
    expect(fetchStub.spans()).toHaveLength(25)
  })

  it("flushes on the interval without anybody calling flush()", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      flushIntervalMs: 5,
    })
    exporter.startSpan({ name: "tick", kind: SpanKind.INTERNAL }).end()

    await delay(40)
    expect(fetchStub.spans().map((s: any) => s.name)).toEqual(["tick"])

    // A second window carries only what happened in it.
    exporter.startSpan({ name: "tock", kind: SpanKind.INTERNAL }).end()
    await delay(40)
    expect(fetchStub.spans().map((s: any) => s.name)).toEqual(["tick", "tock"])
    await exporter.close()
  })

  it("posts nothing when the batch is empty", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      flushIntervalMs: 5,
    })
    await delay(30)
    await exporter.close()
    expect(fetchStub.posts).toHaveLength(0)
  })

  it("close() flushes what is buffered, then stops the loop", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      flushIntervalMs: 5,
    })
    exporter.startSpan({ name: "last", kind: SpanKind.INTERNAL }).end()
    await exporter.close()
    expect(fetchStub.spans().map((s: any) => s.name)).toEqual(["last"])

    const postsAfterClose = fetchStub.posts.length
    exporter.startSpan({ name: "after", kind: SpanKind.INTERNAL }).end()
    await delay(40)
    expect(fetchStub.posts).toHaveLength(postsAfterClose) // the timer is gone
  })

  it("swallows a collector failure — telemetry never breaks the host", async () => {
    fetchStub = stubFetch({ fail: true })
    const exporter = otlpExporter({ endpoint: "http://down:4318", serviceName: "svc" })
    exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).end()
    await exporter.close() // must resolve, not reject
    expect(fetchStub.posts).toHaveLength(1)
  })

  it("trims a trailing slash off the endpoint", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318/", serviceName: "svc" })
    exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).end()
    await exporter.close()
    expect(fetchStub.posts[0]!.url).toBe("http://c:4318/v1/traces")
  })
})

describe("otlpExporter — metric envelope", () => {
  it("POSTs resourceMetrics with a monotonic delta sum", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "billing",
      flushIntervalMs: 60_000,
    })
    exporter.addCount({ name: "kronos.messages.handled", value: 1, attributes: { a: "x" } })
    exporter.addCount({ name: "kronos.messages.handled", value: 1, attributes: { a: "x" } })
    exporter.addCount({ name: "kronos.messages.handled", value: 1, attributes: { a: "y" } })
    await exporter.close()

    const post = fetchStub.metrics()
    expect(post).toHaveLength(1)
    const rm = post[0].resourceMetrics[0]
    expect(attribute(rm.resource.attributes, "service.name")).toEqual({ stringValue: "billing" })
    expect(rm.scopeMetrics[0].scope).toEqual({ name: "@kronos-ts/otlp" })

    // One SERIES per distinct attribute set, aggregated inside the window.
    const metrics = rm.scopeMetrics[0].metrics
    expect(metrics).toHaveLength(2)
    const forX = metrics.find(
      (m: any) => attribute(m.sum.dataPoints[0].attributes, "a").stringValue === "x",
    )
    expect(forX.name).toBe("kronos.messages.handled")
    expect(forX.unit).toBe("1")
    expect(forX.sum.isMonotonic).toBe(true)
    expect(forX.sum.aggregationTemporality).toBe(1)
    expect(forX.sum.dataPoints[0].asInt).toBe("2")
    expect(typeof forX.sum.dataPoints[0].timeUnixNano).toBe("string")
  })

  it("POSTs a histogram with explicit bounds and matching bucket counts", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      flushIntervalMs: 60_000,
    })
    for (const value of [1, 4, 30]) {
      exporter.recordHistogram({
        name: "kronos.message.handler.duration",
        value,
        unit: "ms",
        attributes: { handler_group: "billing" },
      })
    }
    await exporter.close()

    const metric = fetchStub.allMetrics()[0]
    expect(metric.name).toBe("kronos.message.handler.duration")
    expect(metric.unit).toBe("ms")
    const point = metric.histogram.dataPoints[0]
    expect(point.count).toBe("3")
    expect(point.sum).toBe(35)
    expect(point.min).toBe(1)
    expect(point.max).toBe(30)
    expect(point.bucketCounts).toHaveLength(point.explicitBounds.length + 1)
    expect(point.bucketCounts.every((count: unknown) => typeof count === "string")).toBe(true)
    expect(point.bucketCounts.reduce((a: number, b: string) => a + Number(b), 0)).toBe(3)
    // bounds [0, 5, 10, 25, 50, …]: 1 and 4 fall in the "<=5" bucket, 30 in "<=50"
    expect(point.bucketCounts[1]).toBe("2")
    expect(point.bucketCounts[4]).toBe("1")
  })

  it("resets each series after a flush — a window reports only itself", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      flushIntervalMs: 60_000,
    })
    exporter.addCount({ name: "c", value: 3 })
    await exporter.flush()
    exporter.addCount({ name: "c", value: 1 })
    await exporter.close()

    const points = fetchStub.allMetrics().map((m: any) => m.sum.dataPoints[0].asInt)
    expect(points).toEqual(["3", "1"])
  })

  it("sends traces and metrics to their separate endpoints in one flush", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      flushIntervalMs: 60_000,
    })
    exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).end()
    exporter.addCount({ name: "c", value: 1 })
    await exporter.close()

    expect(fetchStub.posts.map((p) => p.url)).toEqual([
      "http://c:4318/v1/traces",
      "http://c:4318/v1/metrics",
    ])
  })
})

describe("otlpExporter — bounds and shutdown", () => {
  it("drops the OLDEST spans past the buffer cap and counts them", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc", maxBufferedSpans: 2 })
    for (const name of ["a", "b", "c"]) exporter.startSpan({ name, kind: SpanKind.INTERNAL }).end()
    await exporter.close()

    expect(fetchStub.spans().map((s: any) => s.name)).toEqual(["b", "c"])
    const dropped = fetchStub.allMetrics().find((m: any) => m.name === "kronos.otlp.dropped")
    expect(dropped.sum.dataPoints[0].asInt).toBe("1")
    expect(attribute(dropped.sum.dataPoints[0].attributes, "signal")).toEqual({ stringValue: "traces" })
  })

  it("records nothing after close(), and close() is idempotent", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    await exporter.close()
    exporter.startSpan({ name: "late", kind: SpanKind.INTERNAL }).end()
    exporter.emitLog({ time: Date.now(), level: "info", message: "late" })
    await exporter.close()

    expect(fetchStub.posts).toHaveLength(0)
  })

  it("bounds a hanging collector by the export deadline", async () => {
    const original = globalThis.fetch
    globalThis.fetch = ((_url: unknown, init: RequestInit) =>
      new Promise((_resolve, reject) => {
        init.signal?.addEventListener("abort", () => reject(new Error("aborted")))
      })) as unknown as typeof fetch
    const seen: unknown[] = []
    try {
      const exporter = otlpExporter({
        endpoint: "http://c:4318",
        serviceName: "svc",
        exportTimeoutMs: 20,
        onExportError: (error) => seen.push(error),
      })
      exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).end()
      const started = performance.now()
      await exporter.close()
      expect(performance.now() - started).toBeLessThan(1000)
      expect(seen).toHaveLength(1)
    } finally {
      globalThis.fetch = original
    }
  })

  it("reports a rejecting collector to onExportError and keeps serving", async () => {
    fetchStub = stubFetch({ fail: true })
    const seen: unknown[] = []
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc", onExportError: (e) => seen.push(e) })
    exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).end()
    await exporter.close()
    expect(seen).toHaveLength(1)
  })
})

describe("otlpExporter — log envelope", () => {
  it("POSTs resourceLogs with severity, body, attributes and the span it was written under", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    exporter.emitLog({
      time: 1_700_000_000_000,
      level: "warn",
      message: "order placed",
      attributes: { orderId: "o-1" },
      trace: { traceId: "0af7651916cd43dd8448eb211c80319c", spanId: "b7ad6b7169203331" },
    })
    await exporter.close()

    const post = fetchStub.posts.find((p: any) => p.url.endsWith("/v1/logs"))
    const record = post.body.resourceLogs[0].scopeLogs[0].logRecords[0]
    expect(record).toMatchObject({
      timeUnixNano: "1700000000000000000",
      severityNumber: 13,
      severityText: "WARN",
      body: { stringValue: "order placed" },
      traceId: "0af7651916cd43dd8448eb211c80319c",
      spanId: "b7ad6b7169203331",
    })
    expect(attribute(record.attributes, "orderId")).toEqual({ stringValue: "o-1" })
  })
})

describe("otlpExporter — headers, resource and the cardinality guard", () => {
  it("sends the configured headers on every POST and the resource attributes beside service.name", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      headers: { authorization: "Bearer k" },
      resource: { "service.version": "1.2.3", "deployment.environment.name": "prod" },
    })
    exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).end()
    await exporter.close()

    const post = fetchStub.posts[0]!
    expect(post.headers).toMatchObject({ authorization: "Bearer k", "content-type": "application/json" })
    const attrs = post.body.resourceSpans[0].resource.attributes
    expect(attribute(attrs, "service.name")).toEqual({ stringValue: "svc" })
    expect(attribute(attrs, "service.version")).toEqual({ stringValue: "1.2.3" })
    expect(attribute(attrs, "deployment.environment.name")).toEqual({ stringValue: "prod" })
  })

  it("drops attribute combinations past the cap, counts them, and says which metric — once", async () => {
    fetchStub = stubFetch()
    const reported: string[] = []
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      maxSeriesPerMetric: 2,
      onExportError: (error) => reported.push((error as Error).message),
    })
    // An id in an attribute: every call is a new series.
    for (const orderId of ["a", "b", "c", "d"]) exporter.addCount({ name: "orders.placed", value: 1, attributes: { orderId } })
    // An existing combination still aggregates past the cap.
    exporter.addCount({ name: "orders.placed", value: 1, attributes: { orderId: "a" } })
    await exporter.close()

    const placed = fetchStub.allMetrics().filter((m: any) => m.name === "orders.placed")
    expect(placed).toHaveLength(2)
    const dropped = fetchStub.allMetrics().find((m: any) => m.name === "kronos.otlp.dropped")
    expect(dropped.sum.dataPoints[0].asInt).toBe("2")
    expect(attribute(dropped.sum.dataPoints[0].attributes, "signal")).toEqual({ stringValue: "metrics" })
    expect(reported).toHaveLength(1)
    expect(reported[0]).toContain('metric "orders.placed" exceeded 2 distinct attribute combinations')
  })
})

describe("otlpExporter — sampling is decided at the root and followed beneath it", () => {
  it("records nothing for an unsampled trace, but still hands out ids and an UNSAMPLED traceparent", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc", sample: 0 })
    const root = exporter.startSpan({ name: "root", kind: SpanKind.INTERNAL })
    const child = exporter.startSpan({ name: "child", kind: SpanKind.INTERNAL, parent: root })
    child.end()
    root.end()
    // Metrics are never sampled — counters stay exact.
    exporter.addCount({ name: "kept", value: 1 })
    await exporter.close()

    expect(root.sampled).toBe(false)
    expect(child.traceId).toBe(root.traceId)
    expect(formatTraceparent(root)).toBe(`00-${root.traceId}-${root.spanId}-00`)
    expect(fetchStub.spans()).toHaveLength(0)
    expect(fetchStub.allMetrics().map((m: any) => m.name)).toEqual(["kept"])
  })

  it("follows an incoming unsampled traceparent, and a sampled one overrides a zero ratio", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc", sample: 0 })
    const unsampled = traceparentOf({ traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00" })
    const sampled = traceparentOf({ traceparent: "00-1af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" })

    exporter.startSpan({ name: "under-unsampled", kind: SpanKind.INTERNAL, parent: unsampled }).end()
    exporter.startSpan({ name: "under-sampled", kind: SpanKind.INTERNAL, parent: sampled }).end()
    await exporter.close()

    expect(fetchStub.spans().map((s: any) => s.name)).toEqual(["under-sampled"])
  })

  it("decides a ratio deterministically from the trace id, so every process agrees", async () => {
    fetchStub = stubFetch()
    const decided: string[] = []
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      sample: (traceId) => {
        decided.push(traceId)
        return traceId.startsWith("0")
      },
    })
    for (let i = 0; i < 64; i++) exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).end()
    await exporter.close()

    expect(decided).toHaveLength(64)
    expect(fetchStub.spans().every((s: any) => s.traceId.startsWith("0"))).toBe(true)
    expect(fetchStub.spans().length).toBe(decided.filter((t) => t.startsWith("0")).length)
  })
})

describe("otlpExporter — endpoints, compression and histogram bounds", () => {
  it("sends a signal to its own URL when one is given, and the rest to the base", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({
      endpoint: "http://c:4318",
      serviceName: "svc",
      endpoints: { traces: "http://tempo:4318/otlp/v1/traces" },
    })
    exporter.startSpan({ name: "s", kind: SpanKind.INTERNAL }).end()
    exporter.addCount({ name: "m", value: 1 })
    await exporter.close()

    expect(fetchStub.posts.map((p) => p.url).sort()).toEqual([
      "http://c:4318/v1/metrics",
      "http://tempo:4318/otlp/v1/traces",
    ])
  })

  it("gzips the body and says so", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc", compression: "gzip" })
    exporter.startSpan({ name: "zipped", kind: SpanKind.INTERNAL }).end()
    await exporter.close()

    expect(fetchStub.posts[0]!.headers["content-encoding"]).toBe("gzip")
    expect(fetchStub.spans()[0].name).toBe("zipped")
  })

  it("buckets a histogram by its own bounds when given", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    for (const value of [50, 500, 5000]) exporter.recordHistogram({ name: "order.value", value, bounds: [100, 1000] })
    await exporter.close()

    const dp = fetchStub.allMetrics().find((m: any) => m.name === "order.value").histogram.dataPoints[0]
    expect(dp.explicitBounds).toEqual([100, 1000])
    expect(dp.bucketCounts).toEqual(["1", "1", "1"])
  })
})
