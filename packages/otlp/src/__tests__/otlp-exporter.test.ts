import { afterEach, describe, expect, it } from "bun:test"
import { SpanKind, otlpExporter, spanId, traceId } from "../otlp-exporter.js"
import { attribute, delay, stubFetch, type FetchStub } from "./stub-fetch.js"

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

  it("records a failed span with ERROR status and the error message", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
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
