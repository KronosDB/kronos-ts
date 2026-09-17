import { afterEach, describe, expect, it } from "bun:test"
import { consoleLogger } from "@kronos-ts/core"
import { otlpExporter, SpanKind } from "../otlp-exporter.js"
import { inertTrace, trace } from "../trace.js"
import { stubFetch, type FetchStub } from "./stub-fetch.js"

let fetchStub: FetchStub | undefined
afterEach(() => fetchStub?.restore())

describe("trace — a scope handed explicitly", () => {
  it("span(fn) keeps a sync function sync, rethrows synchronously, and never records arguments", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const root = exporter.startSpan({ name: "root", kind: SpanKind.INTERNAL })
    const scope = trace(exporter, root)

    const add = scope.span((a: number, b: number) => a + b, { name: "add" })
    expect(add(2, 3)).toBe(5)

    const boom = scope.span(() => {
      throw new Error("secret 4242")
    })
    expect(() => boom()).toThrow("secret 4242")
    root.end()
    await exporter.close()

    const spans = fetchStub.spans()
    // An inline arrow has no name, so it is "operation" — a stable label, never an argument.
    expect(spans.map((s: any) => s.name)).toEqual(["add", "operation", "root"])
    expect(spans[0].parentSpanId).toBe(root.spanId)
    expect(spans[1].status.code).toBe(2)
    expect(JSON.stringify(spans)).not.toContain("4242")
  })

  it("span(fn) ends when a promise settles, and names an anonymous function 'operation'", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const scope = trace(exporter)

    const later = scope.span(async () => "done")
    expect(await later()).toBe("done")
    const failing = scope.span(async () => {
      throw new Error("nope")
    })
    await expect(failing()).rejects.toThrow("nope")
    await exporter.close()

    const spans = fetchStub.spans()
    expect(spans.map((s: any) => s.name)).toEqual(["operation", "operation"])
    expect(spans[0].parentSpanId).toBeUndefined()
    expect(spans[1].status.code).toBe(2)
  })

  it("run(name, fn) hands the CHILD scope, whose traceparent names the child span", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const root = exporter.startSpan({ name: "root", kind: SpanKind.INTERNAL })
    const scope = trace(exporter, root)

    const header = await scope.run("payments.charge", async (child) => child.traceparent)
    root.end()
    await exporter.close()

    const child = fetchStub.spans()[0]
    expect(child.name).toBe("payments.charge")
    expect(child.parentSpanId).toBe(root.spanId)
    expect(header).toBe(`00-${child.traceId}-${child.spanId}-01`)
  })

  it("log(logger) binds the logger to the scope's ids", () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const lines: string[] = []
    const logger = consoleLogger({ write: (line) => lines.push(line) })
    const scope = trace(exporter, { traceId: "0af7651916cd43dd8448eb211c80319c", spanId: "b7ad6b7169203331" })

    scope.log(logger).info("hello")
    expect(JSON.parse(lines[0]!)).toMatchObject({
      trace_id: "0af7651916cd43dd8448eb211c80319c",
      span_id: "b7ad6b7169203331",
    })
  })
})

describe("inertTrace — the no-op scope", () => {
  it("returns the function, hands itself to run, leaves the logger alone", async () => {
    const fn = (x: number) => x * 2
    expect(inertTrace.span(fn)).toBe(fn)
    expect(await inertTrace.run("x", async (child) => child)).toBe(inertTrace)
    const logger = consoleLogger()
    expect(inertTrace.log(logger)).toBe(logger)
    expect(inertTrace.traceparent).toBeUndefined()
  })
})
