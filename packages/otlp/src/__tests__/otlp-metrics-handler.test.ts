import { afterEach, describe, expect, it } from "bun:test"
import type { CommandMessage, Metadata, QueryMessage } from "@kronos-ts/core"
import { emptyMetadata, qn } from "@kronos-ts/core"
import { otlpExporter } from "../otlp-exporter.js"
import { otlpMetricsHandler } from "../otlp-metrics-handler.js"
import { otlpHandler } from "../otlp-handler.js"
import { attribute, stubFetch, type FetchStub } from "./stub-fetch.js"

let fetchStub: FetchStub | undefined

afterEach(() => {
  fetchStub?.restore()
  fetchStub = undefined
})

function commandMessage(metadata: Metadata = emptyMetadata()): CommandMessage {
  return {
    kind: "command",
    identifier: "cmd-1",
    name: qn("billing", "ChargeCard"),
    payload: {},
    metadata,
    timestamp: Date.now(),
  }
}

function queryMessage(): QueryMessage {
  return {
    kind: "query",
    identifier: "qry-1",
    name: qn("billing", "GetInvoice"),
    payload: {},
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

const byName = (metrics: any[], name: string) => metrics.find((m) => m.name === name)

describe("otlpMetricsHandler", () => {
  it("emits throughput and duration for a successful invocation", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const handler = otlpMetricsHandler(async (_m: CommandMessage) => "ok", exporter)

    expect(await handler(commandMessage(), undefined)).toBe("ok")
    await exporter.close()

    const metrics = fetchStub.allMetrics()
    expect(metrics.map((m: any) => m.name).sort()).toEqual([
      "kronos.message.handler.duration",
      "kronos.messages.handled",
    ])

    const handled = byName(metrics, "kronos.messages.handled")
    expect(handled.unit).toBe("1")
    expect(handled.sum.isMonotonic).toBe(true)
    expect(handled.sum.dataPoints[0].asInt).toBe("1")
    const attributes = handled.sum.dataPoints[0].attributes
    // Both dimensions are read off the MESSAGE — nothing rides in from an entry.
    expect(attribute(attributes, "message_type")).toEqual({ stringValue: "command" })
    expect(attribute(attributes, "message_name")).toEqual({ stringValue: "billing.ChargeCard" })

    const duration = byName(metrics, "kronos.message.handler.duration")
    expect(duration.unit).toBe("ms")
    expect(duration.histogram.dataPoints[0].count).toBe("1")
    expect(duration.histogram.dataPoints[0].sum).toBeGreaterThanOrEqual(0)
    // The duration series slices by the same attributes as the counter.
    expect(attribute(duration.histogram.dataPoints[0].attributes, "message_name")).toEqual({
      stringValue: "billing.ChargeCard",
    })
  })

  it("counts a failure on BOTH handled and failed, and still records duration", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const handler = otlpMetricsHandler(async (_m: CommandMessage) => {
      throw new Error("declined")
    }, exporter)

    await expect(handler(commandMessage(), undefined)).rejects.toThrow("declined")
    await exporter.close()

    const metrics = fetchStub.allMetrics()
    expect(byName(metrics, "kronos.messages.handled").sum.dataPoints[0].asInt).toBe("1")
    expect(byName(metrics, "kronos.messages.failed").sum.dataPoints[0].asInt).toBe("1")
    expect(byName(metrics, "kronos.message.handler.duration").histogram.dataPoints[0].count).toBe(
      "1",
    )
  })

  it("aggregates repeated invocations into one data point per series", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const ok = otlpMetricsHandler(async (_m: CommandMessage) => "ok", exporter)
    const bad = otlpMetricsHandler(async (_m: CommandMessage) => {
      throw new Error("x")
    }, exporter)

    await ok(commandMessage(), undefined)
    await ok(commandMessage(), undefined)
    await ok(commandMessage(), undefined)
    await expect(bad(commandMessage(), undefined)).rejects.toThrow("x")
    await exporter.close()

    const metrics = fetchStub.allMetrics()
    expect(byName(metrics, "kronos.messages.handled").sum.dataPoints).toHaveLength(1)
    expect(byName(metrics, "kronos.messages.handled").sum.dataPoints[0].asInt).toBe("4")
    expect(byName(metrics, "kronos.messages.failed").sum.dataPoints[0].asInt).toBe("1")
    expect(byName(metrics, "kronos.message.handler.duration").histogram.dataPoints[0].count).toBe(
      "4",
    )
  })

  it("keeps separate series per message — the wrapper is one function, the series are many", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    // ONE pre-applied wrapper, two message vocabularies: the series split on
    // what arrived, not on how it was wired.
    await otlpMetricsHandler(async (_m: CommandMessage) => "ok", exporter)(commandMessage(), undefined)
    await otlpMetricsHandler(async (_m: QueryMessage) => "ok", exporter)(queryMessage(), undefined)
    await exporter.close()

    const handled = fetchStub.allMetrics().filter((m: any) => m.name === "kronos.messages.handled")
    expect(handled).toHaveLength(2)
    expect(
      handled
        .map((m: any) => attribute(m.sum.dataPoints[0].attributes, "message_name").stringValue)
        .sort(),
    ).toEqual(["billing.ChargeCard", "billing.GetInvoice"])
    expect(
      handled
        .map((m: any) => attribute(m.sum.dataPoints[0].attributes, "message_type").stringValue)
        .sort(),
    ).toEqual(["command", "query"])
  })

  it("keys the series by a SELECTOR over the message when one is given", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    await otlpMetricsHandler(
      async (_m: CommandMessage) => "ok",
      exporter,
      (m) => m.name.name,
    )(commandMessage(), undefined)
    await exporter.close()

    const handled = byName(fetchStub.allMetrics(), "kronos.messages.handled")
    expect(attribute(handled.sum.dataPoints[0].attributes, "message_name")).toEqual({
      stringValue: "ChargeCard",
    })
  })

  it("composes with otlpHandler — plain function composition, no entry in sight", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    const handler = otlpMetricsHandler(
      otlpHandler(async (_m: CommandMessage) => "ok", exporter),
      exporter,
    )
    expect(await handler(commandMessage(), undefined)).toBe("ok")
    await exporter.close()

    expect(fetchStub.spans()).toHaveLength(1)
    expect(
      fetchStub
        .allMetrics()
        .map((m: any) => m.name)
        .sort(),
    ).toEqual(["kronos.message.handler.duration", "kronos.messages.handled"])
  })
})
