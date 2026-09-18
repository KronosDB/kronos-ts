import { afterEach, describe, expect, it } from "bun:test"
import { emptyMetadata, qn, type CommandMessage } from "@kronos-ts/core"
import { otlpExporter } from "../otlp-exporter.js"
import { otlpHandler } from "../otlp-handler.js"
import type { Metrics } from "../metrics.js"
import { attribute, stubFetch, type FetchStub } from "./stub-fetch.js"

let fetchStub: FetchStub | undefined
afterEach(() => fetchStub?.restore())

const command = (name = "ChargeCard"): CommandMessage => ({
  kind: "command",
  identifier: `cmd-${name}`,
  name: qn("billing", name),
  payload: {},
  metadata: emptyMetadata(),
  timestamp: 0,
})

const series = (stub: FetchStub, name: string) => stub.allMetrics().filter((m: any) => m.name === name)
const point = (metric: any) => (metric.sum ?? metric.histogram).dataPoints[0]

describe("otlpHandler — the three standard series come with the span", () => {
  it("emits throughput and duration for a successful invocation, keyed like the span", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    await otlpHandler(async (_m: CommandMessage) => "ok", exporter)(command(), {})
    await exporter.close()

    const [handled] = series(fetchStub, "kronos.messages.handled")
    expect(point(handled).asInt).toBe("1")
    expect(attribute(point(handled).attributes, "message_type")).toEqual({ stringValue: "command" })
    expect(attribute(point(handled).attributes, "message_name")).toEqual({ stringValue: "billing.ChargeCard" })
    expect(fetchStub.spans()[0].name).toBe("billing.ChargeCard")

    const [duration] = series(fetchStub, "kronos.message.handler.duration")
    expect(point(duration).count).toBe("1")
    expect(series(fetchStub, "kronos.messages.failed")).toHaveLength(0)
  })

  it("counts a failure on BOTH handled and failed, and failed carries the error's TYPE", async () => {
    // A typed domain rejection and a bug are different series; the message never leaves.
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    class CardDeclined extends Error {
      override name = "CardDeclined"
    }
    const handler = otlpHandler(async (_m: CommandMessage) => {
      throw new CardDeclined("card 4242 declined")
    }, exporter)

    await expect(handler(command(), {})).rejects.toThrow("declined")
    await exporter.close()

    expect(point(series(fetchStub, "kronos.messages.handled")[0]).asInt).toBe("1")
    const [failed] = series(fetchStub, "kronos.messages.failed")
    expect(point(failed).asInt).toBe("1")
    expect(attribute(point(failed).attributes, "error_type")).toEqual({ stringValue: "CardDeclined" })
    expect(series(fetchStub, "kronos.message.handler.duration")).toHaveLength(1)
    expect(JSON.stringify(fetchStub.allMetrics())).not.toContain("4242")
  })

  it("aggregates repeated invocations into one data point, and keeps separate series per message", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const handler = otlpHandler(async (_m: CommandMessage) => "ok", exporter)

    await handler(command("ChargeCard"), {})
    await handler(command("ChargeCard"), {})
    await handler(command("RefundCard"), {})
    await exporter.close()

    const counts = series(fetchStub, "kronos.messages.handled").map((m: any) => [
      attribute(point(m).attributes, "message_name").stringValue,
      point(m).asInt,
    ])
    expect(counts.sort()).toEqual([
      ["billing.ChargeCard", "2"],
      ["billing.RefundCard", "1"],
    ])
  })

  it("keys the span AND the series by the label when one is given", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    await otlpHandler(async (_m: CommandMessage) => "ok", exporter, () => "charge")(command(), {})
    await exporter.close()

    expect(fetchStub.spans()[0].name).toBe("charge")
    expect(attribute(point(series(fetchStub, "kronos.messages.handled")[0]).attributes, "message_name")).toEqual({
      stringValue: "charge",
    })
  })
})

describe("otlpHandler — ctx.metrics, as easy as a log line", () => {
  it("supplies metrics bound to the handler's message name and kind; the call's own attributes win", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const handler = otlpHandler(async (_m: CommandMessage, ctx: { metrics: Metrics }) => {
      ctx.metrics.count("orders.rejected", { reason: "out-of-stock" })
      ctx.metrics.record("payments.provider.latency", 12, { provider: "adyen" }, { unit: "ms" })
    }, exporter)

    await handler(command("PlaceOrder"), {})
    await exporter.close()

    const [rejected] = series(fetchStub, "orders.rejected")
    expect(point(rejected).asInt).toBe("1")
    expect(attribute(point(rejected).attributes, "reason")).toEqual({ stringValue: "out-of-stock" })
    expect(attribute(point(rejected).attributes, "message_name")).toEqual({ stringValue: "billing.PlaceOrder" })
    expect(attribute(point(rejected).attributes, "message_type")).toEqual({ stringValue: "command" })

    const [latency] = series(fetchStub, "payments.provider.latency")
    expect(latency.unit).toBe("ms")
    expect(point(latency).sum).toBe(12)
  })

  it("exports no metrics at all when the EXPORTER says so — spans are unaffected", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc", metrics: false })
    const handler = otlpHandler(async (_m: CommandMessage, ctx: { metrics: Metrics }) => {
      ctx.metrics.count("orders.rejected", { reason: "x" })
    }, exporter)

    await handler(command(), {})
    await exporter.close()

    expect(fetchStub.metrics()).toHaveLength(0)
    expect(fetchStub.spans()).toHaveLength(1)
  })
})

describe("otlpHandler — under an unsampled trace", () => {
  it("records no spans, stamps an UNSAMPLED traceparent for downstream, and still counts exactly", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc", sample: 0 })
    let stamped: unknown
    const handler = otlpHandler(async (m: CommandMessage, ctx: { trace: { span: <F extends (...a: any[]) => any>(fn: F) => F } }) => {
      stamped = m.metadata.traceparent
      return ctx.trace.span(() => "child work")()
    }, exporter)

    expect(await handler(command(), {})).toBe("child work")
    await exporter.close()

    expect(fetchStub.spans()).toHaveLength(0)
    expect(String(stamped)).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-00$/)
    expect(point(series(fetchStub, "kronos.messages.handled")[0]).asInt).toBe("1")
  })
})
