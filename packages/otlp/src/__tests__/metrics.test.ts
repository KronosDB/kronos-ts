import { afterEach, describe, expect, it } from "bun:test"
import { otlpExporter } from "../otlp-exporter.js"
import { inertMetrics, metrics } from "../metrics.js"
import { attribute, stubFetch, type FetchStub } from "./stub-fetch.js"

let fetchStub: FetchStub | undefined
afterEach(() => fetchStub?.restore())

describe("metrics — a bound view of the exporter", () => {
  it("merges the bound attributes under the call's own, and count defaults to 1", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const m = metrics(exporter, { route: "POST /orders", outcome: "bound" })

    m.count("http.requests", { outcome: "accepted" })
    m.count("http.requests", { outcome: "accepted" }, { by: 2 })
    await exporter.close()

    const [requests] = fetchStub.allMetrics().filter((x: any) => x.name === "http.requests")
    const dp = requests.sum.dataPoints[0]
    expect(dp.asInt).toBe("3")
    expect(attribute(dp.attributes, "route")).toEqual({ stringValue: "POST /orders" })
    expect(attribute(dp.attributes, "outcome")).toEqual({ stringValue: "accepted" })
  })

  it("inertMetrics records nothing and never throws", () => {
    expect(() => {
      inertMetrics.count("x", { a: "b" })
      inertMetrics.record("y", 1)
    }).not.toThrow()
  })
})
