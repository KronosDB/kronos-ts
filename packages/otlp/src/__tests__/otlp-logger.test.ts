import { afterEach, describe, expect, it } from "bun:test"
import { otlpExporter } from "../otlp-exporter.js"
import { otlpLogger } from "../otlp-logger.js"
import { attribute, stubFetch, type FetchStub } from "./stub-fetch.js"

let fetchStub: FetchStub | undefined
afterEach(() => fetchStub?.restore())

const records = (stub: FetchStub) =>
  stub.posts
    .filter((p: any) => p.url.endsWith("/v1/logs"))
    .flatMap((p: any) => p.body.resourceLogs[0].scopeLogs[0].logRecords)

describe("otlpLogger — the core Logger contract over /v1/logs", () => {
  it("filters below the level, merges with() fields (later wins) and drops null fields", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const log = otlpLogger(exporter, { level: "info" }).with({ tenant: "acme", stale: "x" })

    log.debug("ignored")
    log.info("kept", { stale: null, orderId: "o-1" })
    await exporter.close()

    const [record] = records(fetchStub)
    expect(record.body).toEqual({ stringValue: "kept" })
    expect(attribute(record.attributes, "tenant")).toEqual({ stringValue: "acme" })
    expect(attribute(record.attributes, "orderId")).toEqual({ stringValue: "o-1" })
    expect(attribute(record.attributes, "stale")).toBeUndefined()
    expect(records(fetchStub)).toHaveLength(1)
  })

  it("turns trace_id/span_id fields into the record's trace and span ids", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    otlpLogger(exporter)
      .with({ trace_id: "0af7651916cd43dd8448eb211c80319c", span_id: "b7ad6b7169203331" })
      .error("failed")
    await exporter.close()

    const [record] = records(fetchStub)
    expect(record).toMatchObject({
      severityText: "ERROR",
      traceId: "0af7651916cd43dd8448eb211c80319c",
      spanId: "b7ad6b7169203331",
    })
  })
})
