import { describe, expect, it } from "bun:test"
import type { Meter } from "@opentelemetry/api"
import { createOpenTelemetryMetricsRecorder } from "../opentelemetry-metrics-recorder.js"

interface Recorded {
  kind: "counter" | "histogram"
  name: string
  value: number
  attributes?: unknown
}

function fakeMeter() {
  const recorded: Recorded[] = []
  const meter = {
    createCounter(name: string) {
      return { add: (value: number, attributes?: unknown) => recorded.push({ kind: "counter", name, value, attributes }) }
    },
    createHistogram(name: string) {
      return { record: (value: number, attributes?: unknown) => recorded.push({ kind: "histogram", name, value, attributes }) }
    },
  } as unknown as Meter
  return { meter, recorded }
}

describe("createOpenTelemetryMetricsRecorder", () => {
  it("delegates counter.add and histogram.record to the OTel meter instruments", () => {
    const { meter, recorded } = fakeMeter()
    const recorder = createOpenTelemetryMetricsRecorder({ meter })

    recorder.counter("kronos.messages.handled", { unit: "1" }).add(1, { outcome: "success" })
    recorder.histogram("kronos.message.handler.duration", { unit: "ms" }).record(12.5, { message_name: "t.C" })

    expect(recorded).toEqual([
      { kind: "counter", name: "kronos.messages.handled", value: 1, attributes: { outcome: "success" } },
      { kind: "histogram", name: "kronos.message.handler.duration", value: 12.5, attributes: { message_name: "t.C" } },
    ])
  })
})
