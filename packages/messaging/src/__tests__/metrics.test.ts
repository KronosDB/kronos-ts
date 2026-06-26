import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata } from "@kronos-ts/common"
import {
  meteringHandlerEnhancerDefinition,
  noOpMetricsRecorder,
  type MetricsRecorder,
  type MetricAttributes,
} from "../metrics.js"
import type { Message } from "../message.js"

interface Measurement {
  instrument: string
  value: number
  attributes?: MetricAttributes
}

function recordingRecorder() {
  const measurements: Measurement[] = []
  const recorder: MetricsRecorder = {
    counter(name) {
      return { add: (value, attributes) => measurements.push({ instrument: name, value, attributes }) }
    },
    histogram(name) {
      return { record: (value, attributes) => measurements.push({ instrument: name, value, attributes }) }
    },
  }
  return { recorder, measurements }
}

const commandMessage = {
  kind: "command", identifier: "c-1", name: qn("t", "C"),
  payload: {}, metadata: emptyMetadata(), timestamp: Date.now(),
} as unknown as Message

const eventMessage = {
  kind: "event", identifier: "e-1", name: qn("t", "E"),
  payload: {}, metadata: emptyMetadata(), timestamp: Date.now() - 1000, tags: [],
} as unknown as Message

describe("meteringHandlerEnhancerDefinition", () => {
  it("records a success count and a duration for a command handler", async () => {
    const { recorder, measurements } = recordingRecorder()
    const handler = meteringHandlerEnhancerDefinition(recorder).wrapHandler(
      async (_m: Message) => "ok",
      { messageType: "command", messageName: "t.C", handlerGroup: "cmd" },
    )

    const result = await handler(commandMessage)
    expect(result).toBe("ok")

    const handled = measurements.find((m) => m.instrument === "kronos.messages.handled")
    expect(handled?.value).toBe(1)
    expect(handled?.attributes).toMatchObject({
      message_type: "command", message_name: "t.C", handler_group: "cmd", outcome: "success",
    })
    const duration = measurements.find((m) => m.instrument === "kronos.message.handler.duration")
    expect(duration).toBeDefined()
    expect(duration!.value).toBeGreaterThanOrEqual(0)
    // No event-lag for command handlers.
    expect(measurements.some((m) => m.instrument === "kronos.event.processing.lag")).toBe(false)
  })

  it("records a failure outcome and rethrows", async () => {
    const { recorder, measurements } = recordingRecorder()
    const handler = meteringHandlerEnhancerDefinition(recorder).wrapHandler(
      async (_m: Message) => { throw new Error("boom") },
      { messageType: "command", messageName: "t.C", handlerGroup: "cmd" },
    )

    await expect(handler(commandMessage)).rejects.toThrow("boom")
    const handled = measurements.find((m) => m.instrument === "kronos.messages.handled")
    expect(handled?.attributes?.outcome).toBe("failure")
    // Duration is still recorded on failure.
    expect(measurements.some((m) => m.instrument === "kronos.message.handler.duration")).toBe(true)
  })

  it("records processing lag for event handlers", async () => {
    const { recorder, measurements } = recordingRecorder()
    const handler = meteringHandlerEnhancerDefinition(recorder).wrapHandler(
      async (_m: Message) => {},
      { messageType: "event", messageName: "t.E", handlerGroup: "proc" },
    )

    await handler(eventMessage)
    const lag = measurements.find((m) => m.instrument === "kronos.event.processing.lag")
    expect(lag).toBeDefined()
    // Event timestamp was ~1s in the past.
    expect(lag!.value).toBeGreaterThanOrEqual(900)
    expect(lag!.attributes).toMatchObject({ message_name: "t.E", handler_group: "proc" })
  })

  it("respects a custom namespace", async () => {
    const { recorder, measurements } = recordingRecorder()
    const handler = meteringHandlerEnhancerDefinition(recorder, { namespace: "app" }).wrapHandler(
      async (_m: Message) => {},
      { messageType: "query", messageName: "t.Q", handlerGroup: "q" },
    )
    await handler(commandMessage)
    expect(measurements.some((m) => m.instrument === "app.messages.handled")).toBe(true)
  })

  it("noOpMetricsRecorder swallows measurements", () => {
    const recorder = noOpMetricsRecorder()
    expect(() => {
      recorder.counter("x").add(1, { a: "b" })
      recorder.histogram("y").record(5)
    }).not.toThrow()
  })
})
