import { afterEach, describe, expect, it } from "bun:test"
import type { CommandBus, CommandMessage, Metadata, QueryBus, QueryMessage } from "@kronos-ts/core"
import { emptyMetadata, qn } from "@kronos-ts/core"
import { otlpCommandBus, otlpQueryBus } from "../otlp-bus.js"
import { SpanKind, otlpExporter } from "../otlp-exporter.js"
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
    payload: { amount: 10 },
    metadata,
    timestamp: Date.now(),
  }
}

function queryMessage(metadata: Metadata = emptyMetadata()): QueryMessage {
  return {
    kind: "query",
    identifier: "qry-1",
    name: qn("billing", "GetInvoice"),
    payload: { id: "i-1" },
    metadata,
    timestamp: Date.now(),
  }
}

/** The delegate: records what it was handed, answers, or throws. */
function recordingCommandBus(behaviour: () => unknown = () => "ok") {
  const seen: CommandMessage[] = []
  const bus: CommandBus = {
    async dispatch(message) {
      seen.push(message)
      return behaviour()
    },
    subscribe() {},
  }
  return { bus, seen }
}

function recordingQueryBus(behaviour: () => unknown = () => "ok") {
  const seen: QueryMessage[] = []
  const bus = {
    async query(message: QueryMessage) {
      seen.push(message)
      return behaviour()
    },
    subscribe() {},
  } as unknown as QueryBus
  return { bus, seen }
}

const TRACEPARENT = /^00-([0-9a-f]{32})-([0-9a-f]{16})-01$/

describe("otlpCommandBus", () => {
  it("injects a W3C traceparent into the outgoing metadata", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const { bus, seen } = recordingCommandBus()

    const result = await otlpCommandBus(bus, exporter).dispatch(commandMessage())
    await exporter.close()

    expect(result).toBe("ok")
    const propagated = String(seen[0]!.metadata.traceparent)
    const match = TRACEPARENT.exec(propagated)
    expect(match).not.toBeNull()

    // The header names THIS dispatch span — same trace, same span id.
    const span = fetchStub.spans()[0]
    expect(span.name).toBe("dispatch(billing.ChargeCard)")
    expect(span.kind).toBe(SpanKind.PRODUCER)
    expect(match![1]).toBe(span.traceId)
    expect(match![2]).toBe(span.spanId)
    expect(attribute(span.attributes, "kronos.message.id")).toEqual({ stringValue: "cmd-1" })
    expect(attribute(span.attributes, "kronos.message.name")).toEqual({
      stringValue: "billing.ChargeCard",
    })
  })

  it("does not mutate the caller's message", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const { bus, seen } = recordingCommandBus()
    const original = commandMessage()

    await otlpCommandBus(bus, exporter).dispatch(original)
    await exporter.close()

    expect(original.metadata).toEqual({})
    expect(seen[0]!.metadata.traceparent).toBeDefined()
  })

  it("continues the trace the incoming message already carried", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const { bus, seen } = recordingCommandBus()
    const inbound = "0".repeat(31) + "1"
    const inboundSpan = "0".repeat(15) + "2"

    await otlpCommandBus(bus, exporter).dispatch(
      commandMessage({ traceparent: `00-${inbound}-${inboundSpan}-01` }),
    )
    await exporter.close()

    const span = fetchStub.spans()[0]
    expect(span.traceId).toBe(inbound)
    expect(span.parentSpanId).toBe(inboundSpan)
    // …and the message leaves carrying the NEW span, not the one it arrived with.
    expect(seen[0]!.metadata.traceparent).toBe(`00-${inbound}-${span.spanId}-01`)
  })

  it("records a failed dispatch on the span and rethrows", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const { bus } = recordingCommandBus(() => {
      throw new Error("no handler")
    })

    await expect(otlpCommandBus(bus, exporter).dispatch(commandMessage())).rejects.toThrow(
      "no handler",
    )
    await exporter.close()

    expect(fetchStub.spans()[0].status).toEqual({ code: 2, message: "no handler" })
  })

  it("passes subscribe through untouched — the handler span has one author", () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const subscribed: string[] = []
    const bus: CommandBus = {
      async dispatch() {
        return undefined
      },
      subscribe(name) {
        subscribed.push(name)
      },
    }
    const handler = async () => "x"

    otlpCommandBus(bus, exporter).subscribe("billing.ChargeCard", handler)
    expect(subscribed).toEqual(["billing.ChargeCard"])
  })
})

describe("otlpQueryBus", () => {
  it("injects a traceparent and opens a CLIENT span per query", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const { bus, seen } = recordingQueryBus()

    const result = await otlpQueryBus(bus, exporter).query(queryMessage())
    await exporter.close()

    expect(result).toBe("ok")
    const span = fetchStub.spans()[0]
    expect(span.name).toBe("query(billing.GetInvoice)")
    expect(span.kind).toBe(SpanKind.CLIENT)
    expect(seen[0]!.metadata.traceparent).toBe(`00-${span.traceId}-${span.spanId}-01`)
  })

  it("forwards the unit of work it was given", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const uows: unknown[] = []
    const bus = {
      async query(_message: QueryMessage, uow?: unknown) {
        uows.push(uow)
        return "ok"
      },
    } as unknown as QueryBus
    const marker = { marker: true } as any

    await otlpQueryBus(bus, exporter).query(queryMessage(), marker)
    await exporter.close()

    expect(uows).toEqual([marker])
  })

  it("records a failed query on the span and rethrows", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const { bus } = recordingQueryBus(() => {
      throw new Error("not found")
    })

    await expect(otlpQueryBus(bus, exporter).query(queryMessage())).rejects.toThrow("not found")
    await exporter.close()

    expect(fetchStub.spans()[0].status).toEqual({ code: 2, message: "not found" })
  })

  it("delegates the rest of the bus surface unchanged", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const calls: string[] = []
    const bus = {
      async query() {
        return undefined
      },
      subscribe() {
        calls.push("subscribe")
      },
      subscriptionQuery() {
        calls.push("subscriptionQuery")
        return { initialResult: Promise.resolve(undefined) }
      },
      subscribeToUpdates() {
        calls.push("subscribeToUpdates")
        return undefined
      },
      async emitUpdate() {
        calls.push("emitUpdate")
      },
      async completeSubscription() {
        calls.push("completeSubscription")
      },
      async completeSubscriptionExceptionally() {
        calls.push("completeSubscriptionExceptionally")
      },
    } as unknown as QueryBus

    const traced = otlpQueryBus(bus, exporter)
    traced.subscribe("q", async () => undefined)
    traced.subscriptionQuery(queryMessage())
    traced.subscribeToUpdates(queryMessage())
    await traced.emitUpdate("q", () => true, { a: 1 })
    await traced.completeSubscription("q")
    await traced.completeSubscriptionExceptionally("q", new Error("x"))

    expect(calls).toEqual([
      "subscribe",
      "subscriptionQuery",
      "subscribeToUpdates",
      "emitUpdate",
      "completeSubscription",
      "completeSubscriptionExceptionally",
    ])
  })
})
