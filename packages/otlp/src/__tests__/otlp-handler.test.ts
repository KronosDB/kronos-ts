import { afterEach, describe, expect, it } from "bun:test"
import type {
  CommandBus,
  CommandHandlerDefinition,
  CommandMessage,
  Metadata,
  SequencedEventMessage,
  QueryMessage,
} from "@kronos-ts/core"
import { emptyMetadata, qn } from "@kronos-ts/core"
import { otlpCommandBus } from "../otlp-bus.js"
import { SpanKind, otlpExporter } from "../otlp-exporter.js"
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

function eventMessage(metadata: Metadata): SequencedEventMessage {
  return {
    kind: "event",
    identifier: "evt-1",
    name: qn("billing", "CardCharged"),
    version: "1",
    payload: {},
    metadata,
    timestamp: Date.now(),
    tags: [{ key: "id", value: "c-1" }],
  }
}

function queryMessage(metadata: Metadata = emptyMetadata()): QueryMessage {
  return {
    kind: "query",
    identifier: "qry-1",
    name: qn("billing", "GetInvoice"),
    payload: {},
    metadata,
    timestamp: Date.now(),
  }
}

describe("otlpHandler — command and query messages PARENT onto the caller", () => {
  it("re-parents a command handler span onto the dispatch span, end to end", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    const handled = otlpHandler(async (_m: CommandMessage) => "done", exporter)
    const delegate: CommandBus = {
      dispatch: (message) => handled(message as CommandMessage, undefined),
      subscribe: () => {},
    }

    const result = await otlpCommandBus(delegate, exporter).dispatch(commandMessage())
    await exporter.close()

    expect(result).toBe("done")
    const spans = fetchStub.spans()
    const dispatch = spans.find((s: any) => s.name.startsWith("dispatch("))
    const handle = spans.find((s: any) => s.name === "billing.ChargeCard")
    expect(dispatch).toBeDefined()
    expect(handle).toBeDefined()
    // Same trace, real parentage — the dispatcher is still on the stack.
    expect(handle.traceId).toBe(dispatch.traceId)
    expect(handle.parentSpanId).toBe(dispatch.spanId)
    expect(handle.links).toEqual([])
    expect(handle.kind).toBe(SpanKind.CONSUMER)
    // The message is the only source of naming — there is no entry to ask.
    expect(attribute(handle.attributes, "kronos.message.name")).toEqual({
      stringValue: "billing.ChargeCard",
    })
    expect(attribute(handle.attributes, "kronos.message.kind")).toEqual({ stringValue: "command" })
  })

  it("parents a query handler span, with SERVER kind — read off message.kind", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const remote = { traceId: "1".repeat(32), spanId: "2".repeat(16) }

    await otlpHandler(async (_m: QueryMessage) => "v", exporter)(
      queryMessage({ traceparent: `00-${remote.traceId}-${remote.spanId}-01` }),
      undefined,
    )
    await exporter.close()

    const span = fetchStub.spans()[0]
    expect(span.name).toBe("billing.GetInvoice")
    expect(span.kind).toBe(SpanKind.SERVER)
    expect(span.traceId).toBe(remote.traceId)
    expect(span.parentSpanId).toBe(remote.spanId)
    expect(span.links).toEqual([])
  })

  it("starts a fresh root trace when the message carries no traceparent", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    await otlpHandler(async (_m: CommandMessage) => "x", exporter)(commandMessage(), undefined)
    await exporter.close()

    const span = fetchStub.spans()[0]
    expect(span.name).toBe("billing.ChargeCard")
    expect(span.parentSpanId).toBeUndefined()
    expect(span.traceId).toMatch(/^[0-9a-f]{32}$/)
  })

  it("treats a malformed traceparent as absent rather than corrupting the trace", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const traced = otlpHandler(async (_m: CommandMessage) => "x", exporter)

    await traced(commandMessage({ traceparent: "not-a-traceparent" }), undefined)
    // …and an all-zero trace id is invalid per W3C, so it is refused too.
    await traced(
      commandMessage({ traceparent: `00-${"0".repeat(32)}-${"3".repeat(16)}-01` }),
      undefined,
    )
    await exporter.close()

    for (const span of fetchStub.spans()) {
      expect(span.parentSpanId).toBeUndefined()
      expect(span.traceId).not.toBe("0".repeat(32))
    }
  })
})

describe("otlpHandler — an event message LINKS instead", () => {
  it("runs an event handler in its OWN trace, linked to the producing span", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    // The trace the event was produced in — long since finished by the time a
    // tracking processor gets around to the event.
    const producing = exporter.startSpan({
      name: "dispatch(billing.ChargeCard)",
      kind: SpanKind.PRODUCER,
    })
    producing.end()
    const carried = `00-${producing.traceId}-${producing.spanId}-01`

    await otlpHandler(async (_m: SequencedEventMessage) => {}, exporter)(
      eventMessage({ traceparent: carried }),
      undefined,
    )
    await exporter.close()

    const handle = fetchStub.spans().find((s: any) => s.name === "billing.CardCharged")
    expect(handle).toBeDefined()
    // NOT nested: a projection catching up over old events must not be
    // swallowed into the trace of whatever produced them.
    expect(handle.parentSpanId).toBeUndefined()
    expect(handle.traceId).not.toBe(producing.traceId)
    // …but correlated, by link.
    expect(handle.links).toEqual([{ traceId: producing.traceId, spanId: producing.spanId }])
    expect(handle.kind).toBe(SpanKind.CONSUMER)
  })

  it("gives each event of one producing trace its own trace, all linked back", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const producing = exporter.startSpan({ name: "dispatch", kind: SpanKind.PRODUCER })
    producing.end()
    const carried = `00-${producing.traceId}-${producing.spanId}-01`

    const handler = otlpHandler(async (_m: SequencedEventMessage) => {}, exporter)
    await handler(eventMessage({ traceparent: carried }), undefined)
    await handler(eventMessage({ traceparent: carried }), undefined)
    await exporter.close()

    const handles = fetchStub.spans().filter((s: any) => s.name === "billing.CardCharged")
    expect(handles).toHaveLength(2)
    expect(handles[0].traceId).not.toBe(handles[1].traceId)
    for (const handle of handles) {
      expect(handle.links).toEqual([{ traceId: producing.traceId, spanId: producing.spanId }])
    }
  })

  it("an event with no traceparent gets a root span and no links", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    await otlpHandler(async (_m: SequencedEventMessage) => {}, exporter)(
      eventMessage(emptyMetadata()),
      undefined,
    )
    await exporter.close()

    const span = fetchStub.spans()[0]
    expect(span.parentSpanId).toBeUndefined()
    expect(span.links).toEqual([])
  })
})

describe("otlpHandler — mechanics", () => {
  it("records a throwing handler as ERROR and rethrows", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const handler = otlpHandler(async (_m: CommandMessage) => {
      throw new Error("declined")
    }, exporter)

    await expect(handler(commandMessage(), undefined)).rejects.toThrow("declined")
    await exporter.close()

    expect(fetchStub.spans()[0].status).toEqual({ code: 2, message: "declined" })
  })

  it("passes the message and the ctx straight through", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const seen: unknown[] = []

    const handler = otlpHandler(async (message: CommandMessage, ctx: { unitOfWork: string }) => {
      seen.push(message, ctx)
      return "ok"
    }, exporter)

    const ctx = { unitOfWork: "uow" }
    const message = commandMessage()
    expect(await handler(message, ctx)).toBe("ok")
    expect(seen).toEqual([message, ctx])
    await exporter.close()
  })

  it("leaves the ENTRY to the host — the spread carries every other field", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    const entry: CommandHandlerDefinition = {
      kind: "command-handler",
      descriptor: {} as never,
      handler: async () => {},
      appendCondition: (_m, q) => q,
    }

    const wrapped = { ...entry, handler: otlpHandler(entry.handler, exporter) }
    expect(wrapped.kind).toBe(entry.kind)
    expect(wrapped.descriptor).toBe(entry.descriptor)
    expect(wrapped.appendCondition).toBe(entry.appendCondition)

    await wrapped.handler(commandMessage(), {} as never)
    await exporter.close()
    expect(fetchStub.spans()).toHaveLength(1)
  })

  it("names the span by a SELECTOR over the message when one is given", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    await otlpHandler(
      async (_m: CommandMessage) => "x",
      exporter,
      (m) => `handle(${m.name.name})`,
    )(commandMessage(), undefined)
    await exporter.close()

    // A function OF THE MESSAGE — never a per-entry string closed over at
    // wiring time, which is what made the wrapper entry-shaped before.
    expect(fetchStub.spans()[0].name).toBe("handle(ChargeCard)")
  })
})
