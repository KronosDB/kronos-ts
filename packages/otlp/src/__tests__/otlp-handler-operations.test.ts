import { afterEach, describe, expect, it } from "bun:test"
import type { CommandMessage, Metadata, QueryMessage, StandardSchemaV1, UnitOfWork } from "@kronos-ts/core"
import {
  command,
  commandHandler,
  correlatingHandler,
  correlation,
  event,
  inMemoryEventStore,
  interceptingCommandBus,
  interceptingQueryBus,
  kronos,
  localCommandBus,
  localQueryBus,
  qn,
  query,
  queryHandler,
  send,
  state,
  unitOfWork,
} from "@kronos-ts/core"
import { otlpCommandBus, otlpQueryBus } from "../otlp-bus.js"
import { otlpExporter, type OtlpExporter } from "../otlp-exporter.js"
import { otlpHandler, type TraceCapability } from "../otlp-handler.js"
import { stubFetch, type FetchStub } from "./stub-fetch.js"

let fetchStub: FetchStub | undefined

afterEach(() => {
  fetchStub?.restore()
  fetchStub = undefined
})

/** A hand-written Standard Schema that accepts anything — no schema library in this package. */
function shape<T>(): StandardSchemaV1<T, T> {
  return { "~standard": { version: 1, vendor: "hand", validate: (value: unknown) => ({ value: value as T }) } }
}

// ---------------------------------------------------------------------------
// A SENDS B, B QUERIES C — over core's real buses, contexts and in-memory log.
// Every assertion about tracing is made on what the collector received.
// ---------------------------------------------------------------------------

const OrderPlaced = event({
  name: qn("orders", "OrderPlaced"),
  payload: shape<{ orderId: string; sku: string }>(),
  tags: { orderId: (p) => p.orderId },
})

const StockReserved = event({
  name: qn("inventory", "StockReserved"),
  payload: shape<{ sku: string; quantity: number }>(),
  tags: { sku: (p) => p.sku },
})

const PlaceOrder = command({
  name: qn("orders", "PlaceOrder"),
  payload: shape<{ orderId: string; sku: string }>(),
  result: shape<{ reserved: number; placedBefore: boolean; sourced: number }>(),
})

const ReserveStock = command({
  name: qn("inventory", "ReserveStock"),
  payload: shape<{ sku: string }>(),
  result: shape<{ reserved: number }>(),
})

const GetAvailability = query({
  name: qn("inventory", "GetAvailability"),
  payload: shape<{ sku: string }>(),
  result: shape<{ available: number }>(),
})

const Order = state({
  id: { orderId: shape<string>() },
  tags: ({ orderId }) => ({ orderId }),
  evolve: [() => ({ placed: false }), [OrderPlaced, (s) => ({ ...s, placed: true })]],
})

type Seen = {
  readonly units: UnitOfWork[]
  readonly metadata: Record<string, Metadata>
}

function rig(options: { buses: boolean; correlate: boolean; availability?: () => Promise<{ available: number }> }) {
  const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
  const seen: Seen = { units: [], metadata: {} }

  const placeOrder = commandHandler(PlaceOrder, async (message, ctx) => {
    seen.units.push(ctx.unitOfWork)
    seen.metadata.PlaceOrder = message.metadata
    const before = await ctx.load(Order, { orderId: message.payload.orderId })
    const sourced = await ctx.source(Order.query({ orderId: message.payload.orderId }))
    ctx.append(OrderPlaced, message.payload)
    const { reserved } = await ctx.send(ReserveStock, { sku: message.payload.sku })
    return { reserved, placedBefore: before.placed, sourced: sourced.length }
  })

  const reserveStock = commandHandler(ReserveStock, async (message, ctx) => {
    seen.units.push(ctx.unitOfWork)
    seen.metadata.ReserveStock = message.metadata
    const { available } = await ctx.query(GetAvailability, { sku: message.payload.sku })
    ctx.append(StockReserved, { sku: message.payload.sku, quantity: available })
    return { reserved: available }
  })

  const getAvailability = queryHandler(GetAvailability, async (message, ctx) => {
    seen.units.push(ctx.unitOfWork)
    seen.metadata.GetAvailability = message.metadata
    expect("send" in ctx).toBe(false)
    expect("append" in ctx).toBe(false)
    return options.availability ? options.availability() : { available: 7 }
  })

  const wrap = <H extends (message: any, context: any) => any>(handler: H): H =>
    otlpHandler(options.correlate ? correlatingHandler(handler) : handler, exporter) as unknown as H

  const localCommands = localCommandBus(unitOfWork)
  const localQueries = localQueryBus(unitOfWork)
  const commandBus = interceptingCommandBus(
    options.buses ? otlpCommandBus(localCommands, exporter) : localCommands,
    correlation,
  )
  const queryBus = interceptingQueryBus(
    options.buses ? otlpQueryBus(localQueries, exporter) : localQueries,
    correlation,
  )

  const eventStore = inMemoryEventStore()
  const app = kronos({
    commandHandlers: [
      { ...placeOrder, handler: wrap(placeOrder.handler), eventStore, commandBus, queryBus },
      { ...reserveStock, handler: wrap(reserveStock.handler), eventStore, commandBus, queryBus },
    ],
    queryHandlers: [{ ...getAvailability, handler: wrap(getAvailability.handler), eventStore, queryBus }],
  })

  return { app, exporter, commandBus, eventStore, seen }
}

function named(spans: any[], name: string): any {
  const found = spans.filter((span) => span.name === name)
  expect(found).toHaveLength(1)
  return found[0]
}

async function placed(options: { buses: boolean; correlate: boolean }) {
  fetchStub = stubFetch()
  const { app, exporter, commandBus, eventStore, seen } = rig(options)
  const result = await send(commandBus, PlaceOrder, { orderId: "o-1", sku: "espresso" })
  await app.stop()
  await exporter.close()
  return { result, spans: fetchStub.spans(), eventStore, seen }
}

describe("otlpHandler — context operations are child spans", () => {
  for (const correlate of [false, true]) {
    it(`parents the receiving handler onto the operation, without bus wrappers (correlation: ${correlate})`, async () => {
      const { result, spans, seen } = await placed({ buses: false, correlate })

      // Business behavior is untouched: results flow back, and every handling
      // — the nested command and the query included — ran in its own task.
      expect(result).toEqual({ reserved: 7, placedBefore: false, sourced: 0 })
      expect(new Set(seen.units).size).toBe(3)

      const a = named(spans, "orders.PlaceOrder")
      const load = named(spans, "ctx.load")
      const source = named(spans, "ctx.source")
      const sendSpan = named(spans, "ctx.send")
      const b = named(spans, "inventory.ReserveStock")
      const querySpan = named(spans, "ctx.query")
      const c = named(spans, "inventory.GetAvailability")

      expect(a.parentSpanId).toBeUndefined()
      expect(load.parentSpanId).toBe(a.spanId)
      expect(source.parentSpanId).toBe(a.spanId)
      expect(sendSpan.parentSpanId).toBe(a.spanId)
      expect(b.parentSpanId).toBe(sendSpan.spanId)
      expect(querySpan.parentSpanId).toBe(b.spanId)
      expect(c.parentSpanId).toBe(querySpan.spanId)
      for (const span of [load, source, sendSpan, b, querySpan, c]) expect(span.traceId).toBe(a.traceId)
    })

    it(`puts the bus span between the operation and the handler, with bus wrappers (correlation: ${correlate})`, async () => {
      const { result, spans } = await placed({ buses: true, correlate })
      expect(result).toEqual({ reserved: 7, placedBefore: false, sourced: 0 })

      const edge = named(spans, "dispatch(orders.PlaceOrder)")
      const a = named(spans, "orders.PlaceOrder")
      const sendSpan = named(spans, "ctx.send")
      const dispatch = named(spans, "dispatch(inventory.ReserveStock)")
      const b = named(spans, "inventory.ReserveStock")
      const querySpan = named(spans, "ctx.query")
      const asked = named(spans, "query(inventory.GetAvailability)")
      const c = named(spans, "inventory.GetAvailability")

      expect(edge.parentSpanId).toBeUndefined()
      expect(a.parentSpanId).toBe(edge.spanId)
      expect(sendSpan.parentSpanId).toBe(a.spanId)
      expect(dispatch.parentSpanId).toBe(sendSpan.spanId)
      expect(b.parentSpanId).toBe(dispatch.spanId)
      expect(querySpan.parentSpanId).toBe(b.spanId)
      expect(asked.parentSpanId).toBe(querySpan.spanId)
      expect(c.parentSpanId).toBe(asked.spanId)
      for (const span of spans) expect(span.traceId).toBe(edge.traceId)
    })
  }

  it("adds trace identity only — business correlation is the correlation wrapper's", async () => {
    const { seen, spans, eventStore } = await placed({ buses: false, correlate: true })

    const a = seen.metadata.PlaceOrder!
    const b = seen.metadata.ReserveStock!
    const c = seen.metadata.GetAvailability!
    expect(b.correlationId).toBe(a.correlationId)
    expect(c.correlationId).toBe(a.correlationId)
    // A is a root, so it caused itself; B was caused by A, and C by B.
    expect(b.causationId).toBe(a.causationId)
    expect(c.causationId).not.toBe(b.causationId)

    // An appended event carries the span of the handling that produced it.
    const handler = named(spans, "inventory.ReserveStock")
    const { events } = await eventStore.source({ query: { tags: { sku: "espresso" } } })
    const reserved = events.find((e) => e.name.name === "StockReserved")!
    expect(reserved.metadata.traceparent).toBe(`00-${handler.traceId}-${handler.spanId}-01`)
    expect(reserved.metadata.correlationId).toBe(a.correlationId)
  })

  it("without correlation, a nested message carries the traceparent and nothing else", async () => {
    const { seen } = await placed({ buses: false, correlate: false })
    // `correlation` on the bus seeds the pair; the operation added one key.
    expect(Object.keys(seen.metadata.ReserveStock!).sort()).toEqual(["causationId", "correlationId", "traceparent"])
  })

  it("load and source keep working against the real log", async () => {
    fetchStub = stubFetch()
    const { app, exporter, commandBus } = rig({ buses: false, correlate: true })
    await send(commandBus, PlaceOrder, { orderId: "o-1", sku: "espresso" })
    const again = await send(commandBus, PlaceOrder, { orderId: "o-1", sku: "espresso" })
    await app.stop()
    await exporter.close()

    expect(again).toEqual({ reserved: 7, placedBefore: true, sourced: 1 })
  })

  it("records no argument or result of any operation", async () => {
    const { spans } = await placed({ buses: true, correlate: true })
    const exported = JSON.stringify(spans.filter((span: any) => span.name.startsWith("ctx.")))
    for (const secret of ["o-1", "espresso", "orderId", "sku", "reserved", "available"]) {
      expect(exported).not.toContain(secret)
    }
  })
})

describe("otlpHandler — context operations, span lifetime", () => {
  it("keeps an operation open while it awaits, and ends it when it settles", async () => {
    fetchStub = stubFetch()
    let release!: (value: { available: number }) => void
    const pending = new Promise<{ available: number }>((resolve) => (release = resolve))
    let asked!: () => void
    const reached = new Promise<void>((resolve) => (asked = resolve))

    const { app, exporter, commandBus } = rig({
      buses: false,
      correlate: true,
      availability: () => {
        asked()
        return pending
      },
    })

    const sending = send(commandBus, PlaceOrder, { orderId: "o-1", sku: "espresso" })
    await reached
    await exporter.flush()
    // load and source have settled; send and query are still awaiting.
    const early = fetchStub.spans().map((span: any) => span.name)
    expect(early).toContain("ctx.load")
    expect(early).toContain("ctx.source")
    expect(early).not.toContain("ctx.send")
    expect(early).not.toContain("ctx.query")

    release({ available: 3 })
    expect(await sending).toEqual({ reserved: 3, placedBefore: false, sourced: 0 })
    await app.stop()
    await exporter.close()

    // A span enters the batch when it ends: the inner operation first.
    const ended = fetchStub.spans().map((span: any) => span.name)
    expect(ended.indexOf("ctx.query")).toBeGreaterThan(-1)
    expect(ended.indexOf("ctx.send")).toBeGreaterThan(ended.indexOf("ctx.query"))
  })

  it("fails the operation with the child's error, unchanged — and a caught failure leaves the handler OK", async () => {
    fetchStub = stubFetch()
    class OutOfStock extends Error {
      override name = "OutOfStock"
    }
    const thrown = new OutOfStock("espresso is gone")
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })

    let caught: unknown
    const ctx = {
      send: async () => {
        throw thrown
      },
    }
    const handler = otlpHandler(async (_m: CommandMessage, c: typeof ctx) => {
      try {
        await c.send()
      } catch (error) {
        caught = error
      }
      return "recovered"
    }, exporter)

    expect(await handler(commandMessage(), ctx)).toBe("recovered")
    await exporter.close()

    expect(caught).toBe(thrown)
    // The error's TYPE, never its message.
    expect(named(fetchStub.spans(), "ctx.send").status).toEqual({ code: 2, message: "OutOfStock" })
    expect(named(fetchStub.spans(), "billing.ChargeCard").status?.code).not.toBe(2)
    expect(JSON.stringify(fetchStub.spans())).not.toContain("espresso is gone")
  })

  it("an uncaught operation failure fails the operation AND the handler", async () => {
    fetchStub = stubFetch()
    const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
    const ctx = {
      query: async () => {
        throw new TypeError("no")
      },
    }
    const handler = otlpHandler(async (_m: CommandMessage, c: typeof ctx) => c.query(), exporter)

    await expect(handler(commandMessage(), ctx)).rejects.toThrow("no")
    await exporter.close()

    expect(named(fetchStub.spans(), "ctx.query").status).toEqual({ code: 2, message: "TypeError" })
    expect(named(fetchStub.spans(), "billing.ChargeCard").status).toEqual({ code: 2, message: "TypeError" })
  })
})

// ---------------------------------------------------------------------------
// The wrapper itself, against a hand-held context — what it touches, what it
// leaves alone, and which parent an operation takes.
// ---------------------------------------------------------------------------

function commandMessage(metadata: Metadata = {}): CommandMessage {
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
  return { kind: "query", identifier: "qry-1", name: qn("billing", "GetInvoice"), payload: {}, metadata: {}, timestamp: Date.now() }
}

type Sent = { descriptor: unknown; payload: unknown; metadata: Metadata }

function recordingContext() {
  const sent: Sent[] = []
  const context = {
    marker: "kept",
    send: async (descriptor: unknown, payload: unknown, metadata?: Metadata) => {
      sent.push({ descriptor, payload, metadata: metadata ?? {} })
      return "sent"
    },
  }
  return { sent, context }
}

function exporterFor(): OtlpExporter {
  return otlpExporter({ endpoint: "http://c:4318", serviceName: "svc" })
}

describe("otlpHandler — context operations, mechanics", () => {
  it("wraps only the capabilities the context has, and mutates neither context nor metadata", async () => {
    fetchStub = stubFetch()
    const exporter = exporterFor()
    const { sent, context } = recordingContext()
    const original = context.send
    const given: Metadata = Object.freeze({ tenant: "t-1" })
    let supplied: Record<string, unknown> | undefined

    const handler = otlpHandler(async (_m: QueryMessage, c: typeof context) => {
      supplied = c as unknown as Record<string, unknown>
      return c.send("descriptor", "payload", given)
    }, exporter)

    expect(await handler(queryMessage(), context)).toBe("sent")
    await exporter.close()

    expect(context.send).toBe(original)
    expect(given).toEqual({ tenant: "t-1" })
    expect(supplied!.marker).toBe("kept")
    for (const absent of ["load", "source", "query", "append"]) expect(absent in supplied!).toBe(false)

    const span = named(fetchStub.spans(), "ctx.send")
    expect(sent).toEqual([
      {
        descriptor: "descriptor",
        payload: "payload",
        metadata: { tenant: "t-1", traceparent: `00-${span.traceId}-${span.spanId}-01` },
      },
    ])
  })

  it("an explicit parent from ctx.trace.run wins, and the message carries the OPERATION", async () => {
    fetchStub = stubFetch()
    const exporter = exporterFor()
    const { sent, context } = recordingContext()

    const handler = otlpHandler(async (_m: CommandMessage, c: typeof context & TraceCapability) => {
      await c.trace.run("reserve-everything", (child) => c.send("d", "p", { traceparent: child.traceparent! }))
    }, exporter)
    await handler(commandMessage(), context as typeof context & TraceCapability)
    await exporter.close()

    const custom = named(fetchStub.spans(), "reserve-everything")
    const operation = named(fetchStub.spans(), "ctx.send")
    expect(custom.parentSpanId).toBe(named(fetchStub.spans(), "billing.ChargeCard").spanId)
    expect(operation.parentSpanId).toBe(custom.spanId)
    expect(sent[0]!.metadata.traceparent).toBe(`00-${operation.traceId}-${operation.spanId}-01`)
  })

  it("a malformed traceparent on the call falls back to the handler scope", async () => {
    fetchStub = stubFetch()
    const exporter = exporterFor()
    const { sent, context } = recordingContext()

    const handler = otlpHandler(
      async (_m: CommandMessage, c: typeof context) => c.send("d", "p", { traceparent: "not-a-traceparent" }),
      exporter,
    )
    await handler(commandMessage(), context)
    await exporter.close()

    const operation = named(fetchStub.spans(), "ctx.send")
    expect(operation.parentSpanId).toBe(named(fetchStub.spans(), "billing.ChargeCard").spanId)
    expect(sent[0]!.metadata.traceparent).toBe(`00-${operation.traceId}-${operation.spanId}-01`)
  })

  it("an unsampled trace stays unsampled through the operation", async () => {
    fetchStub = stubFetch()
    const exporter = exporterFor()
    const { sent, context } = recordingContext()
    const remote = { traceId: "1".repeat(32), spanId: "2".repeat(16) }

    const handler = otlpHandler(async (_m: CommandMessage, c: typeof context) => c.send("d", "p"), exporter)
    await handler(commandMessage({ traceparent: `00-${remote.traceId}-${remote.spanId}-00` }), context)
    await exporter.close()

    expect(fetchStub.spans()).toHaveLength(0)
    // The identity still propagates — same trace, a real span id, flag `00`.
    expect(sent[0]!.metadata.traceparent).toMatch(new RegExp(`^00-${remote.traceId}-[0-9a-f]{16}-00$`))
    expect(sent[0]!.metadata.traceparent).not.toContain(remote.spanId)
  })

  it("keeps concurrent invocations apart", async () => {
    fetchStub = stubFetch()
    const exporter = exporterFor()
    const sent: Array<{ who: unknown; traceparent: unknown }> = []
    const context = {
      send: async (_d: unknown, payload: unknown, metadata?: Metadata) => {
        await new Promise((resolve) => setTimeout(resolve, payload === "slow" ? 20 : 1))
        sent.push({ who: payload, traceparent: metadata?.traceparent })
      },
    }

    const handler = otlpHandler(
      async (m: CommandMessage, c: typeof context) => Promise.all([c.send("d", m.payload), c.send("d", m.payload)]),
      exporter,
    )
    await Promise.all([
      handler({ ...commandMessage(), identifier: "slow", payload: "slow" }, context),
      handler({ ...commandMessage(), identifier: "fast", payload: "fast" }, context),
    ])
    await exporter.close()

    const spans = fetchStub.spans()
    const handlers = spans.filter((span: any) => span.name === "billing.ChargeCard")
    expect(handlers).toHaveLength(2)
    expect(handlers[0].traceId).not.toBe(handlers[1].traceId)

    const operations = spans.filter((span: any) => span.name === "ctx.send")
    expect(operations).toHaveLength(4)
    expect(new Set(sent.map((entry) => entry.traceparent)).size).toBe(4)
    for (const handling of handlers) {
      const own = operations.filter((span: any) => span.parentSpanId === handling.spanId)
      expect(own).toHaveLength(2)
      for (const span of own) expect(span.traceId).toBe(handling.traceId)
    }
  })
})
