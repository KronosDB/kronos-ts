# @kronos-ts/otlp

Traces, metrics and logs as OTLP/JSON over `fetch`. No `@opentelemetry/*`
dependency, no global tracer, no ambient context: trace identity travels on
message metadata as a W3C `traceparent`, and a handler reaches it as `ctx.trace`.

## Wrap the handler, get the rest

```ts
const exporter = otlpExporter({ endpoint: "http://collector:4318", serviceName: "orders" })

kronos({
  commandHandlers: slice.commandHandlers.map((h) => ({
    ...h,
    handler: otlpHandler(correlatingHandler(h.handler), exporter),
    eventStore, commandBus, queryBus,
  })),
})
```

`otlpHandler` goes **outside** `correlatingHandler` and `loggingHandler`: it
stamps its span onto the handled message, and they read that stamp. The order
is checked in the types and again at boot.

One wrapped handler gets, with nothing else to configure:

| What | How |
|---|---|
| A span per handling | Named after the message. Commands and queries parent onto the caller's trace; events start their own trace, linked to the producing span. |
| A `commit` span | From the handler returning to the task being durable. |
| Spans for Kronos operations | `ctx.load`, `ctx.source`, `ctx.send`, `ctx.query` — each call is a child span. |
| Three metrics | `kronos.messages.handled`, `kronos.messages.failed` (with `error_type`), `kronos.message.handler.duration`. |
| `ctx.trace`, `ctx.metrics` | For everything that is not a Kronos operation. |

## Kronos operations are traced for you

The handler is written as it always was:

```ts
const placeOrder = commandHandler(PlaceOrder, async ({ payload }, ctx) => {
  const order = await ctx.load(Order, { orderId: payload.orderId })
  const { available } = await ctx.query(GetAvailability, { sku: payload.sku })
  return ctx.send(ReserveStock, { sku: payload.sku })
})
```

```text
orders.PlaceOrder
├─ ctx.load
├─ ctx.query
│  └─ inventory.GetAvailability
├─ ctx.send
│  └─ inventory.ReserveStock
└─ commit
```

`ctx.send` and `ctx.query` put their own span's `traceparent` on the outgoing
message, so the receiving handler nests under the operation that reached it.
Only the capabilities a context already has are wrapped — a query context
still has no `send`. A span covers the whole call, not each storage request
beneath it. Span names are fixed strings; payloads, state ids, event queries
and results are never recorded.

Business metadata is not touched. Carrying `correlationId`, `causationId` or
an `actor` forward is `correlatingHandler`'s job, as before.

### A custom parent

There is no ambient "current span". To send beneath a span of your own, pass
that span's `traceparent` — a caller that names the key means it:

```ts
await ctx.trace.run("reserve-all-lines", (span) =>
  Promise.all(lines.map((line) => ctx.send(ReserveStock, line, { traceparent: span.traceparent! }))),
)
```

```text
orders.PlaceOrder
└─ reserve-all-lines
   ├─ ctx.send
   │  └─ inventory.ReserveStock
   └─ ctx.send
      └─ inventory.ReserveStock
```

A malformed `traceparent` is treated as absent. An unsampled trace stays
unsampled: nothing is exported, and `-00` propagates so downstream skips it too.

## Everything else takes `ctx`

A client is built once and captured by the handler's closure. Per call, it is
handed the invocation's `ctx` — that is how per-invocation identity reaches a
shared resource:

```ts
export function inventoryClient(baseUrl: string) {
  return {
    available: (sku: string, ctx: TraceCapability & MetricsCapability) =>
      ctx.trace.run("inventory.available", async (span) => {
        const started = performance.now()
        const response = await fetch(new URL("/availability", baseUrl), {
          method: "POST",
          headers: { traceparent: span.traceparent! },
          body: JSON.stringify({ sku }),
        })
        ctx.metrics.record("inventory.request.duration", performance.now() - started, { provider: "warehouse" })
        return response.ok
      }, { kind: SpanKind.CLIENT }),
  }
}

// in the handler
if (!(await inventory.available(payload.sku, ctx))) ctx.metrics.count("orders.rejected", { reason: "out-of-stock" })
```

`ctx.trace.span(fn, { name })` times a function without handing it the child
scope. A client that must also run without tracing takes an optional `Trace`
and falls back to `inertTrace` / `inertMetrics`.

A resource that a handler wrapper supplies needs nothing at all: with
`otlpHandler` outside `postgresHandler`, every statement issued through
`ctx.sql()` — and through `ctx.db` from `drizzleHandler` or `kyselyHandler` —
is a `db.statement` span under the handling, carrying the statement as
`db.query.text` — placeholders and all, never the parameters. The other way round is a compile error: the trace would arrive too
late to span anything. To drop those spans, filter `db.statement` at the
collector.

## The bus wrappers

`otlpCommandBus` and `otlpQueryBus` add a `dispatch(…)` / `query(…)` span
around the bus call and stamp it onto the message. They are not what keeps a
trace connected — metadata does that. They are worth composing for what no
handler can see:

- a dispatch from an **edge**, where there is no `ctx.send` to open a span;
- the **caller's view** of a call over a transport — queueing and network time
  are the gap between `dispatch(X)` and the handler span `X`;
- failures with **no handling** at all: no handler registered, a timeout, a
  broker that is down.

```ts
const traced = otlpCommandBus(rabbitMqCommandBus(localCommandBus(unitOfWork), rabbit), exporter)
const commandBus = interceptingCommandBus(traced, correlation)
```

Composed, the bus span sits between the operation and the handler:
`ctx.send → dispatch(inventory.ReserveStock) → inventory.ReserveStock`.

## Logs

`otlpLogger(exporter)` is core's `Logger` over `/v1/logs`; pair it with
`loggingHandler`. Logging does not need OTLP — `consoleLogger` is the same
contract. `ctx.trace.log(logger)` binds a logger to the current trace and span.

## Shutdown

`await exporter.close()` **last**, after handlers have drained, so nothing
records into a closed exporter.
