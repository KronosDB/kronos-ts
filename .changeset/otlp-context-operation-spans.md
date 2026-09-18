---
"@kronos-ts/otlp": minor
---

`otlpHandler` traces `ctx.load`, `ctx.source`, `ctx.send` and `ctx.query`.

- Each call is a child span of the handler span, named `ctx.load`, `ctx.source`, `ctx.send` or `ctx.query`. Nothing to configure, and handler code is unchanged. Payloads, state ids, event queries and results are not recorded.
- `ctx.send` and `ctx.query` put their own span's `traceparent` on the outgoing message, so the receiving handler is a child of the operation. With `otlpCommandBus` / `otlpQueryBus` composed, the bus span sits between the two.
- A valid `traceparent` passed in a call's metadata is the operation's parent, which is how a send nests under a `ctx.trace.run` span. A malformed one falls back to the handler span.
- Only capabilities the context already has are wrapped. Other metadata is passed through untouched.
