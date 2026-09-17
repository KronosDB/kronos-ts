---
"@kronos-ts/core": major
"@kronos-ts/kronosdb": major
"@kronos-ts/axon-server": major
"@kronos-ts/rabbitmq": major
"@kronos-ts/test": major
"@kronos-ts/otlp": minor
"@kronos-ts/postgres": minor
---

Queries always run in a task of their own, correlation belongs to the invocation, and statements are observable on the postgres family.

- `QueryBus.query(message)` no longer takes a unit of work. Every bus mints a fresh one for every query, so a `ctx.query` from a handler never shares the handler's transaction, clock or correlation, and a co-located query behaves the same as a remote one. Transports pass the message only.
- Build the query bus from the plain `unitOfWork` factory. `postgresUnitOfWork` returns a factory marked transactional, and `localQueryBus` refuses it at compile time and at construction: a read needs no transaction.
- `correlatingHandler(next, from?)` keeps its cargo in the invocation closure instead of on the unit of work, so a batch delivering several events on one task stamps each event's own cause. It demands nothing of the unit of work. `from` defaults to the new `messageOrigin` (correlation id inherited or seeded, causation id the handled message, `traceparent` carried when present).
- Removed from core: `correlating()`, `CorrelatingUnitOfWork`, `attachCorrelationData`, `correlationData`. Delete `correlating(...)` around your factory; cargo previously attached mid-handling is a cargo function now.
- New in core: `Logger`, `consoleLogger`, `loggingHandler` supplying `ctx.log` with message, correlation and trace fields; `describe()` for wrappers that say what they supply, use, stamp and read, which `kronos()` walks at boot and refuses with an error naming the entry, the chain and the fix. Every shipped wrapper describes itself.
- `@kronos-ts/otlp`: `otlpHandler` stamps its span onto the handled message as `traceparent`, supplies `ctx.trace`, and opens a `commit` span from the handler's end to the task's completion; `trace(exporter, parent)`, `inertTrace`, `Trace`, `TraceCapability`; `otlpLogger` over `/v1/logs`. The exporter records an error's type rather than its message by default (`errors: "message"` opts in), bounds its buffers, puts a deadline on every POST, records nothing after `close()`, and reports export failures to `onExportError`.
- `@kronos-ts/postgres`: `observed(pg)` makes every statement through the adapter, and through the driver client `unwrap()` hands out, a `db.statement` span under the handler that issued it; `postgresHandler` built from an observed pool supplies a trace-bound `ctx.sql()` and demands `trace` on its context. Construct Drizzle or Kysely over `ctx.sql().unwrap()` in a slice-owned wrapper; no package is needed.
