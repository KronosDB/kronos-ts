---
"@kronos-ts/app": minor
"@kronos-ts/messaging": minor
"@kronos-ts/eventsourcing": minor
"@kronos-ts/modelling": minor
"@kronos-ts/test": minor
"@kronos-ts/postgres": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/opentelemetry": minor
"@kronos-ts/drizzle": minor
---

Functional composition — the container is gone.

BREAKING. The app builder, slot registry, decorator pipeline, lifecycle
stages, extensions-as-mutators and `defineModule` are removed. The entry
point is `kronos({ components, modules })`; a module is
`module(name, overrides?, ...registrations)` with per-state snapshot
options as `[state, options]` tuples; dependencies are plain function
arguments.

Handlers receive their capabilities as a typed context argument
(`load`/`append`/`send`/`emitUpdate`/`schedule`/`transaction`); the
module-level `append`/`load`/`send`/`emitUpdate` helpers are no longer
exported. Query handlers get a read-only context (`load`/`transaction`).
`append` accepts a batch of `[descriptor, payload]` tuples. `on()` is
evolver/query only; `onEvent()` is removed.

Every backend extension is an async factory returning
`{ components, start(), close() }` with a uniform no-arg `start()`.
Remote processor administration moved to opt-in control planes
(`kronosDbControlPlane`, `axonServerControlPlane`) fed by
`app.processors`. Correlation lineage now survives distributed dispatch
(interception sits above the local/remote fork) and is seeded from
incoming messages in the invocation wrappers. Axon reconnect detection is
armed by the data path, independent of the control plane.

All 59 `create*`-prefixed factories are renamed to what they return
(`inMemoryEventStore`, `simpleCommandBus`, `postgresEventStore`, …).
Drizzle stores no longer take the ORM operator bundle — only
`{ db, table }`.
