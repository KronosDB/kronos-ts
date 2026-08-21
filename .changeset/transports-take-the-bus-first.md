---
"@kronos-ts/rabbitmq": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/core": minor
---

**BREAKING — transport buses take the local bus FIRST, connection after.** `rabbitMqCommandBus(rabbit, local)` was the last decorator family violating the standing rule: the decorated thing comes first, config after. A transport wraps YOUR bus — same species as `interceptingCommandBus(bus, intercept)` and `otlpCommandBus(bus, exporter)`, which already lead with the bus.

```ts
// before
rabbitMqCommandBus(rabbit, local, { preferLocal })
kronosDbCommandBus(kdb, local)
axonServerCommandBus(conn, local)

// after — the wrapped bus leads, everywhere
rabbitMqCommandBus(local, rabbit, { preferLocal })
kronosDbCommandBus(local, kdb)
axonServerCommandBus(local, conn)
```

The parameter is named `next` in every bus wrapper — transports, intercepting, otlp alike — the same word every handler wrapper uses for the thing it wraps. One pattern, whatever the species: take `next`, return the same shape, config trails.

Stores are unchanged (`kronosDbEventStore(kdb, context)`) — a store is built FROM a connection, not wrapped around a bus, so the resource leads there.

Also in core: interception moved into its own folder, `core/src/interception/` — it is a mechanism of the setup in its own right, built the same way as everything else (functions wrapping over the configuration, providing new capabilities, typechecked at the composition site), and now it sits beside `correlation/` as a peer. Barrel exports are unchanged.
