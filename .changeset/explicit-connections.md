---
"@kronos-ts/rabbitmq": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
---

Connection is an explicit resource; everything else is a function over it.

All three transports shipped as a bundle that hid one connection behind a
`{ components, start, close }` record. A bundle is only legitimate when its
members share a hidden resource — so the resource is named, and each capability
becomes a function of it.

**RabbitMQ.** `rabbitMq(options)` is DELETED. `rabbitMqConnection(url, {
serviceName, instanceId, topology?, retry? })` is the resource — one broker
connection, its three channels, and the lifecycle — and the buses are plain
functions over it.

```ts
// before — one bundle, both buses whether you wanted them or not
const rabbit = await rabbitMq({
  url, identity: { serviceName, instanceId },
  localCommandBus: base.commandBus, localQueryBus: base.queryBus,
})

// after — the resource is named; take only the capabilities you need
const rabbit = await rabbitMqConnection(url, { serviceName, instanceId })
const commandBus = interceptingCommandBus(rabbitMqCommandBus(rabbit, base.commandBus), lineage)
await rabbit.start()   // unchanged: bind-and-consume barrier, after handlers register
```

The old channel-only `amqpConnection(url, connect)` is now `amqpChannelSource`,
which is what a transport borrows.

**KronosDB and Axon Server.** `kronosDb(options)` and `axonServer(options)` are
DELETED — each opened a gRPC channel, a platform stream and a heartbeat PER
CALL, so a service addressing N contexts paid N connections. `kronosDbConnection`
/ `axonServerConnection` own the one channel.

`kronosDbContext(kdb, options)` — the four-components-at-once record that sat in
between — is DELETED too, and so is Axon's `AxonServerBackend`. A context is not
a thing you build; it is a STRING two functions take:

```ts
// before — one connection per context, then a bundle per context
const billing = await kronosDb({ componentName, context: "billing", serializer, unitOfWorkFactory })

// after — one channel, N contexts, one function per seam
const kdb = await kronosDbConnection({ componentName, serializer })
const billingEvents = kronosDbEventStore(kdb, "billing")
const catalogEvents = kronosDbEventStore(kdb, "catalog")
await kdb.start()   // ONE readiness barrier, idempotent, covering every context
```

Both still share the one socket — the per-call context header is the whole
difference — and now the caller who wants only an event store builds only an
event store.

The SERIALIZER moves onto the connection options. It is a property of this
client's wire, not of a context or a bus, and a store keyed by
`(connection, context)` had nowhere honest to put it. The remaining per-bus
knobs (flow control, load factor, query timeout, the local-shortcut flag,
resilience) live in one trailing optional record.

`start()` keeps the platform-start / subscriptionsAcked ordering and is
memoised, so N contexts share one barrier. `close()` drains every bus latch
registered on the connection before stopping the platform stream and the
channel. `kronosDbControlPlane(kdb, app.processors)` /
`axonServerControlPlane(conn, app.processors)` take the connection.
