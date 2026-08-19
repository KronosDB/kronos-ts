---
"@kronos-ts/core": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
---

A transport takes your local bus and returns a bus. Core learns no transport vocabulary.

Core briefly owned a generic routing layer — `distributedCommandBus` /
`distributedQueryBus` over a `CommandBusConnector` / `QueryBusConnector` seam,
plus a `SubscriptionRegistry` for the query side, plus `RoutingStrategy`. All of
it is DELETED. Nothing but a transport ever implemented those interfaces, and
splitting one routing decision across a package boundary meant the reply
timeout, the identity-named reply queue and the prefer-local fork lived in two
places that had to agree.

Every transport now exposes the SAME two-argument shape: it takes your local
segment and returns a bus of the same type, so composition is uniform and
interception always wraps from outside.

```ts
// before — a generic router in core, a connector in the transport
interceptingCommandBus(
  distributedCommandBus(local, rabbitMqCommandConnector(rabbit)), lineage)

// after — the transport owns its own routing
interceptingCommandBus(rabbitMqCommandBus(rabbit, local), lineage)
interceptingCommandBus(kronosDbCommandBus(kdb, local), lineage)
interceptingCommandBus(axonServerCommandBus(conn, local), lineage)
```

**Deleted from `@kronos-ts/core`:** `CommandBusConnector`, `QueryBusConnector`,
`RemoteDispatchOptions`, `SubscriptionRegistry`, `SubscriberRecord`,
`SubscriptionDelivery`, `distributedCommandBus`, `distributedQueryBus` and their
options types, `RoutingStrategy`, `metadataRoutingStrategy`,
`payloadFieldRoutingStrategy`. `SerializedError` survives, moved to the
primitives beside `generateIdentifier`. Core now contains no word for "remote".

**`@kronos-ts/rabbitmq`** — `amqpConnection` is renamed `rabbitMqConnection`,
and its options are exactly what the broker topology needs:
`rabbitMqConnection(url, { serviceName, instanceId, topology?, retry? })`. The
connector pair is gone; `rabbitMqCommandBus(rabbit, local, { preferLocal?,
timeoutMs? }?)` and `rabbitMqQueryBus(rabbit, local, { preferLocal?, timeoutMs?
}?)` absorb what the router and the connector did together. Reply timeouts moved
from the connection to the bus, where the dispatch that waits actually is.
Client-side routing semantics are unchanged: competing consumers on durable
per-command queues, identity-named exclusive reply queues, correlation-id
matched replies, dead-lettering, and the gossip subscriber mirror that lets a
plain function predicate filter subscription queries across instances.

**`@kronos-ts/kronosdb` and `@kronos-ts/axon-server`** — the local segment
becomes a REAL bus. It used to be a private `Map<string, handler>`, and an
inbound server-routed command was run in a freshly minted `unitOfWork()` that
the bus was separately handed. Now `subscribe` registers on `local` and
announces the name to the server, and inbound work is dispatched INTO `local`:

```ts
// before — two sources of truth for one policy
kronosDbCommandBus(connection, unitOfWork, latch, serializer, flowControl, loadFactor)

// after — `local` carries the unit-of-work policy, and only `local`
kronosDbCommandBus(kdb, simpleCommandBus(unitOfWork))
kronosDbCommandBus(kdb, postgresUnitOfWork(pg, unitOfWork) |> simpleCommandBus)
```

That is what makes a `postgresUnitOfWork` apply to server-routed work exactly as
it applies to local work. Server-side routing is untouched: both are smart hubs,
so an outbound dispatch still always goes to the server — there is no
client-side prefer-local fork on either.
