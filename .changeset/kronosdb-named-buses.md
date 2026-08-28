---
"@kronos-ts/kronosdb": minor
---

Buses are named, contexts are logs: the bus wrappers take a plain bus-name
string and stop addressing event store contexts. Aligns with server 0.9
(ADR-0006 decoupled buses, ADR-0007 messaging fabric). BREAKING.

```ts
// before — messaging borrowed the store's context header
kronosDbCommandBus(next, kdb, { context: "orders" })
kronosDbQueryBus(next, kdb, { context: "orders" })

// after — a bus is its own dimension, a plain string, "default" when omitted
kronosDbCommandBus(next, kdb, "orders")
kronosDbQueryBus(next, kdb, "orders", { timeoutMs })
```

Every messaging RPC (handler streams, dispatch, query, subscription queries)
now carries the per-call `kronosdb-bus` header and never `kronosdb-context`.
The server has NO fallback from bus to context — against a 0.9 server, a
client that named contexts for messaging isolation lands on the `default`
bus; name your buses (after your contexts, if 1:1 is what you meant). Store
wrappers are unchanged and keep `kronosdb-context`. With the 0.9 fabric a bus
name is cluster-wide: subscribe via any node, dispatch via any node.

Also: `kronosDbSchedulingEventStore` takes an optional trailing
`context: string` (defaults to the connection's), matching the other store
wrappers; `busMetadata(bus, { token })` is exported beside `kronosMetadata`.
