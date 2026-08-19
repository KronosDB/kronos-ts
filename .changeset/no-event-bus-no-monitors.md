---
"@kronos-ts/core": minor
"@kronos-ts/axon-server": minor
---

The last four seams nobody implemented: no event bus, no monitors, no retrying bus, no connection manager.

**`simpleEventBus` is DELETED**, along with the `EventBus` and
`SubscribableEventSource` exports. Event delivery here is tracked-only — an
`eventProcessor` over an `EventStore`, reading a stream and keeping a token —
so there is no on-commit lane for an in-memory bus to serve and no caller it
could honestly have had. The two-method SHAPE survives as an INTERNAL type,
because `EventStore` really is both halves (it persists events and it notifies
subscribers) and `EventStore extends EventStorageEngine, EventBus` is how that
gets said once. A host names `EventStore`; it never names `EventBus`.

**`MessageMonitor`, `MonitorCallback`, `noOpMessageMonitor`,
`multiMessageMonitor`, `MessageMonitorRegistry` and `messageMonitorRegistry` are
DELETED.** A monitor is a wrapper over a bus you already have — that is exactly
what `otlpCommandBus(bus, exporter)` is — so the seam bought nothing over
writing the wrapper, and the registry existed to hold monitors nobody ever
registered. Registration is not a concept in this framework.

**`retryingCommandBus`, `RetryPolicy` and `exponentialBackoffRetryPolicy` are
DELETED.** Retrying a command generically is wrong by default: without knowing
whether the handler is idempotent, a retry at the bus is a duplicate write. The
transports that need it retry their own reconnect, where the failure is
transport-shaped and the answer is knowable.

**`connectionManager` / `AxonServerConnectionManager` are DELETED** from
`@kronos-ts/axon-server`. It cached one gRPC channel PER CONTEXT, which is the
thing this release stopped doing: a context is a per-call header on ONE channel,
so a per-context channel cache contradicts `axonServerEventStore(conn, ctx)`
outright. Nothing used it.

None of the five had a consumer anywhere in the tree.
