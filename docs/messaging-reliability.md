# Messaging reliability

Command and query adapters receive new requests while existing handlers await work. Each wire command opens a fresh unit of work. Responses follow their own request identifiers and may complete out of order.

```ts
// Before: a parent awaiting a child blocks delivery of that child.
for await (const request of inbound) {
  await handle(request)
}

// After: each admitted handler owns its response and drain activity.
for await (const request of inbound) {
  void handleAndReply(request) // catches request errors and releases capacity
}
```

## Admission and deadlines

KronosDB and Axon bus options accept `limits`. RabbitMQ accepts the same limits on `rabbitMqConnection`; they apply independently to its command and query transports.

```ts
// Before: handler concurrency and outstanding callers had no explicit bound.
const commands = kronosDbCommandBus(localCommandBus(unitOfWork), kdb)

// After: tune independent handler and caller budgets; observe saturation.
const commands = kronosDbCommandBus(localCommandBus(unitOfWork), kdb, "default", {
  timeoutMs: 30_000,
  limits: {
    maxConcurrentHandlers: 128,
    maxPendingRequests: 1024,
    observe: snapshot => metrics.record(snapshot),
  },
})
```

| Resource | Default and behavior |
| --- | --- |
| Running wire handlers | 128 per bus/transport; excess requests receive an overload error |
| Pending outgoing requests | 1024 per bus/transport; excess callers fail before publishing |
| Request deadline | 30 seconds; configurable through bus `timeoutMs` |
| Graceful drain | 30 seconds; configurable through connection `shutdownTimeoutMs` |
| gRPC outgoing queue | 4096 frames per stream; overflow is an explicit error |
| Subscriber registrations | 1024 per query bus |
| Provider registrations | 1024 per gRPC query bus |
| Axon response-credit records | `maxPendingRequests` (1024); includes early credits, expires at the request deadline |
| RabbitMQ subscription mirror | 16384 entries per registry |
| Subscription consumer buffer | 256 updates by default; positive integer required |
| gRPC subscription credit window | 256–1024, independent of smaller consumer buffers |

These bounds count operations, frames, or registrations, not payload bytes or total process memory. In-process handlers invoked directly through local buses remain under their caller's concurrency policy. Configure broker/server message-size and queue-length policies for deployment-specific byte limits.

Admission never waits for a handler slot. If every admitted parent awaits a child, a child at capacity fails immediately and the error reaches its parent. Arbitrarily deep recursion is not guaranteed to succeed. RabbitMQ uses a channel-wide prefetch of `maxConcurrentHandlers + 1`: the extra delivery can reject a child even when all handler slots belong to parents. A finite prefetch equal to the handler limit would recreate the deadlock.

Query timeouts now default to 30 seconds rather than the previous one-hour server timeout. Set `timeoutMs: 3_600_000` explicitly for queries that require the previous duration; the client deadline follows the same setting.

A caller timing out does not free a still-running handler's slot. gRPC deadlines cancel the client RPC; RabbitMQ deadlines release pending response state and publish requests with message expiry. Neither action forcibly stops JavaScript running in another handler, rolls back a committed transaction, or proves that a command was not executed. Timeout and disconnect outcomes can be ambiguous. Application handlers must use appropriate idempotency and cooperative cancellation for external side effects. Parent deadlines are not propagated as a transactional cancellation boundary to child units of work.

Use a unique message identifier for each dispatch. Concurrent outgoing requests with the same identifier on one bus/transport are rejected. This is correlation protection, not a durable deduplication store. Business idempotency keys should be explicit and retained transactionally by the application.

## Delivery and recovery

| Path | Contract |
| --- | --- |
| Local commands | Fresh unit of work per dispatch; normal handler errors reject the caller |
| Local queries | A supplied live unit of work is reused; otherwise a fresh one is opened |
| Wire commands and queries | Independently handled and correlated; no global completion ordering |
| gRPC provider stream recovery | Reconnect replaces the channel, waits for transport readiness, reopens platform monitoring, and resubscribes handlers; old handlers cannot reply on a replacement stream |
| RabbitMQ routing | Replicas with the same service name compete on one queue; distinct service names create distinct queues and can each receive a copy |
| RabbitMQ disconnect | Pending callers fail immediately; the failed transport refuses reuse; create a replacement connection and register/start its handlers |
| Automatic command replay | No adapter-level replay after an ambiguous request failure |
| RabbitMQ unacknowledged delivery | Broker redelivery remains possible after channel loss; application idempotency is required |

KronosDB provider streams use distinct client identifiers, regenerated on each stream incarnation. Caller and platform requests retain the connection's logical client ID. This accommodates the server's provider lookup by client ID: multiple named buses cannot overwrite each other's streams, and delayed cleanup of an old stream cannot remove its replacement. These provider IDs are visible in server diagnostics and are not stable business identifiers.

Reconnect requests coalesce while an attempt is running. Heartbeat loss, platform EOF, and server reconnect instructions can initiate recovery without an application control-plane handler. Recovery callbacks fire after channel readiness, not merely after constructing a lazy channel; configured attempt limits still apply. Closing the connection interrupts retry delays and aborts its provider RPCs after draining. Unexpected provider EOF/errors use a coalesced timer with backoff and the configured attempt limit; opening an async iterator is not treated as proof that the stream recovered. Sustained traffic resets the stream retry budget.

Axon counts every provider instruction toward receive credits, including subscription acknowledgements. Query response credits are separate: they are correlated by request ID and can arrive before the query itself. Early credits are retained within a bounded, expiring table. Admitted replies keep receiving credits during shutdown so they can finish draining.

A RabbitMQ broker return fails an unroutable request immediately. An existing durable queue with no live consumer can still retain the request until expiry. Normal request errors remain isolated from other requests. Socket backpressure stops additional outgoing publishes until drain; it never republishes a message merely because `publish` returned `false`. Reply-buffer exhaustion fails the transport rather than allowing an unbounded stream of buffered replies.

RabbitMQ connection replacement is explicit; this change does not introduce an automatic reconnect/retry loop. Reuse the logical service name and topology, construct new buses over a new connection, register handlers, and await `start()`. Do not retry an ambiguous command solely because the transport disconnected.

## Subscriptions

Subscription queries provide a transient initial result and update stream. Consumer overflow, initial-result failure, initial-result timeout, transport failure, and shutdown release resources. Returning from the update iterator releases registrations. Concurrent reads on one update buffer reject rather than overwriting and stranding the first reader. An unexpected gRPC EOF after the initial result fails the update iterator.

Updates and terminal subscription operations carrying a unit of work execute after successful commit. Rolled-back work emits no updates.

```ts
// Before: the gRPC adapter discarded the unit-of-work argument.
runAfterCommitOrImmediately(sendUpdate)

// After: updates share the same commit boundary as other subscription buses.
runAfterCommitOrImmediately(sendUpdate, uow)
```

A one-item consumer buffer does not imply one wire credit: refills need time to reach the server. The larger wire window fixes the observed small-window race for a consuming subscriber; an overloaded local consumer still receives an overflow error.

**These streams are not durable event feeds.** KronosDB 0.9 can drop an update when server credits are exhausted without notifying the subscriber. Increasing credit headroom does not make unlimited producer bursts lossless. RabbitMQ's subscription registry remains a best-effort gossip mirror; registration propagation and process failure can lose updates or leave stale remote claims. No replay cursor, gap-free reconnect, or exactly-once subscription delivery is promised. Use a durable event store/processor for changes that must survive failure, and requery/resubscribe after interruption.

The standalone flow-controlled sender now surfaces send failures and completes only after queued updates drain. It is not a substitute for missing end-to-end credit acknowledgements in a server protocol.

## Shutdown

Close rejects new operations, ends subscriptions, and drains admitted handlers. RabbitMQ cancels consumers while leaving the channel open for running handlers' replies. Pending outbound waits are rejected so waiting parents can unwind. gRPC handlers remain tracked through response serialization and handoff to the stream consumer.

When the drain deadline expires, close rejects and tears down the transport in `finally`. A response handoff is not an acknowledgement that the remote caller received it. Noncooperative user code may continue after a forced close; its execution outcome remains ambiguous.

## Verification and release gates

```sh
bun run typecheck
bun run typecheck:messaging-tests
bun run build
bun run test:unit
bun run test:messaging:integration
bun run test:integration
```

Both integration commands run each suite in a fresh process. `test:messaging:integration` selects the messaging suites; `test:integration` discovers all integration suites. It covers RabbitMQ 3.13, Axon Server 2025.2.5, KronosDB 0.9.0, in-memory execution, and transactional PostgreSQL behavior using the existing testcontainer fixtures. The tests exercise the wire path with local shortcuts disabled where relevant.

Regression coverage includes nested success and failure, nested saturation, fresh units of work, out-of-order response correlation, duplicate pending identifiers, isolated codec errors, bounded caller/handler counts, client deadlines, subscription commit/rollback, cancelled initial results, iterator cleanup, shutdown draining and expiry, broker returns, publisher backpressure, channel loss, explicit connection replacement, and bursts of 128 synthetic / 256 real RabbitMQ requests. Real gRPC subscription tests consume 384 updates through a one-item buffer, crossing multiple refill boundaries.

Queries consume stream completion before returning the single result. A missing completion fails at the RPC deadline; the adapter does not replay the query or infer success from a partial stream. The dedicated missing-EOF regression verifies cancellation and no replay. Real tests repeat 20 nested success/overload cycles on each gRPC backend before reconnecting.

Live asynchronous error tests use `node:assert/strict` and check the specific expected error. During investigation, Bun's `rejects.toThrow` matcher could stall live gRPC I/O; older assertions accepting any rejection also allowed a transport timeout to masquerade as a business-rule failure. Changing the assertion preserves the real wire path and strengthens the error check. The repository remains pinned to Bun 1.3.14; no runtime upgrade or completion-grace workaround is required.

The expanded verification passed 1083 unit tests and 263 integration tests across all 32 integration suites, with zero failures or skips. This includes three-node KronosDB cross-node routing, event processors, schedulers, snapshots, PostgreSQL adapters, and dead-letter/token stores, in addition to the messaging regressions. Repository and messaging-test type checks and all package builds passed. CI gates the messaging test types and runs backend groups independently, with a fresh process for each test file.

Remaining production acceptance work requires deployment-specific targets and infrastructure: sustained throughput and latency SLOs, payload-size/heap budgets, soak testing, multi-node broker/server failover and partitions, crash-after-commit idempotency checks, durable subscription protocol/replay design, and automatic RabbitMQ recovery if that is a product requirement. The bounded burst and channel-loss tests are concrete reliability evidence, not certification of those untested guarantees.
