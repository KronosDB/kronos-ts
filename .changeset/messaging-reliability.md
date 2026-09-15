---
"@kronos-ts/core": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/rabbitmq": minor
---

Handle nested commands and queries concurrently over the same adapter connection, with independent response correlation, fresh units of work, bounded admission, and shutdown draining. A handler awaiting another handler on its connection no longer blocks that child's delivery.

```ts
// Before: the receive loop waits for the current handler.
await handleAndReply(request)

// After: each handler owns its response and drain accounting.
void handleAndReply(request)
```

New defaults limit each wire bus/transport to 128 running handlers and 1024 pending outgoing requests. Configure `limits.maxConcurrentHandlers`, `limits.maxPendingRequests`, and `limits.observe` to tune and observe capacity. Overload rejects immediately instead of queuing a nested child behind its waiting parent. RabbitMQ uses one additional prefetch credit to deliver those overload responses.

**Compatibility:** gRPC query timeouts now default to 30 seconds, replacing the previous one-hour server timeout. Set the query bus's `timeoutMs: 3_600_000` explicitly to retain that duration. gRPC command callers also have a 30-second default deadline. Connection shutdown has a configurable `shutdownTimeoutMs` (30 seconds by default); expiry rejects close and still tears down the transport. Caller timeout does not release the slot of a handler that is still running.

Subscription updates and completion respect unit-of-work commit/rollback. Closing an iterator, initial-result failure/cancellation, overflow, EOF, or shutdown settles waiters and releases registrations. Small consumer buffers no longer imply a one-credit wire window. Streams remain transient; this release does not promise durable replay or lossless delivery through server credit exhaustion.

Fix gRPC reconnect readiness, provider stream backoff, monitoring recovery, and provider RPC cancellation on close. KronosDB provider streams use unique incarnation IDs to prevent collisions across named buses and reconnects. Axon acknowledges and accounts for control frames, retains response credits that arrive before their query, and drains admitted replies during shutdown.

RabbitMQ now surfaces unroutable returns, isolates failed channels, observes publisher backpressure, cleans partial initialization, and bounds request/handler state. Failed transports require explicit connection replacement; ambiguous commands are not automatically replayed.

See `docs/messaging-reliability.md` for defaults, migration notes, delivery contracts, and verification boundaries. Validation includes 1083 unit tests, 263 integration tests across 32 suites, package builds, and strict messaging-test type checks.
