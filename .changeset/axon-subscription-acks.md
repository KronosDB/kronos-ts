---
"@kronos-ts/axon-server": minor
---

**Breaking:** the `busSubscriptionAckDelayMs` connection option is removed. `axon.start()` no longer sleeps for a fixed 1000 ms; it waits for Axon Server to acknowledge each handler subscription the command and query buses have sent, and resolves when the last acknowledgement arrives (at once when no handler is subscribed).

`start()` now rejects with an `UnavailableError` when the acknowledgements have not arrived within the new `startTimeoutMs` option (default 30000), and with the server's error when it refuses a subscription. An acknowledgement comes from the node the client is connected to; in a cluster, another node can take a moment longer to route to the handler.

Remove `busSubscriptionAckDelayMs` from the options passed to `axonServerConnection()`.
