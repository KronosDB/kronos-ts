---
"@kronos-ts/kronosdb": minor
---

The KronosDB client speaks native protocol version 2, where every stream has a byte window. **Needs a KronosDB server that speaks native protocol version 2** (the release after 0.11); a version 1 client is refused by such a server, and this client cannot talk to 0.11.

A server may send a stream only a window of bytes ahead of the client, and the client grants bytes back as its consumer takes the frames. The transport does it, so nothing above it sees a window. A consumer that falls behind now holds back its own stream and nothing else, and is never failed for it. Reads and subscriptions both stop at the window, so a subscription's backlog in memory is bounded by it in bytes, whatever the size of its events. Cancelling a read now stops the server at once, instead of letting it send the rest of the log.

- New: `KronosDbConnectionConfig.window`, the bytes the server may send on each stream ahead of the client (at least 65536; default 8388608, 8 MiB). It is sent in Hello and applies to every stream of the connection.
- **Breaking:** `SubscriptionOptions.credits` is gone (it was accepted by `kronosDbSubscription` and the `streaming` argument of `kronosDbEventStore`). A subscription's read-ahead is the connection's `window`; `StreamingCondition.readAhead` no longer changes what is asked of the server.
- A subscription is opened with a request that carries END, and sends no credits.
- Handler streams (commands and queries) and subscription queries grant `Permits` where they granted `Credit`. `flowControl.permits` and `refillThreshold` mean what they did.
