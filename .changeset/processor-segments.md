---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
---

A processor is now one or more segments, each with its own token and claim, so several nodes can run the same processor without handling an event twice.

- Unwrapped, a processor is one segment: one node handles it and the others wait, taking over when it stops or its claim lapses.
- `segmented(p, count, sequenceOf)` starts a processor as `count` segments (a power of two). An event's segment is decided by hashing its sequence id, so a sequence stays in one segment and keeps its order. `concurrent`, `deadLettering` and `segmented` must be given the same function.
- A node claims every free segment on start and every 5 seconds after, renews what it holds, and releases on stop. The segments a node holds share one read of the log.
- Token writes are fenced on the claim: `TokenStore.store` takes an optional trailing `owner`, and writes only while that owner holds the claim. Both the in-memory and Postgres stores implement it.
- The running processor has `segments()`, `releaseSegment(id, forMs?)`, `splitSegment(id)` and `mergeSegment(id)`. Balancing and scaling are left to whoever operates the system.
- `RunningProcessor.stop()` now returns a promise, resolved once in-flight batches have finished and the claims are released.
- `resetTokens` resets every segment, and refuses while another node holds one.
