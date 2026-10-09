---
"@kronos-ts/postgres": patch
---

The Postgres dead-letter queue claims a lane on the pool, and its replay transaction writes nothing until every handler has run.

- `process()` takes the lane's lease with one committed statement that skips a lane another replayer holds. Two concurrent replayers no longer block on each other or replay the same letters twice.
- The lane's letters are read after the claim, not before.
- The replay transaction takes no transaction id while handlers run, so streaming processors keep reading the event store during a replay. Evictions, the requeue of the failed letter and the lease release are applied together at the end, in the unit of work.
- When the replay's unit of work fails, the lease is released if it is still the caller's. Otherwise it expires after `claimDurationMs`.
- `claimDurationMs` must exceed the longest replay a handler can take. Past it, another replayer may claim the lane and replay it again.
