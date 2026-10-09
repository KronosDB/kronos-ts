---
"@kronos-ts/postgres": patch
---

The Postgres dead-letter queue replays each letter in its own transaction, and `enqueue` no longer reads whole lanes.

- A failing replay's handler writes are rolled back before the letter is requeued. The letters before it stay evicted, with their handlers' writes.
- Handler writes no longer hold a transaction id across the rest of the lane, so the event store's tail is held back only while one letter's transaction is open.
- Before a letter's eviction commits, `process()` claims the next letter's lease with a statement that never waits on a lock. `claimDurationMs` now has to outlast one letter's replay, not the whole lane. If the next letter cannot be claimed, the walk stops after the current letter.
- `enqueue`, `enqueueIfPresent`, `isFull` and `amountOfSequences` use `count(*)`, `max(sequence_index)` and `count(DISTINCT sequence_identifier)` instead of reading every letter in the lane. The caps, defaults and overflow error are unchanged.
