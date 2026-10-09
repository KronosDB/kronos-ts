---
"@kronos-ts/core": minor
---

Added `deadLetterBackoff(queue, policy)`, which wraps a dead-letter queue so a lane whose head failed is skipped by `process` until the policy's delay has passed, and `exponentialBackoff(options?)`, a policy with a cap and jitter.

- The policy returns a delay in milliseconds, or `"hold"` to keep the lane parked until `queue.retryNow(processingGroup, sequenceIdentifier)` releases it. `retryNow` keeps the letter's attempt count.
- The wrapper only delays. It never evicts a letter, however many times it fails.
- `SequencedDeadLetterQueue.process` calls its `sequenceFilter` with the lane's head letter as a second argument: `(sequenceId, head) => boolean`. An implementation must pass the head letter, and must check the filter again against the head once it has claimed the lane. The filters of `deadLetterReprocessor` and `reprocessDeadLetters` are unchanged.
