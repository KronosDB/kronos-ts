---
"@kronos-ts/core": minor
---

`SequencedDeadLetterQueue.process` replays each letter in its own unit of work. It takes a unit-of-work factory as its last argument instead of a unit of work, and `processingTask` receives the letter's unit of work as a second argument: `(letter, uow) => Promise<EnqueueDecision>`.

- A letter that replays successfully is evicted through its unit of work, so the eviction commits with the handler's writes.
- A letter whose task returns `shouldEnqueue: true` has its unit of work rolled back, so the handler's partial writes are discarded. The letter is then requeued with the decision's cause and diagnostics, and the walk stops.
- A task that throws, or a unit of work that fails to commit, rethrows. Letters already evicted stay evicted.
- `deadLetterReprocessor` passes its `unitOfWork` factory to `process` instead of opening one unit of work around the whole walk. `reprocess`, `reprocessAll` and `reprocessDeadLetters` are unchanged.
- `deadLetterBackoff` passes the factory and the letter's unit of work through. Implementations of `SequencedDeadLetterQueue` must take the new `process` signature.
