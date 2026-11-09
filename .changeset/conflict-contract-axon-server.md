---
"@kronos-ts/axon-server": minor
---

The event store throws `ConcurrencyError` when Axon Server refuses an append whose condition was violated, instead of the gRPC client's error. `ConcurrencyError` extends core's `AppendConditionError`, so `isConflict` recognises it, and is no longer an `instanceof AxonServerError`; it keeps its `errorCode` and `transient` fields, and `isTransientError` still returns true for it.
