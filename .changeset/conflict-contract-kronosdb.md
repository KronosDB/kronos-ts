---
"@kronos-ts/kronosdb": minor
---

`ConcurrencyError` extends core's `AppendConditionError`, so `isConflict` recognises a KronosDB conflict. It is no longer an `instanceof KronosDbError`; it keeps its `errorCode` and `transient` fields, and `isTransientError` still returns true for it.
