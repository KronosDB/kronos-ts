---
"@kronos-ts/postgres": minor
---

A violated append condition throws a subclass of core's `AppendConditionError`, so `isConflict` and `instanceof` core's class recognise it. The package's own `AppendConditionError` export is deprecated.
