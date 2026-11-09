---
"@kronos-ts/core": minor
---

`AppendConditionError` carries `kind: "conflict"`, and the new `isConflict(error)` recognises a conflict by that field, including one thrown by another installed copy of core. `AppendConditionError` and the `FailureKind` type are exported from the package root.
