---
"@kronos-ts/rabbitmq": minor
---

A handler's conflict on another instance fails the caller's `dispatch` or `query` with a conflict that `isConflict` recognises: the reply's error carries its `kind`, and the caller rebuilds core's `AppendConditionError` from it, keeping the remote name, message and stack. A subscription query completed with an error does the same.
