---
"@kronos-ts/axon-server": minor
---

A handler's conflict on another instance fails the caller's `dispatch` or `query` with `ConcurrencyError`, which `isConflict` recognises: the handling side answers a command with `AXONIQ-4004` and a query with `AXONIQ-2000` instead of an execution error. A subscription query completed with an error is rebuilt from its code too, so a non-conflict failure arrives as `QueryExecutionError` instead of a plain `Error`.
