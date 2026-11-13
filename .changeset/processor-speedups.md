---
"@kronos-ts/core": patch
---

Event processors await a handler only when it returns a promise; order is unchanged, and a synchronous throw still fails the batch. A batch's tracking token is built once, from its last event, unless the segment is replaying or merged. `qualifiedNameToString` returns the same string for the same name object.
