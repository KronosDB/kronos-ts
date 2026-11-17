---
"@kronos-ts/core": patch
---

`upcastingEventStore` translates type filters to their stored names on `source()` and on the query of an append condition passed to `append` or `appendEvents`, as it already did on `open()`. A state that folds a renamed event now sees the events stored under the old name, and an append conditioned on the new name conflicts with an event appended under the old one. Without `storedTypes`, `source()` drops the type filter, upcasts, and returns only the asked types; an append condition drops its type filter and checks every event carrying its tags.
