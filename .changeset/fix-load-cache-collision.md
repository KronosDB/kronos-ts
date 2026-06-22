---
"@kronos-ts/eventsourcing": patch
---

Fix `load()` returning the wrong entity's state when two different ids of the same state module are loaded within one UnitOfWork.

The per-UnitOfWork state cache keyed on `${module.name}:${String(id)}`. State ids are objects, so `String(id)` produced `"[object Object]"` for every id — collapsing distinct ids of a module to one cache entry, so the second `load()` returned the first's state. The cache key is now a structural serialization of the id (sorted keys, bigint-safe), so distinct ids get distinct entries and id construction order is irrelevant.
