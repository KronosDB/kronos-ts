---
"@kronos-ts/test": patch
---

EventPair is readonly (and exported): `as const` event tuples in specs now
satisfy `.events(...)` / `.expectEvents(...)` instead of failing TS4104.
