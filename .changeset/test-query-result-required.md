---
"@kronos-ts/test": minor
---

**Breaking:** `query(descriptor, payload)` and `QueryValue<P, R>` take a query that declares its result, as `@kronos-ts/core` now requires of every query. `R` no longer accepts `undefined`.
