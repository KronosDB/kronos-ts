---
"@kronos-ts/core": minor
---

Three renames-and-fixes to the core surface:

- `simpleCommandBus` / `simpleQueryBus` are now `localCommandBus` / `localQueryBus` — the same word the transports already use for that argument, so the composition reads as a sentence: `rabbitMqCommandBus(localCommandBus(unitOfWork), rabbit)`. The `inMemory*` prefix stays reserved for stores, where volatility is the fact worth naming.
- The `lineage` intercept is now `correlation` — it seeds `correlationId`/`causationId`, and the surface already speaks that vocabulary (`correlationData()`, `contributeCorrelationData()`). The word lineage is gone from the codebase.
- Event processor poll timers are `unref()`d, so an all-in-memory process exits naturally when its work is done instead of being held alive by idle pollers.
