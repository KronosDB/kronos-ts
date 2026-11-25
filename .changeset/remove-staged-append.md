---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/test": minor
---

**Breaking:** `EventStore.appendEvents` and the `AppendTransaction` type are removed, from the contract and from every store. Nothing in the packages called them: a unit of work writes through `append`, and an append that must commit with other work is `append(events, condition, uow)`. `eventStoreTransaction` and `EventStoreTransaction`, which nothing used, are removed from `@kronos-ts/core`.
