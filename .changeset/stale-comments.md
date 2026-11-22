---
"@kronos-ts/core": patch
"@kronos-ts/postgres": patch
---

Comments corrected, no code change. `ctx.query` runs the query in a unit of work of its own and does not share the caller's transaction: the comments on `subscribeQueryHandlers`, `ContextQueryFunction` and the `query` member of the query and event handler contexts said it ran in the caller's. The `EventStore` comment and the header of `postgres-event-store.ts` no longer mention `publish` and `subscribe`, which the event store does not have. The Postgres stream's 250 ms poll is described as running beside LISTEN/NOTIFY, as it does.
