---
"@kronos-ts/postgres": minor
---

**Breaking:** `bootstrapSchema` takes an adapter with `transaction` (a `PostgresAdapter` or a `postgresPool`), and `SchemaBootstrapAdapter` is now `Pick<PostgresAdapter, "transaction">`; an object with only `query` is no longer accepted. `bootstrapSchema`, and so `postgresPool().start()`, runs each DDL statement in its own transaction that first takes a transaction-scoped advisory lock. It took a session-level lock through the pool, so the lock, the DDL and the unlock ran on different connections: the lock could stay held after start, concurrent starts could fail with a duplicate-key error or wait forever, and the lock did not serialise them. No session-level lock is taken any more.
