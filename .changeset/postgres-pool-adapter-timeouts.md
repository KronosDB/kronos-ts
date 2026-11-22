---
"@kronos-ts/postgres": minor
---

**Breaking:** `postgresPool(adapter, options)` refuses `idleInTransactionTimeoutMs` and `statementTimeoutMs`: they are a compile error and `postgresPool` throws. Beside an adapter they were accepted and ignored, so the adapter's own timeouts (30s idle in transaction by default) applied instead. Set them on the adapter: `postgresPool(pgAdapter({ connectionString, statementTimeoutMs }))`. `postgresPool(connectionString, options)` takes them as before. The options a pool over an adapter takes are exported as `PostgresAdapterPoolOptions`.
