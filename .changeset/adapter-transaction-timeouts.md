---
"@kronos-ts/postgres": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
---

Move per-transaction safety timeouts onto the database adapter.

- `pgAdapter`, `postgresAdapter`, and `bunSqlAdapter` now accept `idleInTransactionTimeoutMs` (default 30000) and `statementTimeoutMs` (default 0) and arm them via `SET LOCAL` on every transaction they open — UoW-scoped commits, event-store own-tx appends, and the scheduler worker tick alike. Each adapter instance is configured independently, so two adapters pointed at two databases stay decoupled.
- `postgresTransactionManager` no longer takes timeout options and no longer issues `SET LOCAL`; it is now a pure begin/commit/rollback bridge. The `postgres({ transaction: { ... } })` config is removed — set the timeouts on the adapter instead.
- `drizzleTransactionManager` and `knexTransactionManager` accept an `onBeginTransaction(tx)` hook that runs once per transaction, before the UnitOfWork uses it — the seam for arming session settings (e.g. `SET LOCAL idle_in_transaction_session_timeout`) on those clients so a stalled drain is bounded.
