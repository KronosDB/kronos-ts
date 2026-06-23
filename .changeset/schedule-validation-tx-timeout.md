---
"@kronos-ts/eventsourcing": patch
"@kronos-ts/postgres": minor
---

Validate `schedule()`/`scheduleAfter()` inputs and bound transaction lifetime.

- `schedule()` rejects an invalid `at` (`Invalid Date`); `scheduleAfter()` rejects a non-finite `delayMs`. A past time / negative delay is still allowed and fires as soon as possible.
- `postgresTransactionManager` applies `idle_in_transaction_session_timeout` (default 30000ms) via `SET LOCAL` on every transaction, with an optional `statement_timeout`. Configurable through `postgres({ transaction: { idleInTransactionTimeoutMs, statementTimeoutMs } })`. Set either to `0` to disable.
- `postgresTransactionManager.begin()` now rejects instead of hanging when the transaction callback fails before the handle is returned.
