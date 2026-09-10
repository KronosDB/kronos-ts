---
"@kronos-ts/postgres": patch
---

The Postgres token store and dead-letter queue can be the first writer in a batch. Both observed the unit of work's transaction with `activePostgresTransaction`, but the postgres transaction is lazy and only begins when a writer asks. A projection writing through another client, or an automation that only loads and sends, never began it, so every batch threw "this unit of work carries no postgres transaction" even though the task came from `postgresUnitOfWork`. Both now use `sharedPostgresTransaction`, which opens it when the unit of work is a postgres one and still refuses any other.
