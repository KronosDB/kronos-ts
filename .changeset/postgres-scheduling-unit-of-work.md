---
"@kronos-ts/postgres": minor
---

`postgresSchedulingEventStore(next, pg, unitOfWork)` takes its unit-of-work factory as the third argument and has no options. Its worker is tuned by wrapping the store: `batched(store, n)` for rows a tick (default 50) and `polling(store, ms)` for the interval (default 1000). `startScheduling()` reads both off the value it is called on. `PostgresSchedulingConfig` is removed.
