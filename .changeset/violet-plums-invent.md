---
"@kronos-ts/kronosdb": minor
---

Event scheduler client for KronosDB's server-side scheduled appends.

`createKronosDbScheduler(connection, serializer)` exposes `schedule`, `cancel`,
and `list`. The store appends the event when due — no client-side timers or
polling — and the schedule is durable once `schedule()` resolves. Supply your
own token to make retried schedule calls idempotent. gRPC failures map to
typed errors: `ScheduleAlreadyExistsError`, `ScheduleAlreadyResolvedError`,
`ScheduleNotFoundError`.
