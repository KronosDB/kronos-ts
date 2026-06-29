---
"@kronos-ts/eventsourcing": minor
---

Carry correlation data onto scheduled events.

`schedule()` now merges the active unit of work's correlation data onto the scheduled event at schedule-time, mirroring `append()`. The fired event carries the correct correlationId/causationId of the message that scheduled it, instead of only the unit-of-work metadata. No-op when no correlation data is set.
