---
"@kronos-ts/core": patch
---

The event processor no longer pays a millisecond between batches, and no longer runs a no-op claim update per batch.

The loop rescheduled itself with `setTimeout(poll, 0)`; both Bun and Node clamp that to a millisecond, which at the default batch size of 1 capped a processor at under a thousand events a second before any handler ran. It now hops with `setImmediate`. Each batch also ran `extendClaim` inside the token transaction, an update that matched zero rows because nothing ever takes a claim on this path; only the token is written now. With the drizzle token store on Postgres: batch size 1 went from about 175 to about 580 events/s, batch size 10 from about 1,500 to about 2,100.
