---
"@kronos-ts/core": patch
---

A processor stopped while `start()` was still claiming its segments, then started again, could run two workers on one segment and handle every event twice. It happened when claiming crosses a round trip, as with the Postgres token store. A claim round now belongs to the start that began it: one that answers after a stop gives its claims back, and `stop()` (so also the next `start()`) waits for it.
