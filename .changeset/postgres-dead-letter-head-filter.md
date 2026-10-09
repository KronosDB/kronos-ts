---
"@kronos-ts/postgres": patch
---

The Postgres dead-letter queue's `process()` passes each lane's head letter to the sequence filter, and checks the filter again against the head after claiming the lane. A lane whose head no longer passes is released and the next lane is tried; when none is left, `process()` returns false.
