---
"@kronos-ts/core": patch
---

A processor's `stop()` called while an earlier stop is still releasing its segments now waits for that stop too. Before, a second `stop()` resolved at once, so a `resetTokens()` or `start()` after it could claim a segment that the first stop's release, landing late under the same owner, then took away: the reset failed with "the claim is no longer held", and the restarted processor left the segment unhandled until its next claim round.
