---
"@kronos-ts/core": minor
---

**Breaking:** `LATEST_TOKEN` is removed. Nothing in Kronos read it, and it could not seed a processor: its position was `Number.MAX_SAFE_INTEGER`, so a processor given it never read an event. `eventStore.latestToken()`, which returns the token at the end of the log, is unchanged.
