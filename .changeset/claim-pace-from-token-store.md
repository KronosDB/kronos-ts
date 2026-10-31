---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
---

`runEventProcessor` no longer takes `claimIntervalMs`. A processor now looks for free segments and renews its claims every half of its token store's claim timeout, read from the new optional `TokenStore.claimTimeoutMs` field. `inMemoryTokenStore(ms)` and `postgresTokenStore(pg, { claimTimeoutMs })` set it; a store without it is treated as 10000 ms, so the default pace stays 5 seconds.
