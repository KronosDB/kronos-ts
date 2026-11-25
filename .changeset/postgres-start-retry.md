---
"@kronos-ts/postgres": patch
---

`postgresPool.start()` connects and bootstraps again only after an error that says the server could not be reached or cannot serve yet. An error another attempt cannot fix, such as a wrong password or a missing permission, fails `start()` at once; it was retried ten times with backoff first. A host's own `resilience.isRetryable` still decides when given.
