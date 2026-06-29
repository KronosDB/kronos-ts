---
"@kronos-ts/messaging": minor
---

Default event processors to propagate handler errors; remove the swallowing logging handler.

- `loggingErrorHandler` is removed. It logged a failed handler and advanced the token, silently skipping the event — which corrupts a read model. AF5 retired the equivalent swallow-and-continue handler to legacy; the only live processor error handler there is the propagating one.
- The default `errorHandler` for tracking, streaming, and subscribing processors is now `propagatingErrorHandler()`. A failed handler no longer advances the token: the batch rolls back and is redelivered (with backoff), so a transient failure recovers on retry and a real bug stops the processor at the offending event instead of skipping it. To deliberately move past a poison pill, attach a dead-letter queue.
- This changes the default behavior of any processor that previously relied on the swallow-and-continue default. Supply a custom `errorHandler` if you need skip-on-error semantics.
