---
"@kronos-ts/kronosdb": minor
---

Failures to retry later carry core's `unavailable` kind, so `isUnavailable` recognises them: `ConnectionFailedError`, a native `unavailable` refusal, the dispatch, leader, command-timeout and lost-handler codes, a deadline, a shutdown (`ShutdownInProgressError` now extends core's `UnavailableError`) and a full subscription table. A handler's unavailable failure reaches a caller on another instance as `KRONOS-3001`, rebuilt as `ConnectionFailedError`. `isTransientError` is deprecated in favour of core's `isUnavailable` and `isConflict`.
