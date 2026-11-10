---
"@kronos-ts/core": minor
---

`UnavailableError` and `isUnavailable(error)` name a failure to retry later: the store or bus could not serve the request now. `FailureKind` includes `"unavailable"`. `MessagingOverloadedError`, the timeouts of `withMessagingTimeout` and `messagingDeadline`, and a full local subscription table carry it.
