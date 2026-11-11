---
"@kronos-ts/rabbitmq": minor
---

Failures to retry later throw core's `UnavailableError`, which `isUnavailable` recognises: a transport or registry that is not connected or closed, a shutdown, a full publish or reply buffer, a closed channel, a timed-out request and a full subscription table. A handler's unavailable failure reaches a caller on another instance with its kind.
