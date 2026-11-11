---
"@kronos-ts/axon-server": minor
---

Failures to retry later carry core's `unavailable` kind, so `isUnavailable` recognises them: `ConnectionFailedError`, the dispatch and no-leader codes, gRPC `UNAVAILABLE` and `DEADLINE_EXCEEDED` from the event store and buses (now a `ConnectionFailedError` with the gRPC error as its cause), a dispatch deadline (reported as the deadline instead of the client's cancellation), a read that timed out twice, a shutdown (`ShutdownInProgressError` now extends core's `UnavailableError`) and a full subscription table. A handler's unavailable failure reaches a caller on another instance as `AXONIQ-3001`. `GRPC_MESSAGE_TOO_LARGE` is no longer a `ConnectionFailedError` and is not transient: retrying does not make a message smaller. `isTransientError` is deprecated in favour of core's `isUnavailable` and `isConflict`.
