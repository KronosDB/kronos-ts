---
"@kronos-ts/axon-server": patch
---

`axonServerConnection()` waits for the gRPC channel to Axon Server to be ready before it returns. If the channel is not ready within `resilience.healthCheckThresholdMs` (default 5000), it logs one warning through `resilience.log` (default `console.warn`) and returns the handle anyway. Before, the health check ran an empty probe: the handle was returned at once, and an unreachable server was first noticed by the first call.

Opening a connection while Axon Server is down now takes up to `healthCheckThresholdMs` longer.
