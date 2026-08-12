---
"@kronos-ts/kronosdb": patch
---

Reconnect backoff now applies ±25% jitter so scaled-out service instances
that lose their connection together (server restart, failover) don't
reconnect as one synchronized wave.
