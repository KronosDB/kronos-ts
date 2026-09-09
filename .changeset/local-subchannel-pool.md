---
"@kronos-ts/axon-server": patch
"@kronos-ts/kronosdb": patch
---

Each gRPC channel owns its subchannel pool.

grpc-js pools subchannels process-wide, keyed by target address. A later connection to the same `host:port` — a fresh container started on a reused mapped port, or a server restarted behind the same address — could be handed the previous server's HTTP/2 session and have its unary calls hang until the server-side timeout. Both connectors now pass `grpc.use_local_subchannel_pool`, so every channel dials its own connection.
