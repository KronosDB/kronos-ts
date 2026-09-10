---
"@kronos-ts/axon-server": patch
"@kronos-ts/kronosdb": patch
---

Axon Server reads are bounded and repeated when lost, closing a stream cancels its call, and each gRPC channel owns its subchannel pool.

Under Bun, a gRPC call whose answer has fully arrived — headers, the message, trailers with an OK status, the HTTP/2 stream closed cleanly — could still never complete: grpc-js releases an OK status only after the stream's `end` event, and Bun's http2 client intermittently never emits it (Node does). A command handler then parked on `source` or on the snapshot store's `getLast` until Axon Server cancelled the command at its own 300 s timeout — the stall behind the red Axon jobs. `source`, `getLast` and `getHead` now run under a deadline (`readTimeoutMs` on the connection config, default 15 s): a read that does not answer is cancelled — a non-OK status needs no `end` — and asked once more, with a warning; a second loss is an error. Reads are idempotent, so repeating one is safe.

`close()` on a stream from `axonServerEventStore` only set a flag; the reader stayed parked on the server-streaming call until the next event arrived, which at the head of a quiet stream is never, so every closed stream left a live call behind on the server. The stream now carries an `AbortSignal` and `close()` aborts it.

grpc-js also pools subchannels process-wide, keyed by target address. A later connection to the same `host:port` — a fresh container started on a reused mapped port, or a server restarted behind the same address — could be handed the previous server's HTTP/2 session and have its unary calls hang until the server-side timeout. Both connectors now pass `grpc.use_local_subchannel_pool`, so every channel dials its own connection.
