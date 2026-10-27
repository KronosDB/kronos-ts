---
"@kronos-ts/kronosdb": minor
---

**Needs KronosDB 0.11 or later.** KronosDB 0.10 and earlier speak gRPC only; stay on `@kronos-ts/kronosdb` 0.9.x for them.

The KronosDB client now speaks KronosDB's native binary protocol for everything: the event store, its snapshotting and scheduling tiers, the command and query buses (queries, subscription queries and serving them) and the platform stream. There is no gRPC and no protobuf left in the package; it depends on `node:net` / `node:tls` and `@kronos-ts/core`. The server serves the native protocol on its `listen` address (with TLS, on that same port).

A single-event append costs about 15–25 µs of client CPU, against about 75–135 µs before (4–6× less), and a 100-event batch about 2–3 µs per event, against 7–19 µs. A command round trip costs the dispatcher about 8–28 µs of CPU and the handler about 7–35 µs, against about 80–210 µs and 40–100 µs over gRPC, and runs 2–7× as many commands per second on the same machine.

Behaviour you may notice:

- Events read back now carry their `tags` (they were `[]`).
- A failed append condition is a `ConcurrencyError` ("consistency condition violated: conflicting event at position N") rather than a raw gRPC error. Other failures are `KronosDbRequestError` with a `status` (`unavailable`, `rejected`, `not_found`, …); `unavailable` is transient. A command or query nobody handles is such an error, with the server's reason ("no handler available …").
- The server's own answers to a dispatch, `KRONOSDB-4005` (no answer in time) and `KRONOSDB-4006` (the handler's connection was lost), come back as `KronosDbError`s with those codes.
- `kdb.start()` now waits until the server has acknowledged every handler a bus announced (it used to wait for the first frame on the platform stream), so the dispatch right after it is routable. `startTimeoutMs` bounds the wait.
- A handler's credit window is counted per command or query name: a grant raises every name a stream subscribed, so a busy name no longer runs dry while a quiet one holds the grants.
- A subscription query's provider holds the updates a subscriber has no credit for and sends them as credit arrives, instead of letting the bus drop them; a subscriber that never grants any is ended with an error after 1024 held updates.
- Handler streams are registered under the connection's `clientId`, and a newer stream of the same client on the same bus replaces an older one. A second bus built for the same bus name on one connection gets a suffixed id of its own.
- Platform instructions are answered (`InstructionResult`), and `report_status` is honoured. The platform stream's `heartbeatTimeoutMs` now means "the server said nothing on the stream for this long" and defaults to 30000 (the server's heartbeat is every 10 s).
- Added: `kronosDbSubscription` (with `Progress` frames), `kronosDbTail`, `kronosDbTagsAt`, `kronosDbPositionAt`, and `scheduleIdempotent` on the scheduling tier, which replaces reaching for `connection.scheduler` to pass a token.

**Breaking:**

- `KronosDbConnectionConfig`: `nativePort`, `keepAliveTimeMs` and `keepAlivePermitWithoutCalls` are gone; `keepAliveTimeoutMs` is now `connectTimeoutMs` (default 10000). The native protocol shares the one port, and the server pings idle connections itself.
- `KronosDbConnection` is a native link and no more: `.channel`, `.platform`, `.commands`, `.queries`, `.eventStore` and `.scheduler` are removed, `.native` is the socket. `reconnect()` keeps a healthy socket and opens a new one only when there is none.
- Removed exports: `kronosMetadata`, `busMetadata`, `metadataToProto`, `metadataFromProto`, `metadataToStringMap`, `metadataFromStringMap`, `outboundStream` / `OutboundStream`, `kronosDbServiceDefinitions` and the `*ServiceDefinition`s, and the generated protobuf code. `KronosDbErrorCode.GRPC_MESSAGE_TOO_LARGE` is gone. `toEventProcessorInfo` now returns a native `ProcessorStatus` frame body.
- `@bufbuild/protobuf`, `@grpc/grpc-js`, `nice-grpc`, `nice-grpc-common` and, for development, `ts-proto`, `grpc-tools` and `grpc_tools_node_protoc_ts` are no longer dependencies; `zod` is a development dependency only.
