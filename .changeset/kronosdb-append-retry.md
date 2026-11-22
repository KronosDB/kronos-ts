---
"@kronos-ts/kronosdb": minor
---

An append that KronosDB answers `unavailable`, or whose connection is lost before the answer, is sent again with the same events for up to `appendRetryMs` (a new connection option, default 30000; 0 sends every append once). A server that already wrote the events answers with the positions they have, so nothing is written twice and an append rides through a failover or a restart. Needs KronosDB 0.17 or later: the client asks for the retry-safe appends feature in its handshake and sends an append once to a server that does not grant it. A conflict is never sent again, nor is an append with an event that has no identifier.

**Breaking:** an append to an unreachable server now fails after `appendRetryMs` instead of at once. The vendored protocol codec is regenerated; its schema hash is `5dcacfb3`.
