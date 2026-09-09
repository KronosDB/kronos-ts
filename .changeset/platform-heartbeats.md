---
"@kronos-ts/axon-server": patch
"@kronos-ts/kronosdb": patch
---

The platform stream answers every heartbeat the server sends, and the Axon Server client beats every 2.5 s instead of every 10 s.

Axon Server's `client-heartbeat-timeout` defaults to 5000 ms and is refreshed only by heartbeats the client sends, checked once a second. This client beat every 10 s and never echoed the server's, so the server cancelled the stream ("Platform stream inactivity") whenever a check fell in the second half of that interval — every long-running connection, and every CI run under load. Both clients now echo each server heartbeat at once (what Axon Framework's connector does); the Axon client's cadence is half the server's window, the KronosDB client's is a third of its 15 s default. No beat leaves before the server has acknowledged the registration — a heartbeat from a stream the server has not filed yet is an error on its side, and at a 2.5 s cadence the first one could get there first.

The Axon Server client also stops tearing down a healthy stream. Its silence check assumed the server beats at every client; Axon Server beats only at clients whose registered framework version it recognises, so this client — which it never beats at — judged the silence as a dead channel and reconnected every heartbeat window, in production, forever. The check now applies only once the server has beaten at least once; a server that never beats is left to gRPC keepalive.
