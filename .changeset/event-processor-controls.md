---
"@kronos-ts/messaging": minor
"@kronos-ts/app": minor
---

Expose event processors for host/admin control, with a status snapshot.

- `EventProcessorStatus` (running / error / position / caughtUp / replaying) is added to the common `event-processor` module, and `TrackingEventProcessor` now implements the common `EventProcessor` interface and reports `status()`. The processor tracks caught-up and last-error state, clearing the error once a later batch succeeds.
- `RunningApp.eventProcessors()` returns the built processors keyed by name — the seam a host or admin UI enumerates to read status and call `start()` / `stop()` / `resetTokens()`. The framework ships no watchdog or auto-restart; operating processors is the host's responsibility.
- The `EventProcessorStatus` type previously exported from the streaming processor module is no longer re-exported (the streaming processor keeps its own internal per-segment status type).
