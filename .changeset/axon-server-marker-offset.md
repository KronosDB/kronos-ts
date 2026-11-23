---
"@kronos-ts/axon-server": patch
---

A load from a snapshot in `axonServerSnapshottingEventStore` sees every event appended after the snapshot. A snapshot is stored at the consistency marker of the read it was folded from, which is already the next sequence, and the load started one past it. It skipped the first matching event when that event was the next one appended to the context, so a command handler could decide on state that was missing it, and its append was not refused.

A read from `axonServerEventStore` that starts at or past the head returns a marker that an append is accepted on. On Axon Server 2026.1, an append after such a read was refused as a conflict on every retry.

Snapshots already stored on the server keep working. A snapshot written after a load that skipped an event holds state without that event; delete the context's snapshots to have them rebuilt from the log.
