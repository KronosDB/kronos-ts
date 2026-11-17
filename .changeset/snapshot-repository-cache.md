---
"@kronos-ts/core": patch
---

A state and its `snapshot(state, key, when)` copy get separate repositories on the same event store. Loading one no longer decides whether snapshots apply to the other, and a snapshotted state over a log without `storeSnapshot` is refused even after the plain state was loaded through it.
