---
"@kronos-ts/kronosdb": patch
"@kronos-ts/axon-server": patch
---

The KronosDB and Axon Server control planes route split, merge and release instructions to the processor's `splitSegment`, `mergeSegment` and `releaseSegment`, and report each segment a node holds with its own position, `onePartOf`, caught-up, replaying and error state. `activeThreads` is the number of segments held. On KronosDB, a pause waits for the processor to stop, and a refused segment instruction is reported back as a failure.
