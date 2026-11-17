---
"@kronos-ts/core": patch
---

`kronos()` runs every boot check before it subscribes a handler: the wrapper-chain check, a command handler without an event store, an event handler without a processor, conflicting processors under one name, and (new) two entries subscribing a handler for the same command or query on the same bus. A failed boot leaves the buses unchanged, so the same buses can be passed to `kronos()` again.
