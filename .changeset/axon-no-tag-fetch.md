---
"@kronos-ts/axon-server": patch
---

`axonServerEventStore` says that it hands events back without their tags (`returnsTags: false`); Axon Server's read responses do not hold them. `upcastingEventStore` over this store narrows a read by type, where it returned no events for a query with tags.
