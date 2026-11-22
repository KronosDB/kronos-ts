---
"@kronos-ts/axon-server": patch
---

Events read from `axonServerEventStore` carry their tags, on `source()` and on `open()`. They came back with `tags: []`, so `upcastingEventStore` over this store returned no events for a query with tags, and a processor `Sequence` or handler that read `event.tags` found none.

Axon Server's read responses hold no tags, so the store asks for each event's tags with `GetTags`, at most 64 calls at a time. This costs about 0.05 ms per event read: a `source()` of 100 events takes about 6 ms where it took 1 ms. Tags come back in the server's order, which need not be the order they were appended in. A read fails when an event's tags cannot be read.
