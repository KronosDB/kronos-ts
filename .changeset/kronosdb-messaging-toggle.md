---
"@kronos-ts/kronosdb": minor
---

Add a `messaging` option to the `kronosDb()` extension config (default `true`).

With `messaging: false` the extension populates only the `eventStore` and `snapshotStore` slots, leaving `commandBus`/`queryBus` free for another transport (e.g. the RabbitMQ extension) or the in-memory defaults. The platform control plane (processor pause/start/split/merge, status reporting) stays active in both modes.
