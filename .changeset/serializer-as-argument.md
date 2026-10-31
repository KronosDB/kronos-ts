---
"@kronos-ts/postgres": minor
"@kronos-ts/kronosdb": minor
---

`postgresSnapshottingEventStore` takes the serializer as its third argument: `postgresSnapshottingEventStore(next, pg, serializer)`. `PostgresSnapshottingEventStoreConfig` is removed.

`kronosDbSchedulingEventStore` encodes scheduled payloads with the connection's serializer and no longer takes an options argument: `kronosDbSchedulingEventStore(next, kdb, context?)`. `KronosDbSchedulingOptions` is removed.
