---
"@kronos-ts/core": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/postgres": minor
---

A processor opens its stream with the event types its handlers handle, so a log that filters sends nothing else, and it uses a larger batch while it is behind the head.

- The query is a request. An event nobody handles is still dropped on arrival, so a store that ignores it is only slower.
- `MessageStream` has an optional `progressToken()`: how far a filtered stream has looked. A processor stores it as its token when it has nothing to do, so a processor of rare events no longer sits at its last match, and a replay whose last events are quiet ends. The in-memory and Postgres stores and KronosDB (Progress frames) report it.
- A KronosDB `Progress` is held back until the events that arrived before it have been taken. `kronosDbEventStore(kdb, context, streaming?)` takes the subscription options; a stream with a query asks for Progress every second.
- `eventProcessor({ replayBatchSize })`: events per batch while behind the head, default 1000, never below `batchSize`. `StreamingCondition.readAhead` asks a stream to hold that many ready. A failed batch is retried with `batchSize` until the token is past it.
- `upcastingEventStore(next, upcast, { storedTypes })` translates a stream's type filter to the names events are stored under. Without `storedTypes` the type restriction is dropped from the query, not trusted.
- The processor's hop between batches is no longer unref'd. Under Node the next batch waited for the next socket read or timer, a millisecond or two a batch.
