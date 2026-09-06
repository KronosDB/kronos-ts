---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
---

Cleans up internal API surfaces that had no callers. BREAKING only for code that imported them; runtime behaviour of stores, processors and buses is unchanged.

- **Segments are gone.** `Segment`, `ROOT_SEGMENT`, `segment`, `segmentMatches`, `splitSegment`, `mergeSegments`, `isMergeable`, `segmentCount`, `hashOf`, `segments`. Processors are single-lane and `segment` was hard-coded to 0.
- **`MessageStream` is the seven members a processor pulls through**: `next`, `peek`, `hasNextAvailable`, `isCompleted`, `error`, `setCallback`, `close`. `map`, `filter`, `reduce`, `concatWith`, `onErrorContinue`, `messageStream`, `emptyMessageStream` and `failedMessageStream` had no caller.
- **`EventStore` is one type.** `EventStorageEngine`, `EventBus`, `EventSink` and `SubscribableEventSource` are gone with the `publish` and `subscribe` members on every store. Processors read through `open()` and are woken by the store; nothing subscribed. `AppendTransaction` is exported from `event-store.ts`.
- **`Phase.POST_INVOCATION` is gone.** Nothing ever registered on it.
- **`resetTokens(position)` takes no `resetContext`.** It was persisted into the replay token and readable by no handler.
- **Control planes report what a processor has.** `ManagedEventProcessor` (kronosdb, axon-server) is `name`, `running`, `replaying`, `position`, `start`, `stop`, `status()`. `splitSegment`, `mergeSegment`, `releaseSegment`, `processingStatus` and `supportsReset` are gone; segment instructions from the server are ignored (they were already no-ops). `ProcessorStatus` is `{ name, running, caughtUp, replaying, position, error? }`; the wire's thread counts and segment list are filled in `toEventProcessorInfo`. One observable difference: `caughtUp` and `error` on the status report are now the processor's real values instead of a constant `true` and `false`.
