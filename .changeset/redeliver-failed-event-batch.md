---
"@kronos-ts/messaging": patch
---

Fix streaming/tracking event processors skipping a failed event batch until restart

When an event handler batch failed (the UnitOfWork aborted before `PREPARE_COMMIT`), the tracking token was correctly held back, but the live `MessageStream` cursor had already advanced past the batch during accumulation. Nothing realigned the stream to the un-committed checkpoint, so the next poll read the *next* event and the failed batch was silently skipped until the processor restarted and re-read the token store.

Both `createTrackingEventProcessor` and `createStreamingEventProcessor` now close and discard the stream on a batch failure and reopen it from the committed token on the next cycle, so the failed events are redelivered without requiring a restart. This matches Axon Framework's close-and-reopen-from-token recovery contract. `createStreamingEventProcessor` gains an `errorBackoffMs` option (default 1000ms) to throttle retries of a deterministically failing handler.
