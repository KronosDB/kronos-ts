---
"@kronos-ts/core": minor
---

A processor is built from its four parts as arguments, and what it does with its batch is said by wrapping it: `batched`, `concurrent` and `deadLettering`.

- `eventProcessor(name, eventStore, tokenStore, unitOfWork)` replaces `eventProcessor({ ... })`. The `batchSize`, `replayBatchSize`, `sequence` and `deadLetterQueue` fields are gone, and so are `EventProcessorSite`, `EventProcessorLane`, `EventProcessorConfig` and `SequenceDemand`.
- `batched(p, size)`: up to `size` ready events per batch, at the head and behind it. Unwrapped, a processor takes one event at the head and up to 1000 behind, as before.
- A failed batch is retried one event at a time until the token is past it, whatever the batch size.
- `concurrent(p, bucketOf)`: the batch's buckets run side by side. Events in one bucket are handled in log order; events in different buckets are not ordered against each other. The batch still commits as one unit of work, and a failure fails it once every bucket has settled.
- `deadLettering(p, queue, bucketOf)`: parks a failing event and the rest of its bucket under that bucket. With `concurrent` both must be given the same function, or the second wrapper throws.
- `ctx.isReplay()` is fixed per event, read off the token before that event, rather than read off the batch's unit of work.
- Two processors with the same name merge only if they were wrapped the same way.
- `sequentialPerTag` is gone. A bucket per tag value is written out: `(e) => e.tags.find((t) => t.key === "accountId")?.value ?? e.identifier`.
