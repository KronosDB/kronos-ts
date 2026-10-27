---
"@kronos-ts/core": minor
---

A processor value can be wrapped with what it does with its batch: `batched`, `concurrent` and `deadLettering`.

- `batched(p, size)`: up to `size` ready events per batch, at the head and behind it. A failed batch is still retried one event at a time until the token is past it.
- `concurrent(p, bucketOf)`: the batch's buckets run side by side. Events in one bucket are handled in log order; events in different buckets are not ordered against each other. The batch still commits as one unit of work, and a failure fails it once every bucket has settled.
- `deadLettering(p, queue, bucketOf)`: parks a failing event and the rest of its bucket under that bucket. With `concurrent` both must be given the same function, or the second wrapper throws.
- `ctx.isReplay()` is fixed per event, read off the token before that event, rather than read off the batch's unit of work.
- Two processors with the same name merge only if their `concurrent` functions are the same function.
