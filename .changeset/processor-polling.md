---
"@kronos-ts/core": minor
---

`polling(p, ms)` sets how long an idle processor waits before it looks again, and the base of its retry delay. It replaces `runEventProcessor({ pollingIntervalMs })`, which `kronos()` never passed, so processors started by `kronos()` can now be tuned. Two entries naming one processor with different intervals are a boot error.

- `batched` and `polling` take anything with the field they set, so they also tune a store's polling worker.
