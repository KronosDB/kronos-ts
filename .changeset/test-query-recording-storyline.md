---
"@kronos-ts/test": patch
---

`QueryRecording` is exported. It is what `recordingQueryBus` adds to a bus and what `RecordingQueryBus` is built from, and it could not be named from the package. A storyline has `catch` and `finally` as well as `then`, and its type is `Promise<void> & { shows; after }` where it was `PromiseLike<void>`. A storyline is told once, when it is first awaited or `then`, `catch` or `finally` is first called; awaiting the same storyline value again no longer tells it again. It is still not a `Promise` instance: pass `Promise.resolve(storyline)` to something that requires one, such as `expect(…).rejects`.
