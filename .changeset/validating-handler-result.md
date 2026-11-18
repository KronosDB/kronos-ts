---
"@kronos-ts/core": minor
---

**Breaking:** `validatingHandler` validates what a command or query handler returns against its descriptor's `result` schema, and the caller gets the parsed answer. A bad answer throws `validate: "<name>" result failed validation — …` inside the handler's unit of work, so a command's appended events roll back. A command without a `result` is not checked. `ctx.emitUpdate` is not wrapped, so subscription updates are not checked against the query's result.

A handler wrapped in `validatingHandler` that answers something its `result` schema rejects now fails. Unknown keys are dropped from an answer when the schema drops them, as they already are from payloads.
