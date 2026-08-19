---
"@kronos-ts/core": minor
---

Add `ctx.query` to the command, event, and query handler contexts — the in-handler
consult, AF5-style (inject the query gateway anywhere): dispatches through the
active query bus inside the current UnitOfWork, carrying the caller's
correlation metadata like `ctx.send`. `InferResult` is now exported.
