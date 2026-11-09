---
"@kronos-ts/core": patch
---

`InferResult<typeof D.result>` is the result schema's output. `result` is optional on a descriptor, so `typeof D.result` includes `undefined`, and `InferResult` used to turn that into `unknown`. It now sets the `undefined` aside. A descriptor without a result still gives `unknown`. `ctx.query` answers `InferResult` too, as `query()` does, so a host can implement `ctx.send` and `ctx.query` with `send` and `query`.
