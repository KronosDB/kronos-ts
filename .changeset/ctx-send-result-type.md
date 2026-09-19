---
"@kronos-ts/core": patch
---

`ctx.send` resolves to the command descriptor's `result` type, as `ctx.query` and the edge verb `send(bus, …)` already do. It was `unknown`. A command with no `result` schema still resolves to `unknown`. Remove casts on `await ctx.send(...)`.
