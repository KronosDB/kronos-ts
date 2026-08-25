---
"@kronos-ts/drizzle": minor
---

`DrizzleFamily` is `DrizzleUnitOfWork`, and `DrizzleContext` is `DrizzleCommandContext`. BREAKING for
anyone who spelled either.

```ts
// before
type Task = CorrelatingUnitOfWork & DrizzleFamily
commandHandler(Edit, async (m, ctx: DrizzleContext) => { ctx.db() })

// after
type Task = CorrelatingUnitOfWork & DrizzleUnitOfWork
commandHandler(Edit, async (m, ctx: DrizzleCommandContext) => { ctx.db() })
```

The brand names what the factory mints — `drizzleUnitOfWork(next, …)` returns
`() => U & DrizzleUnitOfWork` — and the command context sits beside
`DrizzleEventContext` and `DrizzleQueryContext` with a matching name.
