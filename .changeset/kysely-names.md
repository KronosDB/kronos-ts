---
"@kronos-ts/kysely": minor
---

`KyselyFamily` is `KyselyUnitOfWork`, and `KyselyContext` is `KyselyCommandContext`. BREAKING for
anyone who spelled either.

```ts
// before
type Task = CorrelatingUnitOfWork & KyselyFamily
commandHandler(Edit, async (m, ctx: KyselyContext) => { ctx.db() })

// after
type Task = CorrelatingUnitOfWork & KyselyUnitOfWork
commandHandler(Edit, async (m, ctx: KyselyCommandContext) => { ctx.db() })
```

The brand names what the factory mints — `kyselyUnitOfWork(next, …)` returns
`() => U & KyselyUnitOfWork` — and the command context sits beside
`KyselyEventContext` and `KyselyQueryContext` with a matching name.
