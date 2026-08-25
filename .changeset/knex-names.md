---
"@kronos-ts/knex": minor
---

`KnexFamily` is `KnexUnitOfWork`, and `KnexContext` is `KnexCommandContext`. BREAKING for
anyone who spelled either.

```ts
// before
type Task = CorrelatingUnitOfWork & KnexFamily
commandHandler(Edit, async (m, ctx: KnexContext) => { ctx.knex() })

// after
type Task = CorrelatingUnitOfWork & KnexUnitOfWork
commandHandler(Edit, async (m, ctx: KnexCommandContext) => { ctx.knex() })
```

The brand names what the factory mints — `knexUnitOfWork(next, …)` returns
`() => U & KnexUnitOfWork` — and the command context sits beside
`KnexEventContext` and `KnexQueryContext` with a matching name.
