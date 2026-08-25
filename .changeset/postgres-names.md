---
"@kronos-ts/postgres": minor
---

`PostgresFamily` is `PostgresUnitOfWork`, and `PostgresContext` is `PostgresCommandContext`. BREAKING for
anyone who spelled either.

```ts
// before
type Task = CorrelatingUnitOfWork & PostgresFamily
commandHandler(Edit, async (m, ctx: PostgresContext) => { ctx.sql() })

// after
type Task = CorrelatingUnitOfWork & PostgresUnitOfWork
commandHandler(Edit, async (m, ctx: PostgresCommandContext) => { ctx.sql() })
```

The brand names what the factory mints — `postgresUnitOfWork(next, …)` returns
`() => U & PostgresUnitOfWork` — and the command context sits beside
`PostgresEventContext` and `PostgresQueryContext` with a matching name.
