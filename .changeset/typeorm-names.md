---
"@kronos-ts/typeorm": minor
---

`TypeormFamily` is `TypeormUnitOfWork`, and `TypeormContext` is `TypeormCommandContext`. BREAKING for
anyone who spelled either.

```ts
// before
type Task = CorrelatingUnitOfWork & TypeormFamily
commandHandler(Edit, async (m, ctx: TypeormContext) => { ctx.manager() })

// after
type Task = CorrelatingUnitOfWork & TypeormUnitOfWork
commandHandler(Edit, async (m, ctx: TypeormCommandContext) => { ctx.manager() })
```

The brand names what the factory mints — `typeormUnitOfWork(next, …)` returns
`() => U & TypeormUnitOfWork` — and the command context sits beside
`TypeormEventContext` and `TypeormQueryContext` with a matching name.
