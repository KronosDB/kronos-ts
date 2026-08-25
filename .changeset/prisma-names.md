---
"@kronos-ts/prisma": minor
---

`PrismaFamily` is `PrismaUnitOfWork`, and `PrismaContext` is `PrismaCommandContext`. BREAKING for
anyone who spelled either.

```ts
// before
type Task = CorrelatingUnitOfWork & PrismaFamily
commandHandler(Edit, async (m, ctx: PrismaContext) => { ctx.prisma() })

// after
type Task = CorrelatingUnitOfWork & PrismaUnitOfWork
commandHandler(Edit, async (m, ctx: PrismaCommandContext) => { ctx.prisma() })
```

The brand names what the factory mints — `prismaUnitOfWork(next, …)` returns
`() => U & PrismaUnitOfWork` — and the command context sits beside
`PrismaEventContext` and `PrismaQueryContext` with a matching name.
