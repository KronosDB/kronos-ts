---
"@kronos-ts/core": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/postgres": minor
"@kronos-ts/prisma": minor
"@kronos-ts/typeorm": minor
---

The persistence-family type is gone. The stores enforce the rule themselves,
and they catch more of it than the type did. BREAKING: `PersistenceFamily` and
the six `XFamily` types are removed.

```ts
// before — a phantom brand on the task, and a compile error when they disagreed
type DrizzleUnitOfWork = PersistenceFamily<"drizzle", "…">
drizzleUnitOfWork<U>(next: () => U, db): () => U & DrizzleUnitOfWork
drizzleTokenStore(db): TokenStore<UnitOfWork & DrizzleUnitOfWork>

// after — nothing marks a task, and the store says what it needs when asked
drizzleUnitOfWork<U>(next: () => U, db): () => U
drizzleTokenStore(db): TokenStore
```

**Why it went.** The brand existed because mixing families failed SILENTLY: a
drizzle token store handed a postgres task asked for its transaction, was told
there was none, fell back to its plain handle, and committed the token outside
the batch. But that silence was a bug in the fallback, not a fact of life — and
the brand never caught the likeliest spelling of the same mistake, because
`inMemoryTokenStore()` is assignable into any processor and commits outside the
batch just as happily.

So the fallback is fixed instead: a token store or dead-letter queue handed a
unit of work carrying no transaction of its own now THROWS, naming the factory
to build the processor's `unitOfWork` with. The failure is loud on the first
token write, in any test that runs the processor, whichever store you mixed in.

A handler's accessor still falls back — `ctx.db()` works whether or not the seam
it runs in is transactional, because that is a deployment decision. A token
store has no such freedom, which is why absence is an error there and a default
here.

Source-compatible for anyone already following the rule; the six
`<pkg>UnitOfWork` type exports are removed, and nothing else changes.
