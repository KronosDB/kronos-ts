---
"@kronos-ts/drizzle": minor
"@kronos-ts/kysely": minor
"@kronos-ts/knex": minor
"@kronos-ts/typeorm": minor
"@kronos-ts/prisma": minor
---

Deprecated. Kronos offers one persistence family, `@kronos-ts/postgres`, which owns the task's transaction so token advances, dead-letter parks and read-model writes commit together. A query builder such as Drizzle or Kysely is constructed over `ctx.sql().unwrap()` in a slice-owned handler wrapper, with no package in between. These packages remain published for existing users and receive no further changes. Their unit-of-work factories are now marked transactional, so `localQueryBus` refuses them like any other family's.
