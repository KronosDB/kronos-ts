---
"@kronos-ts/postgres": minor
---

Statement spans follow the trace, and the builder capabilities name their own types.

- `postgresHandler(handler, pg)` spans every statement issued through `ctx.sql()` as `db.statement` whenever the context carries `trace`. With no `trace`, `ctx.sql()` is the plain pool or transaction, as before.
- Removed: `observed` and `ObservedPostgres`. Replace `observed(pg)` with `pg`. To drop statement spans, filter `db.statement` at the collector.
- A wrapper that supplies `ctx.trace` inside `postgresHandler` is a compile error; put it outside. `postgresHandler` no longer declares `uses: ["trace"]`, so a chain without tracing boots.
- `DrizzleCapability` is `{ db: PgDatabase }` from `drizzle-orm/pg-core` and takes no argument, whatever the driver. `DrizzleCapability<typeof schema>` types the relational `db.query.*` API; build with the same `{ schema }`. Replace `DrizzleCapability<ReturnType<typeof makeDb>>` with `DrizzleCapability`.
- `KyselyCapability<Database>` is `{ db: Kysely<Database> }` and takes the table interface. Replace `KyselyCapability<ReturnType<typeof makeDb>>` with `KyselyCapability<Database>`.
- `drizzle-orm` and `kysely` are optional peer dependencies, imported for types only by their own subpaths.
- A handler typed with its own `{ readonly db: MyDb }` still works with `drizzleHandler` and `kyselyHandler`.
