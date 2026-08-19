---
"@kronos-ts/postgres": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/prisma": minor
"@kronos-ts/typeorm": minor
---

Six persistence packages, one identical seven-function family.

A processor's token store, its dead-letter queue and its handlers must write
through the SAME client handle, or the token advances in a transaction the
events never joined. That makes a persistence package a FAMILY keyed by
transaction identity — not a bag of adapters — so all six now expose the same
seven functions for their own client type, and none of them delegates to
another.

```ts
xUnitOfWork(client, make: () => UnitOfWork): () => UnitOfWork
xTokenStore(client): TokenStore
xDeadLetterQueue(client): SequencedDeadLetterQueue
xTransaction(uow): Promise<Tx>          // opens; REJECTS a foreign uow
activeXTransaction(uow): Tx | undefined // observes, never opens
xHandler(handler, client): handler      // wraps the FUNCTION; ctx gains the accessor
interface XContext extends HandlerContext { … }   // + Event / Query variants
```

**`@kronos-ts/postgres` gains the whole family.** The `postgres()` bundle is
DELETED and decomposed: `postgresPool(connectionString | adapter)` is the
resource, with `postgresEventStore(pg, { serializer, tagResolver })` and
`postgresSnapshotStore(pg)` split out of it. NEW: `postgresTokenStore(pg)`,
`postgresDeadLetterQueue(pg)`, `postgresHandler(handler, pg)` and
`PostgresContext { sql(): Sql | Tx }`. They are written against the existing
`PostgresAdapter`, so they work over `pg`, `postgres.js` and `Bun.sql` alike,
and they use the same table shapes as the ORM families with the schema DDL
exported for migrations. You no longer need an ORM to run a durable processor.

`postgresUnitOfWork(pg, make)` opens its transaction LAZILY — that is postgres's
honest default, where drizzle's is eager. Neither conjures a delegate: `make` is
explicit in both.

**The other five are aligned to the drizzle template.** Deleted along the way:
the five `TransactionManager` remnants, fifteen
`xCommandHandler`/`xEventHandler`/`xQueryHandler` triples collapsed into one
generic wrapper each, every DLQ constructor `tableName`/group parameter (the
seam carries the processing group per call), and the config-record constructors
— `drizzleTokenStore({ db, table, claimTimeoutMs })` becomes
`drizzleTokenStore(db, { claimTimeoutMs? })`, a positional handle with a
trailing options record for genuine tuning only.

Never mix families within one processor. That was always true; now the
signatures say so.
