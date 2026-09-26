---
"@kronos-ts/postgres": patch
---

`postgresHandler` opens the unit of work's transaction when the handler is its first writer.

The transaction `postgresUnitOfWork` puts on a unit of work is lazy: it begins when the first writer asks for it. In a processor batch that first writer was never the handler — `sql()` only ever looked for an already-open transaction — so every projection statement ran on the pool, one autocommit at a time, and only the token write at prepare-commit ran inside the transaction. A crash between the two replayed a batch whose projection writes had already landed. Now the wrapper opens the family's transaction before the handler runs, and a handler's statements commit with the token as the docs always said. A unit of work the family did not mint still gets the pool. On a KronosDB event store with a Postgres projection, a processor at batch size 100 goes from about 550 to several thousand events per second.
