---
"@kronos-ts/postgres": patch
---

A command handler's `ctx.sql()` runs on the pool, and `postgresHandler` opens no transaction for a command.

- A command's own statements commit as they run, before its events are appended. A rejected append does not undo them, and a retry runs them again, so write them idempotently.
- A command holds no connection while it loads state. A burst of concurrent commands at least the size of the pool no longer deadlocks until Postgres ends the idle transactions.
- Event handlers are unchanged: `postgresHandler` still opens the unit of work's transaction, and a batch's statements commit with the token.
- `pgAdapter`: when the server ends a transaction's session, the transaction fails at once, the client is destroyed instead of returned to the pool, and its `error` event is no longer uncaught.

```ts
// before: in the append's transaction
await ctx.sql().query("UPDATE customers SET email = $2 WHERE id = $1", [id, email])

// after: on the pool, idempotent
await ctx.sql().query(
  "INSERT INTO customers (id, email) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email",
  [id, email],
)
```
