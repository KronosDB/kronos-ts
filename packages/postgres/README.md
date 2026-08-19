# @kronos-ts/postgres

The full Kronos persistence family on Postgres — event store, snapshot store,
token store, dead-letter queue, unit of work, handler wrapper and scheduler —
over raw SQL. No ORM required.

## Install

```bash
bun add @kronos-ts/postgres
# Choose one driver:
bun add pg                 # for pgAdapter (Node + Bun) — also what a bare
                           # connection string loads
bun add postgres           # for postgresAdapter (porsager — Node + Bun)
# bunSqlAdapter has no external dep (uses Bun.sql built-in, requires Bun >= 1.2)
```

Requires Postgres 14+ (for `xid8` and `pg_snapshot_xmin`).

## Usage

One pool has a lifetime. Everything else is a plain function of it:

```typescript
import { unitOfWork, simpleCommandBus, jsonSerializer, descriptorBasedTagResolver } from "@kronos-ts/core"
import {
  postgresPool,
  postgresEventStore,
  postgresSnapshotStore,
  postgresTokenStore,
  postgresDeadLetterQueue,
  postgresUnitOfWork,
} from "@kronos-ts/postgres"

const pg = postgresPool("postgresql://user:pass@host/db")
await pg.start()          // connects + bootstraps the schema

const eventStore    = postgresEventStore(pg, {
  serializer: jsonSerializer(),
  tagResolver: descriptorBasedTagResolver(),
})
const snapshotStore = postgresSnapshotStore(pg, { serializer: jsonSerializer() })
const tokenStore    = postgresTokenStore(pg)
const deadLetters   = postgresDeadLetterQueue(pg)
const uow           = postgresUnitOfWork(pg, unitOfWork)

const commandBus = simpleCommandBus(uow)
// …
await pg.close()
```

Name only what this deployment uses — nothing else is constructed.
`postgresPool` also takes an adapter you built yourself, which is how you pick a
different driver or tune the pool:

```typescript
postgresPool(pgAdapter({ connectionString, poolConfig: { max: 40 } }))
postgresPool(bunSqlAdapter({ connectionString }), { bootstrap: false })
```

### The family shares one transaction

Every function above is built from the same `pg`, and that is what makes them
one transaction. `postgresUnitOfWork` puts a (lazily opened) transaction on each
unit of work; the token store, the dead-letter queue, the event store and a
handler's own `ctx.sql()` all read it. A crash cannot advance a processor's
token while losing the work that token accounts for.

Never mix families within one processor: a `drizzleTokenStore` alongside a
`postgresEventStore` is two clients, hence two transactions, hence no atomicity.

### `ctx.sql()` in a handler

```typescript
import { postgresHandler, type PostgresContext } from "@kronos-ts/postgres"

const editWidget = commandHandler(EditWidget, async ({ payload }, ctx: PostgresContext) => {
  await ctx.sql().query("UPDATE widgets SET name = $2 WHERE id = $1", [payload.id, payload.name])
  ctx.append(WidgetUpdated, payload)   // commits together, rolls back together
})

kronos({
  commandHandlers: [editWidget]
    .map((h) => ({ ...h, handler: postgresHandler(h.handler, pg) }))
    .map((h) => ({ ...h, eventStore, commandBus, queryBus })),
})
```

`postgresHandler` wraps the handler FUNCTION, not the entry — it is a plain
`(next, pg) => (message, ctx) => result` with `<M, C, R>` inferred, so nothing
about a handler entry appears in its type and the host keeps ownership of the
spread. One wrapper covers command, event and query handlers alike: they differ
only in the context they receive, and the capability is added the same way to
each. `PostgresContext` / `PostgresEventContext` / `PostgresQueryContext` name
the three results.

The erasure is DIRECTIONAL — a handler that ASKS for `sql()` goes in, one that
asks only for the base context comes out — so wrapping twice, or wrapping a
handler that never asked, is a compile error rather than a silent no-op.

`ctx.sql()` returns the unit of work's transaction when one is open and the pool
otherwise; it never OPENS one. For the writer that must be inside a transaction
whether or not anything else has touched it yet, use the accessor pair directly:

- `postgresTransaction(uow)` — opens the lazy transaction if it has not begun.
  Rejects if `uow` did not come from `postgresUnitOfWork`.
- `activePostgresTransaction(uow)` — observes, never opens; `undefined` when
  there is nothing to join.

## Adapters

Three reference adapters ship with the package:

| Adapter | Runtime | Driver | LISTEN support |
|---------|---------|--------|----------------|
| `pgAdapter` | Node, Bun | `pg` (node-postgres) 8.20+ | native |
| `postgresAdapter` | Node, Bun | `postgres` (porsager) 3.4+ | native |
| `bunSqlAdapter` | Bun 1.2+ | built-in `Bun.sql` | native (1.2+) or polling shim |

You can also implement your own — `PostgresAdapter` from `@kronos-ts/postgres` is the contract.

### pgAdapter

```typescript
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"

const adapter = pgAdapter({
  connectionString: "postgresql://user:pass@host/db",
  poolConfig: { max: 10 }, // optional pg.Pool overrides
  // Per-transaction safety timeouts, armed via SET LOCAL on every transaction
  // this adapter opens. Defaults: 30s idle-in-transaction, statement off.
  idleInTransactionTimeoutMs: 30_000, // 0 disables
  statementTimeoutMs: 0, // opt in per deployment
})
```

> **Safety timeouts live on the adapter.** Every transaction opened through the
> adapter (UoW-scoped commits, the event store's own-tx appends, the scheduler
> worker) is bounded, and each adapter instance is configured independently — so
> an event-store adapter and an event-processing adapter pointed at two
> different databases stay fully decoupled. `idleInTransactionTimeoutMs` is the
> important one: it stops a stalled transaction from pinning a connection (and
> `pg_snapshot_xmin`, which gates the tailing query) open indefinitely. The same
> two options are available on `postgresAdapter` and `bunSqlAdapter`.

### postgresAdapter

```typescript
import { postgresAdapter } from "@kronos-ts/postgres/adapters/postgres"

const adapter = postgresAdapter({
  connectionString: "postgresql://user:pass@host/db",
  clientOptions: { idle_timeout: 20 }, // optional porsager/postgres options
})
```

### bunSqlAdapter

```typescript
import { bunSqlAdapter } from "@kronos-ts/postgres/adapters/bun-sql"

// Bun runtime only — throws at connect() if run under Node
const adapter = bunSqlAdapter({
  connectionString: "postgresql://user:pass@host/db",
})
```

## Transactions

Every command handler runs inside a single Unit of Work, and with this extension that UoW carries a Postgres transaction. A handler's appended events are buffered and flushed as one bulk write at commit; anything else you write **on the same transaction** commits — or rolls back — atomically with them. So "edit a row and append an `Updated` event" is one atomic operation.

The transaction is **lazy**: it opens on the first writer (an append, or your own SQL) and never opens for pure-read handlers, so read-only work claims no connection from the pool.

This is the framework's transaction, exposed for you to enroll work into. The package does **not** ship an ORM client — you bring (or wrap) your own. The two pieces you need are the accessor pair:

- `postgresTransaction(uow)` — returns the unit of work's transaction, opening the lazy one on first call. Use on **write** paths.
- `activePostgresTransaction(uow)` — returns it only if already open (else `undefined`), without forcing one. Use on **read-only** paths that shouldn't provoke a connection.

### Running your own SQL — no ORM needed

The transaction handle runs parameterised SQL directly (it's what the event store appends with):

```typescript
import { postgresTransaction } from "@kronos-ts/postgres"

// inside a command handler:
const tx = await postgresTransaction(ctx.unitOfWork)
await tx.query("UPDATE widgets SET name = $1 WHERE id = $2", [name, id])
ctx.append(WidgetUpdated, { id, name })   // commits together, rolls back together
```

### Handing the transaction to an ORM

`tx.unwrap<T>()` returns the live driver connection backing the transaction — the *same* connection the event store appends on. Bind your ORM to it and its writes join the transaction. The handle type depends on the adapter you wired:

| Adapter | `unwrap()` returns |
|---------|--------------------|
| `pgAdapter` | `pg` `PoolClient` |
| `postgresAdapter` | scoped `Sql` (porsager) |
| `bunSqlAdapter` | scoped Bun `SQL` |

#### Drizzle (the easy case)

`drizzle(connection)` accepts an already-open connection, so binding is a one-liner — pick the import matching your adapter:

```typescript
// pgAdapter
import { drizzle } from "drizzle-orm/node-postgres"
import type { PoolClient } from "pg"
const db = drizzle(tx.unwrap<PoolClient>(), { schema })

// postgresAdapter
import { drizzle } from "drizzle-orm/postgres-js"
import type { Sql } from "postgres"
const db = drizzle(tx.unwrap<Sql>(), { schema })

// bunSqlAdapter
import { drizzle } from "drizzle-orm/bun-sql"
import type { SQL } from "bun"
const db = drizzle(tx.unwrap<SQL>(), { schema })
```

`drizzle(conn)` is a thin wrapper, so a small per-call helper is the ergonomic shape:

```typescript
async function uowDb(ctx: { unitOfWork: UnitOfWork }) {
  const tx = await postgresTransaction(ctx.unitOfWork)
  return drizzle(tx.unwrap<PoolClient>(), { schema })
}

// in a command handler:
const db = await uowDb(ctx)
await db.insert(widgets).values({ id, name })
  .onConflictDoUpdate({ target: widgets.id, set: { name } })
ctx.append(WidgetUpdated, { id, name })
```

#### Kysely / TypeORM and other pool-owning ORMs

ORMs that manage their own connection pool are more work — they don't take a borrowed connection directly. The route is a thin custom dialect/driver whose `acquireConnection()` hands back `unwrap()`'s connection and whose begin/commit/release are no-ops (the UoW owns the transaction lifecycle). Doable, just more plumbing than Drizzle. For occasional statements, the raw `tx.query(...)` path above is usually simpler than wiring a dialect.

### The one rule

Bind your client to **`unwrap()`'s connection** — never to a separately-created pool. A separate pool is a separate connection, hence a separate transaction, and your writes silently stop being atomic with your events. `unwrap()` is the guarantee that you're on the framework's connection.

## DCB Semantics

The engine implements [Dynamic Consistency Boundaries](https://dcb.events):
- **Atomic multi-tag conflict check** on append with advisory-lock serialization
- **Criteria-based sourcing** with `@>` (contains-all) tag semantics
- **Gap-free tailing** using `xid8` + `pg_snapshot_xmin(pg_current_snapshot())`

Concurrent writers on **disjoint tags** run in parallel (advisory-lock taxonomy permits this). Concurrent writers on the **same tag** serialise — exactly one commits; the other receives `AppendConditionError`.

### Gap-free streaming

The `open()` streaming method uses a two-phase cursor to prevent the concurrent-commit gap bug:

- **Initial fetch:** `WHERE sequence_position > $cursor AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())`
- **Subsequent fetches:** `WHERE (transaction_id, sequence_position) > ($xid, $pos) AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())`

The `pg_snapshot_xmin` watermark hides events from in-flight transactions, preventing the cursor from advancing past events that haven't committed yet. When a slow transaction finally commits, its events become visible in the correct xid8 order — no gaps, no re-ordering.

## Schema

`postgresPool(..., { bootstrap: true })` (the default) auto-creates these tables on `start()`:
- `kronos_events` — append-only event log with `xid8` watermark, BIGSERIAL position, JSONB payload, GIN index on tags
- `kronos_snapshots` — composite-PK `(state_name, state_id)` with BYTEA payload
- `kronos_scheduled_events` — durable schedules, tombstoned on fire/cancel
- `kronos_token_entries` — processor tokens, PK `(processor_name, segment)`
- `kronos_dead_letters` — parked letters, partitioned by `processing_group`

The last two are the SAME column sets the ORM families write, so a deployment
can move between families without a migration.

Custom table names are a property of the pool, not of each store:

```typescript
postgresPool(connectionString, {
  tableNames: { events: "my_events", snapshots: "my_snaps", scheduled: "my_sched",
                tokens: "my_tokens", deadLetters: "my_dead_letters" },
})
```

## SQLSTATE

DCB violations raise SQLSTATE `KR001`. The adapter catches this and throws `AppendConditionError`. To detect violations from your own code:

```typescript
import { AppendConditionError } from "@kronos-ts/postgres"

try {
  await eventStore.append(events, condition)
} catch (e) {
  if (e instanceof AppendConditionError) {
    // Re-source the state to get a fresh marker, then retry.
  }
}
```

If you're working directly against the driver (bypassing the adapter), `isDcbViolation(err)` from `@kronos-ts/postgres` inspects a raw pg / postgres.js / Bun.sql error's `.code` field for the KR001 SQLSTATE.

## FAQ

**Q: Can I run my own migrations?** Yes — set `bootstrap: false` and apply equivalent DDL via your own migration tooling. Every builder is exported: `buildEventsTableDDL`, `buildEventsIndexesDDL`, `buildSnapshotsTableDDL`, `buildScheduledEventsTableDDL`, `buildScheduledEventsIndexesDDL`, `buildTokensTableDDL`, `buildDeadLettersTableDDL`, `buildDeadLettersIndexesDDL` — plus `bootstrapSchema` if you want the whole thing in one call.

**Q: PgBouncer compatibility?** Yes — the adapter uses xact-scoped advisory locks (never session-scoped), so transaction-pooling mode is safe.

**Q: Older Postgres support?** No. `xid8` requires PG14+. PG14 has been GA since 2021 and is universally supported across managed services.

**Q: Is Bun.sql LISTEN available in all Bun versions?** `Bun.SQL` was introduced in Bun 1.2. The `bunSqlAdapter` feature-detects native LISTEN; if unavailable (older Bun), it falls back to a 250ms polling shim. For production streaming workloads, use Bun 1.2+ or switch to `pgAdapter` / `postgresAdapter` which always have native LISTEN.

**Q: TokenStore? Dead-letter queue?** Both ship here — `postgresTokenStore(pg)` and `postgresDeadLetterQueue(pg)`. Use them rather than an ORM family's: they write through the same client handle as your appends, so they join the same transaction. The dead-letter queue takes the processing group per CALL, not in its constructor — one queue object is one table, and which partition a call touches is the caller's business.
