# @kronos-ts/postgres

First-party Postgres extension for Kronos. Provides DCB-compliant event storage and snapshots via the framework's `eventStore` + `snapshotStore` slots.

## Install

```bash
bun add @kronos-ts/postgres
# Choose one driver:
bun add pg                 # for pgAdapter (Node + Bun)
bun add postgres           # for postgresAdapter (porsager — Node + Bun)
# bunSqlAdapter has no external dep (uses Bun.sql built-in, requires Bun >= 1.2)
```

Requires Postgres 14+ (for `xid8` and `pg_snapshot_xmin`).

## Usage

```typescript
import { kronos } from "@kronos-ts/app"
import { postgres } from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"

const app = await kronos()
  .use(postgres({
    adapter: pgAdapter({ connectionString: "postgresql://user:pass@host/db" }),
    // Default: auto-create schema on connect. Set false to manage migrations yourself.
    bootstrap: true,
  }))
  .start()
```

### Accessing slots

In normal usage you don't touch the `eventStore` directly — command handlers, `load()`, and tracking processors handle it. If you need the raw store (e.g. for tests, scripts, or low-level access), capture it via a probe decorator before `start()` — `RunningApp` doesn't expose it on its public surface:

```typescript
import type { App } from "@kronos-ts/app"
import type { EventStore } from "@kronos-ts/eventsourcing"
import { EventCriteria } from "@kronos-ts/messaging"
import { tag } from "@kronos-ts/common"

let eventStore: EventStore | undefined
const capture = (a: App) => {
  a.decorate("eventStore", (inner) => { eventStore = inner; return inner })
}

const app = await kronos()
  .use(capture)
  .use(postgres({ adapter: pgAdapter({ connectionString }) }))
  .start()

// 1. Source events for a state (criteria + optional start position).
//    Returns the matched events plus a consistency marker.
const criteria = EventCriteria.havingTags(tag("order", "123"))
const { events: sourced, marker } = await eventStore!.source({ criteria })

// 2. Append with a DCB precondition. The marker locks in the prefix you read
//    from source() — if anything matching `criteria` was appended in the
//    meantime, AppendConditionError is thrown.
const newMarker = await eventStore!.append(newEvents, { criteria, marker })

// 3. Tail events. open() returns a pull-based MessageStream<SequencedEvent>,
//    not an AsyncIterable. Pull with next() and register a callback for
//    wake-ups when more events arrive.
const stream = eventStore!.open({ position: 0n, criteria })
stream.setCallback(() => {
  while (stream.hasNextAvailable()) {
    const seq = stream.next()
    if (!seq) break
    // seq.event is the EventMessage; seq.sequence is the bigint position.
  }
})
// remember to stream.close() when you're done
```

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
})
```

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

This is the framework's transaction, exposed for you to enroll work into. The extension does **not** ship an ORM client — you bring (or wrap) your own. The two pieces you need are both from `@kronos-ts/messaging`:

- `getOrBeginActiveTransaction<PostgresAdapterTransaction>()` — returns the UoW's transaction, opening the lazy one on first call. Use on **write** paths.
- `getActiveTransaction<PostgresAdapterTransaction>()` — returns it only if already open (else `undefined`), without forcing one. Use on **read-only** paths that shouldn't provoke a connection.

### Running your own SQL — no ORM needed

The transaction handle runs parameterised SQL directly (it's what the event store appends with):

```typescript
import { getOrBeginActiveTransaction } from "@kronos-ts/messaging"
import type { PostgresAdapterTransaction } from "@kronos-ts/postgres"
import { append } from "@kronos-ts/eventsourcing"

// inside a command handler:
const tx = await getOrBeginActiveTransaction<PostgresAdapterTransaction>()
await tx!.query("UPDATE widgets SET name = $1 WHERE id = $2", [name, id])
append(WidgetUpdated, { id, name })   // commits together, rolls back together
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
async function uowDb() {
  const tx = await getOrBeginActiveTransaction<PostgresAdapterTransaction>()
  if (!tx) throw new Error("no active UnitOfWork transaction")
  return drizzle(tx.unwrap<PoolClient>(), { schema })
}

// in a command handler:
const db = await uowDb()
await db.insert(widgets).values({ id, name })
  .onConflictDoUpdate({ target: widgets.id, set: { name } })
append(WidgetUpdated, { id, name })
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

`bootstrap: true` (default) auto-creates these tables on connect:
- `kronos_events` — append-only event log with `xid8` watermark, BIGSERIAL position, JSONB payload, GIN index on tags
- `kronos_snapshots` — composite-PK `(state_name, state_id)` with BYTEA payload

Custom table names: `postgres({ adapter, tableNames: { events: "my_events", snapshots: "my_snaps" } })`.

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

**Q: Can I run my own migrations?** Yes — set `bootstrap: false` and apply equivalent DDL via your own migration tooling. The reference DDL lives in `packages/extensions/postgres/src/schema.ts` (`buildEventsTableDDL`, `buildEventsIndexesDDL`, `buildSnapshotsTableDDL`).

**Q: PgBouncer compatibility?** Yes — the adapter uses xact-scoped advisory locks (never session-scoped), so transaction-pooling mode is safe.

**Q: Older Postgres support?** No. `xid8` requires PG14+. PG14 has been GA since 2021 and is universally supported across managed services.

**Q: Is Bun.sql LISTEN available in all Bun versions?** `Bun.SQL` was introduced in Bun 1.2. The `bunSqlAdapter` feature-detects native LISTEN; if unavailable (older Bun), it falls back to a 250ms polling shim. For production streaming workloads, use Bun 1.2+ or switch to `pgAdapter` / `postgresAdapter` which always have native LISTEN.

**Q: TokenStore?** Out of scope for this package. Compose with `@kronos-ts/drizzle` / `@kronos-ts/knex` / etc. for token-store needs.
