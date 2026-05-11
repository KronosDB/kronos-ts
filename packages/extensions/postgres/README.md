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
import { createApp } from "@kronos-ts/core"
import { postgres } from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"

const app = createApp()
app.use(postgres({
  adapter: pgAdapter({ connectionString: "postgresql://user:pass@host/db" }),
  // Default: auto-create schema on connect. Set false to manage migrations yourself.
  bootstrap: true,
}))

await app.start()
```

### Accessing slots

```typescript
// Capture slots via app.decorate() before app.start()
let eventStore
app.decorate("eventStore", (inner) => { eventStore = inner; return inner })
await app.start()

// Append events (DCB)
const marker = await eventStore.append(events, {
  criteria: { kind: "tags", tags: [{ key: "order", value: "123" }] },
  marker: previousMarker,
})

// Source events for an entity
const { events: sourced, marker: newMarker } = await eventStore.source({
  criteria: { kind: "tags", tags: [{ key: "order", value: "123" }] },
  start: 0n,
})

// Tail events (gap-free, xid8-watermarked)
const stream = eventStore.open({ position: lastSeenPosition })
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
- `kronos_snapshots` — composite-PK `(entity_name, entity_id)` with BYTEA payload

Custom table names: `postgres({ adapter, tableNames: { events: "my_events", snapshots: "my_snaps" } })`.

## SQLSTATE

DCB violations raise SQLSTATE `KR001`. The adapter catches this and throws `AppendConditionError`. To detect violations from your own code:

```typescript
import { isDcbViolation, AppendConditionError } from "@kronos-ts/postgres"

try {
  await eventStore.append(events, condition)
} catch (e) {
  if (e instanceof AppendConditionError) {
    // Retry with fresh marker
  }
}
```

## FAQ

**Q: Can I run my own migrations?** Yes — set `bootstrap: false` and apply the DDL from `@kronos-ts/postgres/schema` yourself.

**Q: PgBouncer compatibility?** Yes — the adapter uses xact-scoped advisory locks (never session-scoped), so transaction-pooling mode is safe.

**Q: Older Postgres support?** No. `xid8` requires PG14+. PG14 has been GA since 2021 and is universally supported across managed services.

**Q: Is Bun.sql LISTEN available in all Bun versions?** `Bun.SQL` was introduced in Bun 1.2. The `bunSqlAdapter` feature-detects native LISTEN; if unavailable (older Bun), it falls back to a 250ms polling shim. For production streaming workloads, use Bun 1.2+ or switch to `pgAdapter` / `postgresAdapter` which always have native LISTEN.

**Q: TokenStore?** Out of scope for this package. Compose with `@kronos-ts/drizzle` / `@kronos-ts/knex` / etc. for token-store needs.
