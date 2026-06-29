// Public entry point for @kronos-ts/postgres.
//
// Wave-1 only exports the error surface; subsequent waves layer in:
//   Plan 04 — postgres() extension factory + PostgresConfig (./postgres.js),
//             createPostgresEventStore (./postgres-event-store.js)
//   Plan 05 — createPostgresSnapshotStore (./postgres-snapshot-store.js)
//
// Adapter implementations are NOT exported from this barrel — users import
// them via the sub-path exports declared in package.json:
//   import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"

export {
  AppendConditionError,
  KRONOS_DCB_VIOLATION_SQLSTATE,
  isDcbViolation,
} from "./errors.js"

// Adapter contract types (re-export so users can write
// `function myFn(adapter: PostgresAdapter)` against the package root).
// Adapter implementations stay sub-path-only.
export {
  IsolationLevel,
  type PostgresAdapter,
  type PostgresAdapterTransaction,
  type ListenSubscription,
  type QueryRow,
} from "./adapter.js"

// Engine factory (Plan 04 + extended in Plan 05)
export {
  createPostgresEventStore,
  type PostgresEventStoreConfig,
  type Serializer,
  type TagResolver,
} from "./postgres-event-store.js"

// Snapshot store factory (Plan 05)
export {
  createPostgresSnapshotStore,
  type PostgresSnapshotStoreConfig,
} from "./postgres-snapshot-store.js"

// Extension factory (Plan 05)
export { postgres, type PostgresConfig } from "./postgres.js"

// Transaction manager — bridges the framework's TransactionManager lifecycle
// to adapter.transaction(). Users typically get this wired automatically via
// `postgres(config)`; exported for direct use when composing UoW runners by
// hand.
export { postgresTransactionManager } from "./postgres-transaction-manager.js"

// Per-transaction safety timeouts (idle-in-transaction / statement), armed by
// each adapter's transaction() at BEGIN. The options are spread onto every
// adapter config; the helpers are exported for authors of custom adapters.
export {
  type SessionTimeoutOptions,
  type ResolvedSessionTimeouts,
  resolveSessionTimeouts,
  applySessionTimeouts,
} from "./session-timeouts.js"

// Postgres event scheduler — durable schedule() + cancel() + polling worker
// that fires due schedules into the event store. Wired into postgres()
// automatically when a uowFactory with the lazy postgres tx is in place;
// exported here so users who compose their own wiring can construct one.
export {
  createPostgresEventScheduler,
  type PostgresEventScheduler,
  type PostgresEventSchedulerConfig,
} from "./postgres-event-scheduler.js"

// Schema bootstrap + DDL builders — exposed for users who want to run their
// own migrations (set `postgres({ bootstrap: false })`) or drive the store
// directly without going through the extension factory.
export {
  bootstrapSchema,
  buildEventsTableDDL,
  buildEventsIndexesDDL,
  buildSnapshotsTableDDL,
  buildScheduledEventsTableDDL,
  buildScheduledEventsIndexesDDL,
  DEFAULT_TABLE_NAMES,
  type TableNames,
} from "./schema.js"
