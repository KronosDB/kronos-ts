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

// Schema bootstrap + DDL builders — exposed for users who want to run their
// own migrations (set `postgres({ bootstrap: false })`) or drive the store
// directly without going through the extension factory.
export {
  bootstrapSchema,
  buildEventsTableDDL,
  buildEventsIndexesDDL,
  buildSnapshotsTableDDL,
  DEFAULT_TABLE_NAMES,
  type TableNames,
} from "./schema.js"
