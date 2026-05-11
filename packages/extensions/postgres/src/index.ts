// Public entry point for @kronos-ts/postgres.
//
// Wave-1 only exports the error surface; subsequent waves layer in:
//   Plan 03 — adapter interface (./adapter.js)
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
