// @kronos-ts/postgres — the FULL persistence family, no ORM required.
//
//   const pg = postgresPool(connectionString)
//   await pg.start()
//
//   const uow        = postgresUnitOfWork(unitOfWork, pg)
//   const eventStore = postgresSchedulingEventStore(
//     postgresSnapshottingEventStore(
//       postgresEventStore(pg, { tagResolver }), pg, { serializer }),
//     pg, { unitOfWork: uow, tagResolver })
//   const tokenStore  = postgresTokenStore(pg)
//   const deadLetters = postgresDeadLetterQueue(pg)
//
// TWO STORE TIERS, ONE OBJECT. Snapshotting and scheduling are both wrappers
// on the log and both ADDITIVE, so the composed value carries both capabilities
// and a host names it once. There is no `scheduler` const any more, because
// there is no second thing to wire.
//
// Every one of them is a function of the pool, and the pool is the only thing
// with a lifetime. There is no bundle: a host names the pieces this deployment
// actually uses, and nothing else gets constructed.
//
// PRINCIPLE: this family is keyed by TRANSACTION IDENTITY. The token store, the
// dead-letter queue, the event store and a handler's own `ctx.sql()` all write
// through the transaction `postgresUnitOfWork` put on the unit of work, so they
// commit or roll back as one. Never mix two families within one processor.
//
// Adapter implementations are NOT exported from this barrel — import them via
// the sub-path exports declared in package.json:
//   import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"

export {
  AppendConditionError,
  KRONOS_DCB_VIOLATION_SQLSTATE,
  isDcbViolation,
} from "./errors.js"

// Adapter contract types (re-exported so users can write
// `function myFn(adapter: PostgresAdapter)` against the package root).
// Adapter implementations stay sub-path-only.
export {
  IsolationLevel,
  type PostgresAdapter,
  type PostgresAdapterTransaction,
  type ListenSubscription,
  type QueryRow,
} from "./adapter.js"

// The resource — a connection string or an adapter you built, with a lifetime.
export {
  postgresPool,
  type PostgresResource,
  type PostgresPoolOptions,
} from "./postgres-pool.js"

// The stores.
export {
  postgresEventStore,
  type PostgresEventStoreConfig,
  type TagResolver,
} from "./postgres-event-store.js"

export {
  postgresSnapshottingEventStore,
  type PostgresSnapshottingEventStoreConfig,
} from "./postgres-snapshotting-event-store.js"

export {
  postgresTokenStore,
  type PostgresTokenStoreOptions,
} from "./postgres-token-store.js"

export {
  postgresDeadLetterQueue,
  type PostgresDeadLetterQueueOptions,
} from "./postgres-dead-letter-queue.js"

// The unit-of-work factory and its TYPED accessor pair. The transaction lives
// in this package, keyed by unit of work — the base `UnitOfWork` has none — so
// `postgresTransaction` is the only way to open one and
// `activePostgresTransaction` the only way to observe one without opening it.
export {
  postgresUnitOfWork,
  type PostgresFamily,
  postgresTransaction,
  activePostgresTransaction,
} from "./postgres-transaction.js"

// The handler-function wrapper and its named context types — ONE wrapper for
// command, event and query handlers alike.
export {
  postgresHandler,
  type PostgresCapability,
  type PostgresContext,
  type PostgresEventContext,
  type PostgresQueryContext,
  type Sql,
  type Tx,
} from "./postgres-handler.js"

// Per-transaction safety timeouts (idle-in-transaction / statement), armed by
// each adapter's transaction() at BEGIN. The options are spread onto every
// adapter config; the helpers are exported for authors of custom adapters.
export {
  type SessionTimeoutOptions,
  type ResolvedSessionTimeouts,
  resolveSessionTimeouts,
  applySessionTimeouts,
} from "./session-timeouts.js"

// THE SCHEDULING CAPABILITY TIER: durable schedule + cancel on the log itself,
// plus a polling worker that fires due schedules into the store it wraps.
export {
  postgresSchedulingEventStore,
  type PostgresSchedulingControl,
  type PostgresSchedulingConfig,
} from "./postgres-scheduling-event-store.js"

// Schema bootstrap + DDL builders — the migration surface. `postgresPool` runs
// bootstrapSchema for you; pass `{ bootstrap: false }` and run these yourself
// when your own migration tooling owns the database.
export {
  bootstrapSchema,
  buildEventsTableDDL,
  buildEventsIndexesDDL,
  buildSnapshotsTableDDL,
  buildScheduledEventsTableDDL,
  buildScheduledEventsIndexesDDL,
  buildTokensTableDDL,
  buildDeadLettersTableDDL,
  buildDeadLettersIndexesDDL,
  DEFAULT_TABLE_NAMES,
  KRONOS_SCHEMA_LOCK_KEY,
  type TableNames,
  type SchemaBootstrapAdapter,
  type BootstrapSchemaOptions,
} from "./schema.js"
