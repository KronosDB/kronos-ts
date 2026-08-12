/**
 * postgres(config) — Extension factory for @kronos-ts/postgres.
 *
 * Populates five slots:
 *   - eventStore           : EventStorageEngine via createPostgresEventStore
 *   - snapshotStore        : SnapshotStore via createPostgresSnapshotStore
 *   - transactionManager   : postgresTransactionManager(adapter)
 *   - unitOfWorkFactory    : lazyTransactionalUnitOfWorkFactory(runInNewUoW, tm)
 *   - eventScheduler       : createPostgresEventScheduler(...) (durable,
 *                            background worker started in "processors" stage)
 *
 * Setting the last two together is what gives `append() + schedule()` (and
 * any future postgres-extension writer) a SHARED UoW transaction —
 * everything that writes inside one UoW commits or rolls back atomically.
 * Lazy variant chosen so pure-read UoWs (queries, projections that don't
 * write) never claim a pool connection. Users who need different
 * composition (e.g., eager for benchmarking, custom UoW wrapping) can
 * override with `app.forceSet(...)`.
 *
 * Lifecycle (mirrors @kronos-ts/kronosdb extension shape):
 *   - app.onStart("connect", ...) — adapter.connect() with withRetry; then
 *     bootstrapSchema (skip if config.bootstrap === false so users running
 *     their own migration tooling are not surprised)
 *   - app.onStop("connect", ...)  — adapter.disconnect()
 *
 * Does NOT populate eventBus, commandBus, queryBus, or tokenStore (out of
 * scope for this extension — postgres token store is a separate package).
 */

import type { ResilienceConfig } from "@kronos-ts/common"
import { withRetry } from "@kronos-ts/common"
import {
  lazyTransactionalUnitOfWorkFactory,
  runInNewUoW,
} from "@kronos-ts/messaging"
import type { PostgresAdapter } from "./adapter.js"
import { createPostgresEventStore } from "./postgres-event-store.js"
import { createPostgresSnapshotStore } from "./postgres-snapshot-store.js"
import { postgresTransactionManager } from "./postgres-transaction-manager.js"
import type { Serializer } from "@kronos-ts/common"
import type { TagResolver } from "@kronos-ts/eventsourcing"
import {
  createPostgresEventScheduler,
  type PostgresEventScheduler,
} from "./postgres-event-scheduler.js"
import { bootstrapSchema, DEFAULT_TABLE_NAMES, type TableNames } from "./schema.js"

export interface PostgresConfig {
  /** Driver adapter — pg / postgres / bun-sql / custom. Created by the user. */
  readonly adapter: PostgresAdapter
  /** Auto-create the schema on connect. Defaults to true. Set false when
   *  running your own migrations. */
  readonly bootstrap?: boolean
  /** Override default table names (kronos_events / kronos_snapshots). */
  readonly tableNames?: TableNames
  /** Retry policy for the initial connect + bootstrap. Defaults to
   *  framework defaults via withRetry. */
  readonly resilience?: Partial<ResilienceConfig>
  /** Tuning for the durable scheduler's polling worker. */
  readonly scheduler?: {
    readonly pollIntervalMs?: number
    readonly batchSize?: number
  }
}

/**
 * A Postgres backend. There is no lifecycle framework: this is an async factory
 * that connects (and bootstraps) eagerly, hands back the components it provides,
 * and gives you a `start`/`close` pair to call in whatever order your bootstrap
 * says. The order is written down in your composition root rather than encoded
 * in framework stages:
 *
 * ```ts
 * const pg  = await postgres({ adapter, serializer, tagResolver })
 * const app = createApp({ components: { ...inMemoryComponents(), ...pg.components }, modules })
 * await pg.start()          // scheduler, once every handler is subscribed
 * // …
 * await app.stop(); await pg.close()
 * ```
 */
/** Everything postgres() needs: its own config plus the framework values it borrows. */
export type PostgresOptions = PostgresConfig & { serializer: Serializer; tagResolver: TagResolver }

export interface PostgresBackend {
  readonly components: PostgresComponents
  /** Start background workers (the durable scheduler). Call after handlers are registered. */
  start(): Promise<void>
  /** Stop workers and disconnect. */
  close(): Promise<void>
}

export interface PostgresComponents {
  eventStore: ReturnType<typeof createPostgresEventStore>
  snapshotStore: ReturnType<typeof createPostgresSnapshotStore>
  transactionManager: ReturnType<typeof postgresTransactionManager>
  unitOfWorkFactory: ReturnType<typeof lazyTransactionalUnitOfWorkFactory>
  eventScheduler: PostgresEventScheduler
}

export async function postgres(
  options: PostgresOptions,
): Promise<PostgresBackend> {
  const config = options
  const { adapter, resilience, serializer, tagResolver } = config
  const bootstrap = config.bootstrap ?? true
  const tables = config.tableNames ?? DEFAULT_TABLE_NAMES

  // Safety timeouts (idle-in-transaction / statement) are armed by the adapter
  // itself, so two adapters on two databases stay independently configured.
  const transactionManager = postgresTransactionManager(adapter)

  await withRetry(() => adapter.connect(), { event: "initial-connect", ...resilience })
  if (bootstrap) {
    await withRetry(() => bootstrapSchema(adapter, { tableNames: tables }), {
      event: "initial-connect",
      ...resilience,
    })
  }

  const eventStore = createPostgresEventStore({ adapter, serializer, tagResolver, tableNames: tables })
  const snapshotStore = createPostgresSnapshotStore({ adapter, serializer, tableNames: tables })
  // Lazy: pure-read UoWs never claim a connection; the first writer begins the
  // tx, and everything in that UoW — events AND co-located writes — commits or
  // rolls back together.
  const unitOfWorkFactory = lazyTransactionalUnitOfWorkFactory(runInNewUoW, transactionManager)
  const eventScheduler = createPostgresEventScheduler({
    adapter,
    eventStore,
    uowFactory: unitOfWorkFactory,
    tagResolver,
    tableNames: tables,
    ...config.scheduler,
  })

  return {
    components: { eventStore, snapshotStore, transactionManager, unitOfWorkFactory, eventScheduler },
    async start() {
      await eventScheduler.start()
    },
    async close() {
      await eventScheduler.stop()
      await adapter.disconnect()
    },
  }
}
