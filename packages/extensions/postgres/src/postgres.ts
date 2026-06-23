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

import type { App } from "@kronos-ts/app"
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
  /** Safety timeouts applied via `SET LOCAL` to every UoW-scoped transaction.
   *  Guards against a stalled UoW holding a connection — and pinning
   *  `pg_snapshot_xmin`, which would stall all streaming tailing — open until
   *  restart. Defaults: 30s idle-in-transaction, statement timeout disabled. */
  readonly transaction?: {
    readonly idleInTransactionTimeoutMs?: number
    readonly statementTimeoutMs?: number
  }
}

export function postgres(config: PostgresConfig): (app: App) => void {
  const { adapter, resilience } = config
  const bootstrap = config.bootstrap ?? true
  const tables = config.tableNames ?? DEFAULT_TABLE_NAMES

  const txManager = postgresTransactionManager(adapter, undefined, config.transaction)

  return (app: App) => {
    app.set("eventStore", ({ serializer, tagResolver }) =>
      createPostgresEventStore({ adapter, serializer, tagResolver, tableNames: tables }),
    )
    app.set("snapshotStore", ({ serializer }) =>
      createPostgresSnapshotStore({ adapter, serializer, tableNames: tables }),
    )
    app.set("transactionManager", () => txManager)
    // Lazy: pure-read UoWs never claim a connection; the first writer (an
    // append flush, or a user's own SQL via getOrBeginActiveTransaction)
    // begins the tx, and everything in that UoW — events AND co-located
    // writes — commits or rolls back together. The command bus runs handlers
    // through this factory (see createSimpleCommandBus), so command handlers
    // get the transaction without this extension reaching into the command
    // pipeline.
    app.set("unitOfWorkFactory", () =>
      lazyTransactionalUnitOfWorkFactory(runInNewUoW, txManager),
    )

    // Durable scheduler — closure captures the instance so the worker can be
    // start()'d in "processors" and stop()'d in "connect" symmetric to other
    // background workers.
    let scheduler: PostgresEventScheduler | undefined
    app.set("eventScheduler", ({ eventStore, unitOfWorkFactory, tagResolver }) => {
      scheduler = createPostgresEventScheduler({
        adapter,
        eventStore,
        uowFactory: unitOfWorkFactory,
        tagResolver,
        tableNames: tables,
        ...config.scheduler,
      })
      return scheduler
    })

    app.onStart("connect", async () => {
      await withRetry(() => adapter.connect(), { event: "initial-connect", ...resilience })
      if (bootstrap) {
        await withRetry(() => bootstrapSchema(adapter, { tableNames: tables }), {
          event: "initial-connect",
          ...resilience,
        })
      }
    })

    // Worker spins up after registration/warmup so all slots are resolved and
    // any user-supplied processors are in place before due schedules start
    // firing into the event store.
    app.onStart("processors", async () => {
      if (scheduler) await scheduler.start()
    })

    app.onStop("connect", async () => {
      if (scheduler) await scheduler.stop()
      await adapter.disconnect()
    })
  }
}
